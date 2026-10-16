/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_PL_SYS_PACKAGE_OB_DBMS_LOGIC_TABLE_MYSQL_H_
#define OCEANBASE_PL_SYS_PACKAGE_OB_DBMS_LOGIC_TABLE_MYSQL_H_

#include "pl/ob_pl.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSLogicTableMySql
{
public:
  ObDBMSLogicTableMySql() {}
  virtual ~ObDBMSLogicTableMySql() {}

  static int prewarm(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_namespace(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int drop_ltable(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int set_namespace_resource_limit(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int gather_namespace_stats_inner(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  struct PrewarmContext
  {
    PrewarmContext()
      : tenant_id_(OB_INVALID_TENANT_ID),
        namespace_id_(common::OB_INVALID_ID),
        part_object_id_(common::OB_INVALID_ID),
        hot_table_hit_(false),
        leader_only_(false),
        has_ivf_prewarm_(false),
        ivf_center_table_id_(common::OB_INVALID_ID),
        ivf_center_tablet_id_(),
        ivf_ltable_ids_(),
        allocator_(nullptr),
        pl_ctx_(nullptr)
    {}
    uint64_t                             tenant_id_;
    common::ObString                     collection_id_;
    uint64_t                             namespace_id_;
    common::ObString                     db_name_;
    common::ObString                     logic_data_table_name_;
    common::ObString                     kv_data_table_name_;
    common::ObString                     hot_table_name_;
    common::ObTabletID                   kv_tablet_id_;
    share::ObLSID                        ls_id_;
    // Logical centroid/CID rows are encoded into the physical KV tablet. Once
    // every observer supports leader-only PREWARM this contains only its LS
    // leader; during rolling upgrade it retains the legacy fanout targets.
    common::ObSEArray<common::ObAddr, 4> target_addrs_;
    uint64_t                             part_object_id_;
    bool                                 hot_table_hit_;
    bool                                 leader_only_;
    // IVF prewarm params; populated by gather_ivf_prewarm_params()
    bool                                 has_ivf_prewarm_;
    uint64_t                             ivf_center_table_id_;
    common::ObTabletID                   ivf_center_tablet_id_;
    common::ObSEArray<uint64_t, 4>       ivf_ltable_ids_;
    common::ObIAllocator                *allocator_;
    ObPLExecCtx                         *pl_ctx_;
  };

  struct DropNamespaceContext
  {
    DropNamespaceContext()
      : tenant_id_(OB_INVALID_TENANT_ID),
        namespace_id_(common::OB_INVALID_ID),
        recycle_ts_us_(0),
        already_recycled_(false),
        allocator_(nullptr),
        pl_ctx_(nullptr)
    {}
    uint64_t                   tenant_id_;
    common::ObString           collection_id_;   // CHAR(32) uuid string
    uint64_t                   namespace_id_;
    common::ObString           db_name_;
    common::ObString           namespace_name_;  // original name read from row
    common::ObString           hot_table_name_;  // <collection_id>_hot_table
    common::ObString           logic_schema_table_name_; // <collection_id>_logic_schema_table
    common::ObSqlString        pending_drop_namespace_name_;
    int64_t                    recycle_ts_us_;   // timestamp used in __recyclebin_<orig>_<ts_us>
    bool                       already_recycled_; // true when row already starts with __recyclebin_
    common::ObMySQLTransaction trans_;
    common::ObIAllocator      *allocator_;
    ObPLExecCtx               *pl_ctx_;
  };

  struct DropLtableContext
  {
    DropLtableContext()
      : tenant_id_(OB_INVALID_TENANT_ID),
        namespace_id_(common::OB_INVALID_ID),
        ltable_id_(common::OB_INVALID_ID),
        recycle_ts_us_(0),
        namespace_already_recycled_(false),
        ltable_already_recycled_(false),
        allocator_(nullptr),
        pl_ctx_(nullptr)
    {}
    uint64_t                   tenant_id_;
    common::ObString           collection_id_;
    uint64_t                   namespace_id_;
    uint64_t                   ltable_id_;
    common::ObString           db_name_;
    common::ObSqlString        escaped_db_name_;
    common::ObString           ltable_name_;
    common::ObString           logic_schema_table_name_;
    common::ObSqlString        pending_drop_ltable_name_;
    int64_t                    recycle_ts_us_;
    bool                       namespace_already_recycled_;
    bool                       ltable_already_recycled_;
    common::ObMySQLTransaction trans_;
    common::ObIAllocator      *allocator_;
    ObPLExecCtx               *pl_ctx_;
  };

  // prewarm helpers
  static int build_prewarm_context(ObPLExecCtx &ctx,
                                   const common::ObString &collection_id,
                                   uint64_t namespace_id,
                                   PrewarmContext &prewarm_ctx);
  static int fetch_collection_meta(PrewarmContext &ctx);
  static int build_collection_related_table_names(PrewarmContext &ctx);
  static int fetch_namespace_meta(PrewarmContext &ctx);
  static int check_namespace_not_in_dropping_state(PrewarmContext &ctx);
  static int check_namespace_in_hot_table(PrewarmContext &ctx);
  static int update_hot_table_last_access_time(PrewarmContext &ctx);
  static int calc_partition_and_tablet_by_namespace_id(
      PrewarmContext &ctx,
      const share::schema::ObTableSchema &kv_table_schema);
  static int resolve_tablet_location(PrewarmContext &ctx, bool force_renew = false);
  static int build_prewarm_rpc_arg(PrewarmContext &ctx,
                                   obrpc::ObPrewarmLogicTableArg &rpc_arg);
  // target_addrs contains one leader in the new protocol and legacy fanout
  // targets during rolling upgrade. Preserve the batch RPC framework for both.
  // last_rpc_err preserves the last concrete error after retries are exhausted.
  static int do_one_prewarm_fanout(const obrpc::ObPrewarmLogicTableArg &rpc_arg,
                                   const common::ObIArray<common::ObAddr> &addrs_to_try,
                                   common::ObSEArray<common::ObAddr, 4> &succeeded_addrs,
                                   bool leader_only,
                                   bool &need_retry,
                                   int &last_rpc_err);
  static int resolve_and_send_prewarm_rpc_with_retry(PrewarmContext &ctx);
  static int insert_hot_table_record(PrewarmContext &ctx);
  static int gather_ivf_prewarm_params(PrewarmContext &ctx);
  // Query sdk_ltables for all ltable_id values under (collection_id, namespace_id).
  // Prewarm semantics is "warm the whole namespace", so we always collect all
  // ltable_ids instead of honoring a stale @ltable_id on the session.
  static int collect_ltable_ids_for_namespace_(const PrewarmContext &ctx,
                                               common::ObIArray<uint64_t> &ltable_ids);
  static int append_quoted_identifier_(const common::ObString &identifier,
                                       common::ObSqlString &sql);

  // drop_namespace helpers
  static int build_drop_namespace_context(ObPLExecCtx &ctx,
                                          const common::ObString &collection_id,
                                          uint64_t namespace_id,
                                          DropNamespaceContext &dns_ctx);
  // Validate collection_id exists in sdk_collections (read inside trans for snapshot consistency).
  static int check_collection_exists_in_trans(DropNamespaceContext &ctx);
  // SELECT ... FOR UPDATE on sdk_namespaces row, fill ctx.namespace_name_ /
  // ctx.already_recycled_.  Returns OB_ENTRY_NOT_EXIST if the row is missing.
  static int lock_namespace_row_and_load_state(DropNamespaceContext &ctx);
  // UPDATE sdk_namespaces SET namespace_name = '__recyclebin_<orig>_<ts_us>'
  // with idempotency guard.  Builds ctx.pending_drop_namespace_name_ first.
  static int rename_namespace_to_recyclebin(DropNamespaceContext &ctx);
  // DELETE FROM sdk_ltables WHERE collection_id=? AND namespace_id=?.
  // The synchronous catalog stage drops the ltable mapping rows outright so SDK
  // clients observe the namespace as gone immediately; the background namespace
  // deletion task reclaims physical data later.
  static int delete_ltables_for_namespace(DropNamespaceContext &ctx);
  // DELETE FROM sdk_namespace_stat WHERE collection_id=? AND namespace_id=?.
  // Best-effort: missing table or zero affected rows is not an error.
  static int cleanup_namespace_stats(DropNamespaceContext &ctx);
  // DELETE FROM <collection_id>_logic_schema_table WHERE namespace_id=?.
  // The schema table always exists for namespace-mode collections (created at
  // create_collection time by the SDK), so a missing table is treated as a hard
  // error and triggers a transaction rollback.
  static int delete_logic_schema_rows(DropNamespaceContext &ctx);
  // DELETE FROM <collection_id>_hot_table WHERE namespace_id=?.  Skipped
  // entirely in non-shared-storage mode (hot_table is SS-only).  In SS mode,
  // missing table is best-effort: in case the collection was created before
  // SS-mode was enabled.
  static int delete_hot_table_row(DropNamespaceContext &ctx);
  // Top-level transaction orchestrator.
  static int execute_drop_namespace_trans(DropNamespaceContext &ctx);

  // drop_ltable helpers
  static int build_drop_ltable_context(ObPLExecCtx &ctx,
                                       const common::ObString &collection_id,
                                       uint64_t namespace_id,
                                       uint64_t ltable_id,
                                       DropLtableContext &dlt_ctx);
  static int check_collection_exists_in_trans(DropLtableContext &ctx);
  static int lock_namespace_row_for_drop_ltable(DropLtableContext &ctx);
  static int lock_ltable_row_and_load_state(DropLtableContext &ctx);
  static int rename_ltable_to_recyclebin(DropLtableContext &ctx);
  static int delete_logic_schema_rows_for_ltable(DropLtableContext &ctx);
  static int execute_drop_ltable_trans(DropLtableContext &ctx);
  static int escape_sql_literal_(const common::ObString &value,
                                 common::ObSqlString &escaped);
  static int escape_sql_identifier_(const common::ObString &identifier,
                                    common::ObSqlString &escaped);
  static bool is_benign_stale_exec_errcode_(int errcode);
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_PL_SYS_PACKAGE_OB_DBMS_LOGIC_TABLE_MYSQL_H_
