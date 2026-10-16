/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_MGR
#define OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_MGR

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/scn.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"

namespace oceanbase
{
namespace storage
{

// Tenant-private manager for the __all_shared_meta_reserved_scn table.
//
// Wraps ObSharedMetaReservedOperator with tenant-bound semantics so callers
// don't need to thread tenant_id / sql_client through every operation. The
// manager is stateful: init() binds the tenant, after which create / remove /
// get_max operate on that tenant only.
class ObSharedMetaReservedMgr final
{
public:
  ObSharedMetaReservedMgr();
  ~ObSharedMetaReservedMgr() = default;

  int init(const uint64_t tenant_id);
  void reset();

  // Insert the MAX_RESERVED control row for the bound tenant:
  //   (tenant_id_, MAX_RESERVED, SCN::min_scn())
  int create_meta_row(common::ObMySQLTransaction &trans);

  // Insert one reservation row for `(tenant_id_, type, meta_scn)` in a private
  // autocommit transaction. Internally takes the EXCLUSIVE table lock, rejects
  // `meta_scn <= gc_meta_scn` with OB_OP_NOT_ALLOW (the scn has already been or
  // is about to be GC'd), advances MAX_RESERVED, then inserts the row. Duplicate
  // primary key is propagated as an SQL error.
  int create_reserved_scn(
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);
  // Same as above but runs inside the caller's transaction (the caller commits).
  // `trans` must already be started against the real tenant (see
  // ObSharedMetaReservedUtil::get_real_tenant_id). The caller is responsible for
  // ordering this before any in-trans advance of gc_meta_scn, otherwise the
  // gc_meta_scn guard would reject its own reservation.
  int create_reserved_scn(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);

  // 创建owner-keyed保留行, 使用GTS作为meta_scn。GTS在拿到EXCLUSIVE表锁和
  // gc控制行锁之后才获取, 因此不可能小于已提交的gc_meta_scn(违反该不变式
  // 返回OB_ERR_UNEXPECTED)。成功时通过meta_scn返回实际写入的GTS；失败时
  // meta_scn无效。主键或owner唯一键冲突会透传给调用方。
  int create_reserved_scn(
      const ObSharedMetaReservedType &type,
      const common::ObString &owner,
      share::SCN &meta_scn);
  int create_reserved_scn(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedType &type,
      const common::ObString &owner,
      share::SCN &meta_scn);

  // 删除 `(tenant_id_, type, meta_scn)` 对应的 reservation 行。DELETE 前会在
  // 同一事务内获取 reserved 表 EXCLUSIVE 锁，以固定 TABLE -> TABLET 锁序。
  int remove_reserved_scn(
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);
  // 在调用方事务内删除 reservation 行。`trans` 必须已在
  // gen_meta_tenant_id(tenant_id_) 上启动；接口会先获取 reserved 表
  // EXCLUSIVE 锁，再执行 DELETE。
  int remove_reserved_scn(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);

  // 删除 `(tenant_id_, type, owner)` 对应的 owner-keyed reservation 行。
  // 缺失行按幂等成功处理；DELETE 前会在同一事务内获取 reserved 表 EXCLUSIVE 锁。
  int remove_reserved_scn(
      const ObSharedMetaReservedType &type,
      const common::ObString &owner);
  int remove_reserved_scn(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedType &type,
      const common::ObString &owner);

  // Read the meta_scn from the MAX_RESERVED control row of `tenant_id_`.
  // Acquires a SHARE table lock inside `trans` before the read when
  // `need_lock` is true so the result observes a stable snapshot relative to
  // concurrent create.
  // `trans` must already be
  // started against gen_meta_tenant_id(tenant_id_); the caller is responsible
  // for committing or rolling back.
  int get_max_reserved_scn(
      const bool need_lock,
      common::ObMySQLTransaction &trans,
      share::SCN &max_meta_scn);

private:
  // Map the bound `tenant_id_` to the tenant that actually stores
  // __all_shared_meta_reserved_scn (where the trans runs and the table lock is
  // taken): user -> its meta tenant, meta/sys -> SYS tenant.
  int get_real_tenant_id_(uint64_t &real_tenant_id) const;
  // Start `trans` against the SQL execution tenant for the bound tenant.
  int start_trans_(common::ObMySQLTransaction &trans);
  // End `trans`. Commits when `result` == OB_SUCCESS, otherwise rolls back.
  // The end-result is folded into `result`: if commit fails, it overwrites
  // `result` only when `result` was previously OB_SUCCESS.
  int commit_trans_(
      common::ObMySQLTransaction &trans,
      int &result);
  // Acquire a transaction-scoped table write lock on
  // __all_shared_meta_reserved_scn.
  int lock_table_in_exclusive_(common::ObMySQLTransaction &trans);
  // Acquire a transaction-scoped table shared lock on
  // __all_shared_meta_reserved_scn.
  int lock_table_in_share_(common::ObMySQLTransaction &trans);
  // Build the MAX_RESERVED row info for the bound tenant:
  //   (tenant_id_, MAX_RESERVED, meta_scn)
  int prepare_max_reserved_info_(
      const share::SCN &meta_scn,
      ObSharedMetaReservedInfo &info) const;
  // Read the MAX_RESERVED row for the bound tenant from
  // __all_shared_meta_reserved_scn.
  int get_max_reserved_info_(ObSharedMetaReservedInfo &info) const;
  int get_max_reserved_info_(
      common::ObISQLClient &sql_client,
      ObSharedMetaReservedInfo &info) const;
  // Advance MAX_RESERVED up when `meta_scn` is greater than `info.meta_scn_`;
  // otherwise the high water mark is left unchanged.
  int update_max_reserved_info_(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedInfo &info,
      const share::SCN &meta_scn);
  int create_reserved_scn_(
      common::ObMySQLTransaction &trans,
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);
  int remove_reserved_scn_(
      common::ObISQLClient &sql_client,
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);
  int remove_reserved_scn_(
      common::ObISQLClient &sql_client,
      const ObSharedMetaReservedType &type,
      const common::ObString &owner);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  static constexpr int64_t SHARED_META_RESERVED_SQL_TIMEOUT_US = 10 * 1000 * 1000; // 10s
  DISALLOW_COPY_AND_ASSIGN(ObSharedMetaReservedMgr);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_MGR
