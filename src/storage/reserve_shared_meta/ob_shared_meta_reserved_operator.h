/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_OPERATOR
#define OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_OPERATOR

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"

namespace oceanbase
{
namespace storage
{

// Forward declaration needed for friend.
class ObSharedMetaReservedIterator;

// SQL operator for the __all_shared_meta_reserved_scn table.
//
// All operations are tenant-private. Each row of the table is a
// reservation placeholder; see ObSharedMetaReservedInfo for the row
// semantics. The operator is *stateless* aside from its SQL client
// pointer; concurrent callers may share a single instance.
class ObSharedMetaReservedOperator final
{
  friend class ObSharedMetaReservedIterator;
public:
  ObSharedMetaReservedOperator();
  ~ObSharedMetaReservedOperator() = default;
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us);
  void reset();

  // Insert one reservation row.
  // Duplicate-key errors from SQL are propagated to the caller.
  int insert_row(
      const ObSharedMetaReservedInfo &info,
      int64_t &affected_rows);

  // Delete the row whose primary key exactly matches `info`.
  int delete_row(
      const ObSharedMetaReservedInfo &info,
      int64_t &affected_rows);

  // Delete the unique owner-keyed row for `(tenant_id, type, owner)`.
  int delete_row(
      const uint64_t tenant_id,
      const ObSharedMetaReservedType &type,
      const common::ObString &owner,
      int64_t &affected_rows);

  // Scan rows for `tenant_id`. When `type.is_valid()` is true the scan is
  // also filtered by `type`; otherwise rows of all types are returned.
  // Results are appended to `infos` (the array is NOT cleared).
  int scan_rows(
      const uint64_t tenant_id,
      const ObSharedMetaReservedType &type,
      const int64_t limit_count,
      common::ObIArray<ObSharedMetaReservedInfo> &infos);

private:
  // Parse one row at the result's current position into `info`. Caller is
  // responsible for advancing the cursor via result.next() before calling.
  static int parse_row_(
      common::sqlclient::ObMySQLResult &result,
      ObSharedMetaReservedInfo &info);
  int get_real_tenant_id_(
      const uint64_t tenant_id,
      uint64_t &real_tenant_id);
  int get_sql_timeout_us_(int64_t &timeout_us);
  int do_sql_write_(
      const uint64_t tenant_id,
      const char *sql,
      int64_t &affected_rows);
  int do_sql_read_(
      const uint64_t tenant_id,
      const char *sql,
      common::ObMySQLProxy::MySQLResult &result);

private:
  bool is_inited_;
  common::ObISQLClient *sql_client_;
  int64_t sql_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedMetaReservedOperator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_OPERATOR
