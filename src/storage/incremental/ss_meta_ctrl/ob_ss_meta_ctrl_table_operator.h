/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SS_META_CTRL_TABLE_OPERATOR_H_
#define OCEANBASE_SHARED_STORAGE_SS_META_CTRL_TABLE_OPERATOR_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "share/ob_dml_sql_splicer.h"
#include "storage/incremental/sslog/ob_sslog_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaCtrlPageIterator;

class ObSSMetaCtrlTableOperator final
{
public:
  friend class ObSSMetaCtrlTableIterator;
  friend class ObSSMetaCtrlTableKeyIterator;
  friend class ObSSMetaCtrlRowkeyIterator;
  friend class ObSSMetaCtrlPageIterator;
  ObSSMetaCtrlTableOperator();
  ~ObSSMetaCtrlTableOperator() = default;
  // sql_timeout_us: caller-specified upper bound (us) for QUERY_TIMEOUT hint.
  // The effective timeout is further capped by THIS_WORKER.get_timeout_remain()
  // when a worker timeout is set.
  int init(common::ObISQLClient &sql_client, const int64_t sql_timeout_us);
  int insert_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType meta_type,
      const common::ObString &meta_key,
      const common::ObString &meta_value,
      int64_t &affected_rows);
  int update_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType meta_type,
      const common::ObString &meta_key,
      const common::ObString &meta_value,
      int64_t &affected_rows);
  // When prefix_delete is true, meta_key is a serialized key prefix and
  // delete uses append_meta_key_like_prefix_ (meta_key LIKE '<prefix>%').
  int delete_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType meta_type,
      const common::ObString &meta_key,
      const bool prefix_delete,
      int64_t &affected_rows);
  // delete_rows_from_rowkey: delete up to `delete_limit_count` rows whose
  // primary key (tenant_id, meta_type, ls_id, meta_key) is >= the given
  // rowkey, ordered by primary key ascending.
  int delete_rows_from_rowkey(
      const uint64_t tenant_id,
      const blocksstable::ObStorageObjectType meta_type,
      const share::ObLSID &ls_id,
      const common::ObString &meta_key,
      const int64_t delete_limit_count,
      int64_t &affected_rows);
  int read_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType meta_type,
      const common::ObString &meta_key,
      common::ObIAllocator &allocator,
      common::ObString &meta_value);
  int lock_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const blocksstable::ObStorageObjectType meta_type,
      const common::ObString &meta_key,
      common::ObIAllocator &allocator,
      common::ObString &meta_value);
  void reset();

private:
  int parse_result_whole_row_(
      common::ObMySQLProxy::MySQLResult &result,
      common::ObIAllocator &allocator,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      blocksstable::ObStorageObjectType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value);
  // Same as above but additionally extracts the auto-managed timestamp columns
  // gmt_create / gmt_modified (microseconds). Used by callers that need row
  // metadata. The 6-arg overload forwards to this with discarded timestamps.
  int parse_result_whole_row_(
      common::ObMySQLProxy::MySQLResult &result,
      common::ObIAllocator &allocator,
      int64_t &gmt_create,
      int64_t &gmt_modified,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      blocksstable::ObStorageObjectType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value);
  int parse_result_meta_value_(
      common::ObMySQLProxy::MySQLResult &result,
      common::ObIAllocator &allocator,
      common::ObString &meta_value);
  // append_meta_key_like_prefix_: append a prefix LIKE predicate fragment
  //   " LIKE '<escaped_prefix>%' ESCAPE '!'"
  // to `sql`. Only a trailing '%' wildcard is added after `prefix`, so meta_key
  // must start with `prefix` (anchored prefix match, not a substring match). The
  // '%', '_', '!' bytes in `prefix` are escaped with '!' so they are matched
  // literally. Caller must already have appended the column reference (e.g.
  // " AND meta_key") before calling this. Shared by delete_row(prefix_delete=true)
  // and ObSSMetaCtrlTableIterator.
  int append_meta_key_like_prefix_(
      const common::ObString &prefix,
      common::ObSqlString &sql);
  // append_meta_key_gt_: append " AND meta_key > '<meta_key>'" to `sql`.
  // Uses the same '%.*s' literal style as read_row / delete_rows_from_rowkey.
  int append_meta_key_gt_(
      const common::ObString &meta_key,
      common::ObSqlString &sql);
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
  void free_string_(
      ObString &string);
private:
  bool is_inited_;
  common::ObISQLClient *sql_client_;
  int64_t sql_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTableOperator);
};


} //storage
} //oceanbase




#endif /* OCEANBASE_SHARED_STORAGE_SS_META_CTRL_TABLE_OPERATOR_H_ */
