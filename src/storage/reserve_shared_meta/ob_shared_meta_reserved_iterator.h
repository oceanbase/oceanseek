/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_ITERATOR
#define OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_ITERATOR

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"

namespace oceanbase
{
namespace storage
{

// Streaming row-at-a-time iterator over __all_shared_meta_reserved_scn for
// one tenant. Control rows (MAX_RESERVED) are filtered out: get_next() only
// yields real reservation rows (BACKUP, SNAPSHOT, GC, ...).
//
// The SELECT is issued lazily on the first get_next() call. The iterator
// owns the underlying ReadResult on the heap; reset() / dtor releases it.
//
// Typical usage:
//   ObSharedMetaReservedIterator iter;
//   if (OB_FAIL(iter.init(tenant_id, sql_timeout_us))) { ... }
//   ObSharedMetaReservedInfo info;
//   while (OB_SUCC(ret)) {
//     if (OB_FAIL(iter.get_next(info))) {
//       if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
//       LOG_WARN("...", K(ret));
//     }
//     // process info
//   }
class ObSharedMetaReservedIterator final
{
public:
  ObSharedMetaReservedIterator();
  ~ObSharedMetaReservedIterator();

  int init(
      const uint64_t tenant_id,
      const int64_t sql_timeout_us);

  // Returns OB_SUCCESS + info, OB_ITER_END when exhausted, or error.
  int get_next(ObSharedMetaReservedInfo &info);

  void reset();

  TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id), K_(sql_timeout_us),
               K_(returned_row_count));

private:
  int open_();
  int build_select_sql_(common::ObSqlString &sql);
  void release_result_();

  bool is_inited_;
  bool opened_;
  uint64_t tenant_id_;
  uint64_t real_tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObMySQLProxy::ReadResult *result_;  // heap-allocated, owned
  int64_t sql_timeout_us_;
  int64_t returned_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedMetaReservedIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_ITERATOR
