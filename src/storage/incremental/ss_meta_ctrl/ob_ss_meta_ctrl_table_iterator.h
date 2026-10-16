/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBSE_SHARED_STORAGE_SS_META_CTRL_TABLE_ITERATOR_H_
#define OCEANBSE_SHARED_STORAGE_SS_META_CTRL_TABLE_ITERATOR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"

namespace oceanbase
{
namespace storage
{

// init() parameter ordering convention for the iterator family in this file:
//
//   Row iterators (Table / Value / LockValue):
//     init(sql_client, sql_timeout_us, tenant_id, ls_id, [iter_type,] meta_key_prefix)
//     - SQL plumbing first (proxy + timeout), business filters after.
//
//   Key/aggregate iterators (TableKey / Tenant / LS / Tablet):
//     init(tenant_id, [ls_id,] sql_client, sql_timeout_us, ...)
//     - tenant_id first because it is also used to route the SELECT to the
//       correct storage tenant via get_real_tenant_id_; SQL plumbing follows.
//
// Both groups keep (sql_client, sql_timeout_us) adjacent so that the SQL
// plumbing pair is always passed together. Callers should not reorder these.
//
// Streaming iterator over rows of __all_ss_meta_ctrl_table.
//
// Filter semantics:
//   tenant_id / ls_id                mandatory; matched exactly
//   iter_type CTRL_VALUE             excludes every *_LOCK_ROW type
//             LOCK_VALUE             matches every *_LOCK_ROW type
//             TABLET_LOCK_VALUE      matches TABLET_LOCK_ROW exactly
//             MAJOR_SSTABLE_VALUE    matches MAJOR_SSTABLE exactly
//             WHOLE_VALUE            no meta_type predicate
//   meta_key_prefix.empty()          => no key filter (all rows for the tuple)
//                  non-empty         => append_meta_key_like_prefix_:
//                                      meta_key LIKE '<escaped_prefix>%' ESCAPE '!'
//                                      (anchored prefix match, not substring)
//
// The SELECT is issued lazily on the first get_next() call. The iterator owns
// the underlying ReadResult on the heap (24KB buffer); reset() / dtor releases
// it.
//
// Typical usage:
//   ObSSMetaCtrlTableIterator iter;
//   if (OB_FAIL(iter.init(*GCTX.sql_proxy_, sql_timeout_us, tenant_id, ls_id,
//                         type, prefix))) {
//     ...
//   }
//   ObArenaAllocator allocator("SSMetaCtrlIt");
//   while (OB_SUCC(ret)) {
//     allocator.reset();
//     if (OB_FAIL(iter.get_next(alloc, t, ls, mt, key, val))) {
//       if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
//       LOG_WARN("...", K(ret));
//     } else {
//       // process row
//     }
//   }

struct ObSSMetaCtrlTableIteratorType final
{
public:
  enum TYPE
  {
    INVALID = -1,
    CTRL_VALUE = 0,
    LOCK_VALUE = 1,
    WHOLE_VALUE = 2,
    TABLET_LOCK_VALUE = 3,
    MAJOR_SSTABLE_VALUE = 4,
    MAX,
  };
  ObSSMetaCtrlTableIteratorType(const ObSSMetaCtrlTableIteratorType::TYPE &type) : type_(type) {}
  ~ObSSMetaCtrlTableIteratorType() = default;
  void reset() { type_ = INVALID; }
  bool is_valid() const { return type_ > INVALID && type_ < MAX; }
  TO_STRING_KV(K_(type));
  TYPE type_;
};

class ObSSMetaCtrlTableIterator final
{
public:
  ObSSMetaCtrlTableIterator();
  ~ObSSMetaCtrlTableIterator();

  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlTableIteratorType &type,
      const common::ObString &meta_key_prefix);

  // Fetch the next row. meta_key / meta_value are deep-copied into `allocator`.
  // Returns OB_ITER_END after the last row.
  int get_next(
      common::ObIAllocator &allocator,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      blocksstable::ObStorageObjectType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value);
  // Same as above but additionally yields the auto-managed timestamp columns
  // gmt_create / gmt_modified (microseconds). Returns OB_ITER_END after the
  // last row.
  int get_next(
      common::ObIAllocator &allocator,
      int64_t &gmt_create,
      int64_t &gmt_modified,
      uint64_t &tenant_id,
      share::ObLSID &ls_id,
      blocksstable::ObStorageObjectType &meta_type,
      common::ObString &meta_key,
      common::ObString &meta_value);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id), K_(ls_id),
               K_(iter_type), "meta_key_prefix", meta_key_prefix_.string(),
               K_(returned_row_count));

private:
  int open_();
  int build_select_sql_(common::ObSqlString &sql);
  int build_filter_condition_(common::ObSqlString &sql);
  void release_result_();

private:
  bool is_inited_;
  bool opened_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaCtrlTableIteratorType iter_type_;
  common::ObSqlString meta_key_prefix_;       // deep-copied on init
  common::ObMySQLProxy::ReadResult *result_;  // heap-allocated, owned
  ObSSMetaCtrlTableOperator operator_;        // friend; reused for sql/parse
  int64_t returned_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTableIterator);
};

class ObSSMetaCtrlValueIterator final
{
public:
  ObSSMetaCtrlValueIterator();
  ~ObSSMetaCtrlValueIterator();
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObString &meta_key_prefix);
  // Fetch the next (ctrl_key, ctrl_value) pair. tenant_id / ls_id reflect the
  // underlying row columns; for a single iterator instance they always equal
  // the values passed to init(). Returns OB_ITER_END after the last row.
  int get_next(uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  // Same as above but additionally yields the auto-managed timestamp columns
  // gmt_create / gmt_modified (microseconds) of the underlying row. Returns
  // OB_ITER_END after the last row.
  int get_next(int64_t &gmt_create,
               int64_t &gmt_modified,
               uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  // Same as above but only returns the ctrl key and value; tenant_id / ls_id
  // are discarded. Returns OB_ITER_END after the last row.
  int get_next(ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
private:
  bool is_inited_;
  ObSSMetaCtrlTableIterator table_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlValueIterator);
};

// Dedicated iterator for MAJOR_SSTABLE control rows. Unlike
// ObSSMetaCtrlValueIterator, this iterator pushes an exact
// `meta_type = MAJOR_SSTABLE` predicate into SQL so callers do not scan and
// deserialize unrelated control rows under the same (tenant_id, ls_id, key
// prefix).
class ObSSMetaCtrlMajorSSTableValueIterator final
{
public:
  ObSSMetaCtrlMajorSSTableValueIterator();
  ~ObSSMetaCtrlMajorSSTableValueIterator();
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObString &meta_key_prefix);
  int get_next(uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  int get_next(int64_t &gmt_create,
               int64_t &gmt_modified,
               uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);
  int get_next(ObSSMetaCtrlKey &ctrl_key,
               ObSSMetaCtrlValue &ctrl_value);

private:
  bool is_inited_;
  ObSSMetaCtrlTableIterator table_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlMajorSSTableValueIterator);
};

class ObSSMetaCtrlLockValueIterator final
{
public:
  ObSSMetaCtrlLockValueIterator();
  ~ObSSMetaCtrlLockValueIterator();
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObString &meta_key_prefix);
  // Fetch the next row. tenant_id / ls_id / meta_key reflect the underlying
  // row columns; for a single iterator instance tenant_id and ls_id always
  // equal the values passed to init(). meta_key is the deserialized
  // ObSSMetaCtrlKey of the row. Returns OB_ITER_END after the last row.
  int get_next(uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &meta_key,
               ObSSMetaCtrlLockRowValue &lock_row_value);
  // Same as above but only returns the lock row value; tenant_id / ls_id /
  // meta_key are discarded. Returns OB_ITER_END after the last row.
  int get_next(ObSSMetaCtrlLockRowValue &lock_row_value);
private:
  bool is_inited_;
  ObSSMetaCtrlTableIterator table_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlLockValueIterator);
};

class ObSSMetaCtrlTabletLockValueIterator final
{
public:
  ObSSMetaCtrlTabletLockValueIterator();
  ~ObSSMetaCtrlTabletLockValueIterator();
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObString &meta_key_prefix);
  // 获取给定 (tenant_id, ls_id) 下的下一条 TABLET_LOCK_ROW；当 meta_key_prefix
  // 非空时，按前缀 LIKE 语义（meta_key LIKE '<meta_key_prefix>%' ESCAPE '!'）
  // 进行过滤，即匹配以该前缀开头的 meta_key。tenant_id / ls_id / meta_key 反映
  // 底层行的列值；对于单个迭代器实例，tenant_id 和 ls_id 始终等于传给 init() 的
  // 值。meta_key 是该行反序列化得到的 ObSSMetaCtrlKey。最后一行之后返回
  // OB_ITER_END。
  int get_next(uint64_t &tenant_id,
               share::ObLSID &ls_id,
               ObSSMetaCtrlKey &meta_key,
               ObSSMetaCtrlLockRowValue &lock_row_value);
  // Same as above but only returns the TABLET_LOCK_ROW key and value;
  // tenant_id / ls_id are discarded. Returns OB_ITER_END after the last row.
  int get_next(ObSSMetaCtrlKey &meta_key,
               ObSSMetaCtrlLockRowValue &lock_row_value);
  // Same as above but only returns the lock row value; tenant_id / ls_id /
  // meta_key are discarded. Returns OB_ITER_END after the last row.
  int get_next(ObSSMetaCtrlLockRowValue &lock_row_value);
private:
  bool is_inited_;
  ObSSMetaCtrlTableIterator table_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTabletLockValueIterator);
};

// Streaming iterator over a single key column of __all_ss_meta_ctrl_table
// with optional DISTINCT and ORDER BY. Used as the shared implementation of
// the per-column iterators (tenant_id / ls_id / meta_key / ...).
//
// Column type:
//   INT_COLUMN     fetch via get_int(); use the int overload of get_next().
//   VARCHAR_COLUMN fetch via get_varchar(); use the ObString overload of
//                  get_next() and supply an allocator (deep-copied output).
//
// Filter semantics:
//   tenant_id        mandatory; used only to route the SELECT to the correct
//                    storage tenant via get_real_tenant_id_. It is NOT added
//                    as a WHERE predicate automatically -- if the caller wants
//                    WHERE tenant_id = X, pass it through extra_where.
//   column_name      mandatory; the column on which DISTINCT / ORDER BY apply.
//                    Must be a caller-owned literal that outlives the iterator.
//   extra_where      optional; appended verbatim after " WHERE ". Empty string
//                    means no predicate. Callers are responsible for supplying
//                    a safe SQL fragment (numeric equality, no user strings).
//   distinct         when true, emits SELECT DISTINCT; when false, plain SELECT.
//                    Set to false for varchar columns whose rows are already
//                    unique by storage rowkey (e.g. meta_key) and where the
//                    caller will dedupe in C++.
class ObSSMetaCtrlTableKeyIterator final
{
public:
  enum ColumnType : int8_t
  {
    INVALID_COLUMN_TYPE = 0,
    INT_COLUMN = 1,
    VARCHAR_COLUMN = 2,
  };

  ObSSMetaCtrlTableKeyIterator();
  ~ObSSMetaCtrlTableKeyIterator();

  // Pick the matching get_next overload below for the chosen column_type.
  int init(const uint64_t tenant_id,
           common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us,
           const ColumnType column_type,
           const bool distinct,
           const common::ObString &column_name,
           const common::ObString &extra_where_clause);
  // Returns OB_ITER_END after the last row. Returns OB_ERR_UNEXPECTED if
  // column_type_ is not INT_COLUMN.
  int get_next(int64_t &value);
  // Same as above for VARCHAR_COLUMN; deep-copies the value into `allocator`.
  // Returns OB_ERR_UNEXPECTED if column_type_ is not VARCHAR_COLUMN.
  int get_next(common::ObIAllocator &allocator, common::ObString &value);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id),
               K_(column_type), K_(distinct),
               "column_name", column_name_.string(),
               "extra_where_clause", extra_where_clause_.string(),
               K_(returned_row_count));

private:
  int open_();
  int build_select_sql_(common::ObSqlString &sql);
  void release_result_();

private:
  bool is_inited_;
  bool opened_;
  uint64_t tenant_id_;                        // routed via get_real_tenant_id_
  ColumnType column_type_;
  bool distinct_;
  common::ObSqlString column_name_;           // deep-copied on init; null-terminated via ptr()
  common::ObSqlString extra_where_clause_;    // deep-copied on init
  common::ObMySQLProxy::ReadResult *result_;  // heap-allocated, owned
  ObSSMetaCtrlTableOperator operator_;        // friend; reused for sql exec
  int64_t returned_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTableKeyIterator);
};

// Streaming iterator that yields the tenant_id values whose rows the input
// tenant is allowed to see in __all_ss_meta_ctrl_table. Emission depends on the
// input tenant type:
//   input sys         => every distinct tenant_id present in the sys storage
//                        table (sys itself + each meta tenant) AND, for each
//                        meta tenant found there, the distinct tenant_id values
//                        actually stored in that meta tenant's table (its user
//                        tenant rows). Each id is produced at most once.
//   input meta tenant => only itself.
//   input user tenant => only itself.
//
// Rationale: a user tenant's rows are physically stored in its meta tenant's
// table (user->meta routing in get_real_tenant_id_), so a sys-level scan cannot
// discover user tenants by querying the sys storage table directly. So for the
// sys input we run a two-level enumeration: the outer key_iterator_ lists the
// distinct tenant_id of the sys table; whenever it yields a meta tenant we open
// an inner user_key_iterator_ over that meta tenant's table (reached by routing
// through the paired user tenant id) and drain its distinct tenant_id values
// before advancing the outer iterator. Unlike a fixed meta<->user id
// derivation, this only emits user tenants that actually have rows. meta / user
// inputs yield their single id without any SQL.
//
// NOTE: this is a general-purpose, self-contained tenant enumerator kept for
// callers that need to discover the tenant_id set of __all_ss_meta_ctrl_table
// from inner SQL alone (no schema service / MTL dependency). The
// __all_virtual_shared_meta_control virtual table does NOT use it: that scan
// runs route_policy=local on a single node and resolves its tenant set from the
// schema service (ObAllVirtualSSMetaCtrl::build_tenant_ids_), which also lets it
// skip non-NORMAL (creating / dropping) tenants. As of this writing the only
// in-tree callers are the unit tests; keep the two paths' tenant-discovery
// semantics in mind if a production caller is added later.
class ObSSMetaCtrlTenantIterator final
{
public:
  ObSSMetaCtrlTenantIterator();
  ~ObSSMetaCtrlTenantIterator();

  int init(const uint64_t tenant_id,
           common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us);
  // Returns OB_ITER_END after the last row.
  int get_next(uint64_t &tenant_id);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(is_sys_input), K_(self_tenant_id),
               K_(self_emitted), K_(user_iter_opened), K_(key_iterator),
               K_(user_key_iterator));

private:
  // sys input: open the inner drill-down iterator over the meta tenant's table
  // that physically stores user tenant `user_tenant_id`'s rows.
  int open_user_iter_(const uint64_t user_tenant_id);

private:
  bool is_inited_;
  bool is_sys_input_;                  // input tenant is the sys tenant
  // saved for the sys input's per-meta-tenant inner iterator (re)initialization.
  common::ObISQLClient *sql_client_;
  int64_t sql_timeout_us_;
  // meta / user input: the single id to emit, and whether it was emitted.
  uint64_t self_tenant_id_;
  bool self_emitted_;
  // sys input only.
  ObSSMetaCtrlTableKeyIterator key_iterator_;       // outer: distinct tenant_id of sys table
  ObSSMetaCtrlTableKeyIterator user_key_iterator_;  // inner: distinct tenant_id of a meta tenant's table
  bool user_iter_opened_;                           // whether the inner iterator is active
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTenantIterator);
};

// Streaming iterator that yields distinct ls_id values of a given tenant from
// __all_ss_meta_ctrl_table. The input tenant_id is routed via
// get_real_tenant_id_ to the actual storage tenant, and the SELECT filters by
// that tenant_id column and returns ls_ids in ascending order.
class ObSSMetaCtrlLSIterator final
{
public:
  ObSSMetaCtrlLSIterator();
  ~ObSSMetaCtrlLSIterator();

  int init(const uint64_t tenant_id,
           common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us);
  // Returns OB_ITER_END after the last row.
  int get_next(share::ObLSID &ls_id);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(key_iterator));

private:
  bool is_inited_;
  common::ObSqlString where_clause_;  // owns "tenant_id = <X>" string
  ObSSMetaCtrlTableKeyIterator key_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlLSIterator);
};

// Streaming iterator that yields (tablet_id, reorg_scn) pairs for a given
// (tenant_id, ls_id) tuple, scanning TABLET_LOCK_ROW rows of
// __all_ss_meta_ctrl_table. tablet_id and reorg_scn are NOT table columns;
// they are encoded in the tablet path segments of TABLET_LOCK_ROW meta_key,
// so this iterator reads meta_key under the where
// clause
//   "tenant_id = X AND ls_id = Y AND meta_type = TABLET_LOCK_ROW"
// in meta_key ASC order and deserializes each meta_key on the C++ side to
// extract the pair.
//
// No dedup is performed. Since a lock-row control_scn is fixed at zero, a
// valid table contains at most one row for each (tablet_id, reorg_scn) pair.
class ObSSMetaCtrlTabletIterator final
{
public:
  ObSSMetaCtrlTabletIterator();
  ~ObSSMetaCtrlTabletIterator();

  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us);
  // Returns OB_ITER_END after the last row.
  int get_next(common::ObTabletID &tablet_id, share::SCN &reorg_scn);
  void reset();

  TO_STRING_KV(K_(is_inited),
               "where_clause", where_clause_.string(),
               K_(key_iterator));

private:
  bool is_inited_;
  common::ObSqlString where_clause_;       // owns "tenant_id = X AND ls_id = Y AND meta_type = TABLET_LOCK_ROW"
  ObSSMetaCtrlTableKeyIterator key_iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlTabletIterator);
};

// 按主键流式读取指定 tenant_id 的 __all_shared_meta_control 数据。
// 返回顺序为 (meta_type, ls_id, meta_key) 升序；每次 get_next() 都会把
// meta_key 以及可选的 meta_value 深拷贝到 allocator。
//
// 典型用法：
//   ObSSMetaCtrlRowkeyIterator iter;
//   if (OB_FAIL(iter.init(tenant_id, *GCTX.sql_proxy_, sql_timeout_us))) {
//     ...
//   }
//   ObArenaAllocator allocator("SSMetaCtrlRKIt");
//   while (OB_SUCC(ret)) {
//     allocator.reset();
//     if (OB_FAIL(iter.get_next(allocator, t, mt, ls, key))) {
//       if (OB_ITER_END == ret) { ret = OB_SUCCESS; break; }
//       LOG_WARN("...", K(ret));
//     } else {
//       // 处理 rowkey
//     }
//   }
class ObSSMetaCtrlRowkeyIterator final
{
public:
  ObSSMetaCtrlRowkeyIterator();
  ~ObSSMetaCtrlRowkeyIterator();

  int init(const uint64_t tenant_id,
           common::ObISQLClient &sql_client,
           const int64_t sql_timeout_us);
  // Returns OB_ITER_END after the last row.
  int get_next(common::ObIAllocator &allocator,
               uint64_t &tenant_id,
               blocksstable::ObStorageObjectType &meta_type,
               share::ObLSID &ls_id,
               common::ObString &meta_key);
  // 返回完整行，供需要时间戳和 meta_value 的调用方使用。
  int get_next(common::ObIAllocator &allocator,
               int64_t &gmt_create,
               int64_t &gmt_modified,
               uint64_t &tenant_id,
               blocksstable::ObStorageObjectType &meta_type,
               share::ObLSID &ls_id,
               common::ObString &meta_key,
               common::ObString &meta_value);
  // Advance the cursor by one row without returning rowkey columns.
  // Returns OB_ITER_END after the last row.
  int get_next();
  void reset();

  TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id), K_(returned_row_count));

private:
  int open_();
  int build_select_sql_(common::ObSqlString &sql);
  void release_result_();

private:
  bool is_inited_;
  bool opened_;
  uint64_t tenant_id_;
  common::ObMySQLProxy::ReadResult *result_;
  ObSSMetaCtrlTableOperator operator_;
  int64_t returned_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaCtrlRowkeyIterator);
};


} //storage
} //oceanbase

#endif /* OCEANBSE_SHARED_STORAGE_SS_META_CTRL_TABLE_ITERATOR_H_ */
