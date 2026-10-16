/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_ITERATOR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"

namespace oceanbase
{
namespace storage
{

// shared GC 任务表范围扫描和全表扫描 Iterator 的公共基类。负责持有 SQL client、
// 解析后的实际租户以及底层 ReadResult 的生命周期。具体的 WHERE 子句和投影列由
// 子类实现的 build_select_sql_() 提供。
//
// SELECT 在首次调用 fetch_next_row_() 时延迟执行。Iterator 在堆上持有底层
// ReadResult，并在 reset() 或析构时释放。get_next() 由子类实现，因为具体的
// Iterator 对外提供不同的行类型和强类型 value 接口。
class ObSharedGCTaskTableIteratorBase
{
public:
  ObSharedGCTaskTableIteratorBase();
  virtual ~ObSharedGCTaskTableIteratorBase();

  void reset();

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(opened), K_(tenant_id),
                       K_(real_tenant_id), K_(returned_row_count));

protected:
  // Subclass supplies the full SELECT statement (column list + WHERE/ORDER BY).
  virtual int build_select_sql_(common::ObSqlString &sql) = 0;

  // Lazily opens on first call, then advances the cursor by one row.
  // Returns OB_SUCCESS and sets `result` to the current row on success,
  // OB_ITER_END when exhausted, or an error code.
  int fetch_next_row_(common::sqlclient::ObMySQLResult *&result);

  void release_result_();

  bool is_inited_;
  bool opened_;
  uint64_t tenant_id_;
  uint64_t real_tenant_id_;
  common::ObISQLClient *client_;
  common::ObMySQLProxy::ReadResult *result_;  // heap-allocated, owned
  int64_t returned_row_count_;

private:
  int open_();
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCTaskTableIteratorBase);
};

// Streaming row-at-a-time iterator over __all_shared_gc_task.
//
// Filter semantics:
//   tenant_id / type       mandatory, matched exactly
//   round_id               matched exactly to round_id
//   task_id                restricted to [slice_range.start_task_id, slice_range.end_task_id]
//
// The task_id slice is used to split one round into disjoint tasks.
// An empty range (slice_range.start_task_id > slice_range.end_task_id) is allowed and yields zero rows.
class ObSharedGCTaskTableRangeIterator final : public ObSharedGCTaskTableIteratorBase
{
public:
  ObSharedGCTaskTableRangeIterator();
  virtual ~ObSharedGCTaskTableRangeIterator();

  int init(
      const uint64_t tenant_id,
      const ObSharedGCTaskType type,
      const int64_t round_id,
      const ObSSGCTaskIdRange &slice_range,
      common::ObISQLClient &client);

  // Returns OB_SUCCESS + task (deep-copied into allocator),
  // OB_ITER_END when exhausted, or error.
  int get_next(common::ObIAllocator &allocator, ObSharedGCTask &task);

  void reset();

  INHERIT_TO_STRING_KV("base", ObSharedGCTaskTableIteratorBase,
                       K_(type), K_(round_id), K_(slice_range));

protected:
  virtual int build_select_sql_(common::ObSqlString &sql) override;

private:
  ObSharedGCTaskType type_;
  int64_t round_id_;
  ObSSGCTaskIdRange slice_range_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCTaskTableRangeIterator);
};

// type-dimension pushdown filter: types_ is only consulted when
// is_filter_enabled_ is true. The flag and data are bundled so they can
// never drift apart. Empty + enabled is treated the same as disabled (scan all).
struct ObSharedGCTaskTypeFilter final
{
public:
  ObSharedGCTaskTypeFilter() { reset(); }
  ~ObSharedGCTaskTypeFilter() { reset(); }
  void reset() { is_filter_enabled_ = false; types_.reset(); }
  int assign(const ObSharedGCTaskTypeFilter &other);
  bool need_filter() const { return is_filter_enabled_ && !types_.empty(); }
  TO_STRING_KV(K_(is_filter_enabled), K_(types));

public:
  bool is_filter_enabled_;
  common::ObSEArray<ObSharedGCTaskType, 2> types_;
};

// round_id-dimension pushdown filter: the closed interval
// [round_id_low_, round_id_high_] is only consulted when is_filter_enabled_
// is true; otherwise the round_id is unbounded (scan all rounds).
struct ObSharedGCTaskRoundIdFilter final
{
public:
  ObSharedGCTaskRoundIdFilter() { reset(); }
  ~ObSharedGCTaskRoundIdFilter() { reset(); }
  void reset()
  {
    is_filter_enabled_ = false;
    round_id_low_ = INT64_MAX;
    round_id_high_ = 0;
  }
  bool need_filter() const { return is_filter_enabled_; }
  bool is_valid() const { return (!is_filter_enabled_) || (round_id_low_ <= round_id_high_); }
  TO_STRING_KV(K_(is_filter_enabled), K_(round_id_low), K_(round_id_high));

public:
  bool is_filter_enabled_;
  int64_t round_id_low_;
  int64_t round_id_high_;
};

// Full-table-scan iterator over __all_shared_gc_task or
// __all_shared_gc_task_history (selected by table_name at init). It filters on
// tenant_id and, optionally, on type / round_id (predicate pushdown for the
// readable virtual tables), projects gmt_create / gmt_modified in addition to
// the task body, and orders by (type, round_id, task_id). Used by the readable
// virtual tables __all_virtual_shared_gc_task[_history].
class ObSharedGCTaskTableFullIterator final : public ObSharedGCTaskTableIteratorBase
{
public:
  ObSharedGCTaskTableFullIterator();
  virtual ~ObSharedGCTaskTableFullIterator();

  // `table_name` must be OB_ALL_SHARED_GC_TASK_TNAME or
  // OB_ALL_SHARED_GC_TASK_HISTORY_TNAME.
  //
  // Pushdown filters (each carries its own enable flag; a disabled filter means
  // "scan that dimension fully"):
  //   type_filter      if enabled+non-empty, restricts to `type IN (...)`.
  //   round_id_filter  if enabled, restricts to `round_id BETWEEN low AND high`.
  // These are pure optimizations: the upper SQL operator re-applies the full
  // predicate, so an over-broad filter only affects I/O, never correctness.
  int init(
      const uint64_t tenant_id,
      const char *table_name,
      const ObSharedGCTaskTypeFilter &type_filter,
      const ObSharedGCTaskRoundIdFilter &round_id_filter,
      common::ObISQLClient &client);

  // Returns OB_SUCCESS + task (deep-copied into allocator) plus the row's
  // gmt_create / gmt_modified, OB_ITER_END when exhausted, or error.
  int get_next(
      common::ObIAllocator &allocator,
      ObSharedGCTask &task,
      int64_t &gmt_create,
      int64_t &gmt_modified);

  void reset();

  INHERIT_TO_STRING_KV("base", ObSharedGCTaskTableIteratorBase, KCSTRING_(table_name),
                       K_(type_filter), K_(round_id_filter));

protected:
  virtual int build_select_sql_(common::ObSqlString &sql) override;

private:
  const char *table_name_;
  ObSharedGCTaskTypeFilter type_filter_;
  ObSharedGCTaskRoundIdFilter round_id_filter_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCTaskTableFullIterator);
};

// Streaming row-at-a-time iterator over the LS_META_GC rows of
// __all_shared_gc_task for one tenant. Unlike the range iterator it needs no
// round_id / task_id slice: LS_META_GC tasks always use round_id 0 and are
// keyed only by task_id, so this simply scans every LS_META_GC row of the
// tenant in task_id order and decodes each row's value into an
// ObSharedGCLSMetaInfo (ls_id, meta_scn) for the caller.
class ObSharedGCTaskLSMetaIterator final : public ObSharedGCTaskTableIteratorBase
{
public:
  ObSharedGCTaskLSMetaIterator();
  virtual ~ObSharedGCTaskLSMetaIterator();

  int init(
      const uint64_t tenant_id,
      common::ObISQLClient &client);

  // Returns OB_SUCCESS + the decoded ls_meta_info of the current row,
  // OB_ITER_END when exhausted, or error.
  int get_next(ObSharedGCLSMetaInfo &ls_meta_info);

  // Returns OB_SUCCESS + the full task row (deep-copied into allocator),
  // OB_ITER_END when exhausted, or error.
  int get_next(common::ObIAllocator &allocator, ObSharedGCTask &task);

  void reset();

  INHERIT_TO_STRING_KV("base", ObSharedGCTaskTableIteratorBase, "type",
                       static_cast<int64_t>(ObSharedGCTaskType::LS_META_GC));

protected:
  virtual int build_select_sql_(common::ObSqlString &sql) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCTaskLSMetaIterator);
};

// 扫描持久化在 SYS 租户 __all_shared_gc_task 中的 DELETE_TENANT 任务。
// key.tenant_id_ 和 SQL 执行租户均为 OB_SYS_TENANT_ID；task_id_ 和强类型
// value 用于标识待删除租户。
class ObDeleteTenantTaskIterator final : public ObSharedGCTaskTableIteratorBase
{
public:
  ObDeleteTenantTaskIterator();
  virtual ~ObDeleteTenantTaskIterator();

  int init(common::ObISQLClient &client);
  int get_next(common::ObIAllocator &allocator, ObSharedGCTask &task);
  int get_next(
      common::ObIAllocator &allocator,
      ObSharedGCTask &task,
      ObDeleteTenantTaskValue &value);

  void reset();

  INHERIT_TO_STRING_KV("base", ObSharedGCTaskTableIteratorBase, "type",
                       static_cast<int64_t>(ObSharedGCTaskType::DELETE_TENANT));

protected:
  virtual int build_select_sql_(common::ObSqlString &sql) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteTenantTaskIterator);
};

// 扫描持久化在 SYS 租户 __all_shared_gc_task 中的 DELETE_SERVER 任务。
// task_id_ 和强类型 value 用于标识待删除 Server。
class ObDeleteServerTaskIterator final : public ObSharedGCTaskTableIteratorBase
{
public:
  ObDeleteServerTaskIterator();
  virtual ~ObDeleteServerTaskIterator();

  int init(common::ObISQLClient &client);
  int get_next(common::ObIAllocator &allocator, ObSharedGCTask &task);
  int get_next(
      common::ObIAllocator &allocator,
      ObSharedGCTask &task,
      ObDeleteServerTaskValue &value);

  void reset();

  INHERIT_TO_STRING_KV("base", ObSharedGCTaskTableIteratorBase, "type",
                       static_cast<int64_t>(ObSharedGCTaskType::DELETE_SERVER));

protected:
  virtual int build_select_sql_(common::ObSqlString &sql) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteServerTaskIterator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SHARED_GC_TASK_TABLE_ITERATOR_H_
