/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_TASK_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_TASK_H_

#include "common/row/ob_row.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"

namespace oceanbase
{
namespace observer
{

// 虚拟表 __all_virtual_shared_gc_task (12606) 和
// __all_virtual_shared_gc_task_history (12607)。
//
// 读取所有可见租户的 __all_shared_gc_task / __all_shared_gc_task_history 数据，
// 将 OB_UNIS 序列化的 value 列解码为便于排查问题的可读文本。根据行的 type
// 选择用于解码 value 的结构：
//   MARK_SWEEP_GC(1)     -> ObSharedGCMacroIdList
//   FAIL_GC(2)           -> ObSharedGCFailInfo
//   FAIL_GC_MACRO_ID(3)  -> ObSharedGCMacroIdList
//   LS_META_GC(4)        -> ObSharedGCLSMetaInfo
//   DELETE_TENANT(5)     -> ObDeleteTenantTaskValue
//   DELETE_SERVER(6)     -> ObDeleteServerTaskValue
//   LS_CKPT_GC(7)        -> ObSharedGCLSCheckpointInfo
//   MARK_SWEEP_GC_IN_TABLE_STAT(8)       -> 十进制宏块数量字符串
//   MARK_SWEEP_GC_OBJECT_STORAGE_STAT(9) -> 十进制宏块数量字符串
//   MARK_SWEEP_GC_DIAG(10) -> one round marker with an empty value
//
// 数据源为 ObSharedGCTaskTableFullIterator。该 Iterator 扫描当前租户的一张物理表，
// 并将内部 SQL 路由到实际存储数据行的租户（user->meta、meta->sys、sys->自身）。
// 12606 和 12607 共用此类，Iterator 工厂根据表 ID 调用 set_table_name()
// 选择物理表。
//
// 租户迭代由本类自行管理，不切换租户上下文：SYS 租户通过 GCTX.schema_service_
// 枚举集群中的所有租户，并将明确的租户 ID 传给 Iterator 读取各租户数据；内部 SQL
// 由连接池按租户 ID 路由。用户租户仅迭代自身。整个扫描过程运行在查询租户的
// MTL 上下文中。
class ObAllVirtualSharedGCTask : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSharedGCTask();
  virtual ~ObAllVirtualSharedGCTask();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  // Selects the physical table to scan. Must be OB_ALL_SHARED_GC_TASK_TNAME or
  // OB_ALL_SHARED_GC_TASK_HISTORY_TNAME. Set by the iterator factory.
  void set_table_name(const char *table_name) { table_name_ = table_name; }
private:
  enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    TYPE,
    ROUND_ID,
    TASK_ID,
    GMT_CREATE,
    GMT_MODIFIED,
    VALUE,
    EXTRA_INFO,
    SERVER_ID,
    TRACE_ID,
  };

  // A closed [tenant_id_low_, tenant_id_high_] range extracted from one key range.
  struct ObSharedGCTaskTenantIdRange final
  {
  public:
    ObSharedGCTaskTenantIdRange() { reset(); }
    ~ObSharedGCTaskTenantIdRange() { reset(); }
    ObSharedGCTaskTenantIdRange(const uint64_t tenant_id_low, const uint64_t tenant_id_high)
      : tenant_id_low_(tenant_id_low), tenant_id_high_(tenant_id_high) {}
    void reset() { tenant_id_low_ = 0; tenant_id_high_ = UINT64_MAX; }
    TO_STRING_KV(K_(tenant_id_low), K_(tenant_id_high));

  public:
    uint64_t tenant_id_low_;
    uint64_t tenant_id_high_;
  };

  // tenant_id-dimension filter: ranges_ is only consulted when
  // is_filter_enabled_ is true. Unlike the single-interval round filter,
  // this keeps the full list of ranges so an IN-list (e.g. tenant_id IN (1002,1004))
  // prunes each excluded tenant precisely instead of collapsing into one enclosing interval
  // (which would issue wasted per-tenant SQLs for the tenants in between).
  struct ObSharedGCTaskTenantIdFilter final
  {
    ObSharedGCTaskTenantIdFilter() { reset(); }
    ~ObSharedGCTaskTenantIdFilter() { reset(); }
    void reset() { is_filter_enabled_ = false; ranges_.reset(); }
    int assign(const ObSharedGCTaskTenantIdFilter &other);
    bool need_filter() const { return is_filter_enabled_ && !ranges_.empty(); }
    TO_STRING_KV(K_(is_filter_enabled), K_(ranges));

  public:
    bool is_filter_enabled_;
    common::ObSEArray<ObSharedGCTaskTenantIdRange, 2> ranges_;
  };

  // 按 TYPE 列的排序规则将过滤字符串转换为任务类型。
  static storage::ObSharedGCTaskType parse_type_filter_(const common::ObString &type_name);
  // Parse key_ranges_ (the query's rowkey predicates on tenant_id, type, round_id, task_id)
  // into the pushdown members below. Called lazily on first row.
  int extract_key_ranges_();
  // 当复合 rowkey 无法表达非前导 TYPE 条件时，从下推过滤表达式补充提取 TYPE。
  int extract_type_filter_from_op_filters_();
  // True if tenant_id should be filtered out (skipped): the filter is enabled and
  // tenant_id falls inside none of its ranges. Returns false (keep) when the
  // filter is disabled / empty, or when tenant_id is inside some range.
  bool is_tenant_id_filtered_(const uint64_t tenant_id) const;
  // Build the list of tenants to iterate: all cluster tenants for SYS, or just
  // the querying tenant itself for a user/meta tenant. Called lazily on first row.
  int build_tenant_ids_();
  // Read the next virtual row for the tenant at tenant_idx_, opening its
  // iterator lazily. Returns OB_ITER_END when that tenant is exhausted.
  int process_curr_tenant_(common::ObNewRow *&row);
  // Reset the per-tenant iterator state before advancing to the next tenant.
  void release_last_tenant_();
  // 读取下一条物理任务记录；MARK_SWEEP_GC 类型反序列化宏块列表后逐宏块输出，
  // 统计类型保留十进制数量字符串并按单行输出。
  int fetch_next_task_();
  // convert a single macro id into a readable string.
  int convert_macro_id_(const blocksstable::MacroBlockId &macro_id,
                        common::ObString &readable_value);
  // convert the OB_UNIS-serialized FAIL_GC value into a readable string.
  int convert_fail_info_(const common::ObString &raw_value,
                         common::ObString &readable_value);
  // 将 LS_META_GC / LS_CKPT_GC 的 OB_UNIS 值转成可读字符串。
  int convert_ls_meta_info_(const common::ObString &raw_value,
                            common::ObString &readable_value);
  // convert the OB_UNIS-serialized DELETE_TENANT value into a readable string.
  int convert_delete_tenant_value_(const common::ObString &raw_value,
                                   common::ObString &readable_value);
  // convert the OB_UNIS-serialized DELETE_SERVER value into a readable string.
  int convert_delete_server_value_(const common::ObString &raw_value,
                                   common::ObString &readable_value);
  int fill_cells_(const storage::ObSharedGCTask &task,
                  const int64_t gmt_create,
                  const int64_t gmt_modified,
                  const common::ObString &value_str);
private:
  static constexpr int64_t MAX_VALUE_LEN = 256L * 1024L; // 256KB
  const char *table_name_;
  // Tenants to iterate. For SYS this is every cluster tenant; for a user/meta tenant
  // it is just itself. tenant_idx_ == -1 means the list has not been built yet.
  common::ObArray<uint64_t> tenant_ids_;
  int64_t tenant_idx_;
  ObSharedGCTaskTenantIdFilter tenant_id_filter_;
  storage::ObSharedGCTaskTypeFilter type_filter_;
  bool is_type_filter_always_false_;
  storage::ObSharedGCTaskRoundIdFilter round_id_filter_;
  storage::ObSharedGCTaskTableFullIterator task_iter_;
  bool task_iter_opened_;
  // 当前正在输出的物理任务。MARK_SWEEP_GC 按宏块拆行，统计类型和其他类型输出单行。
  storage::ObSharedGCTask cur_task_;
  storage::ObSharedGCMacroIdList cur_macro_id_list_;
  int64_t cur_macro_idx_;
  int64_t cur_gmt_create_;
  int64_t cur_gmt_modified_;
  bool has_pending_task_; // cur_task_ still has rows to emit
  // holds the current physical task across the macro id rows it produces;
  // reset only when fetching the next physical row.
  common::ObArenaAllocator task_allocator_;
  // owns value_buf_, which is allocated once and reused across all rows/tenants.
  common::ObArenaAllocator row_allocator_;
  // readable value buffer of MAX_VALUE_LEN, lazily allocated from row_allocator_
  // once and reused for every emitted row; the value is copied into cur_row_
  // before the next row is fetched, so reuse is safe.
  char *value_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSharedGCTask);
};

} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_TASK_H_ */
