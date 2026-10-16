/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_FAIL_GC_DAG_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_FAIL_GC_DAG_H_

#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_page_iterator.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace storage
{

class ObSSMetaCtrlTableOperator;
class ObAtomicProtocolCtrl;

struct ObSSFailGCCtrlRow final
{
public:
  ObSSFailGCCtrlRow();
  ~ObSSFailGCCtrlRow() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObSSFailGCCtrlRow &row);
  TO_STRING_KV(K_(ctrl_key), K_(ctrl_value));

  ObSSMetaCtrlKey ctrl_key_;
  ObSSMetaCtrlValue ctrl_value_;
};

struct ObSSFailGCInfo final
{
public:
  ObSSFailGCInfo();
  ~ObSSFailGCInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObSSFailGCInfo &info);
  TO_STRING_KV(K_(key_info), K_(ctrl_value), K_(need_generate_task));

  ObSSMetaCtrlKeyInfo key_info_;
  ObSSMetaCtrlValue ctrl_value_;
  bool need_generate_task_;
};

struct ObSSFailGCLSCtx final
{
public:
  ObSSFailGCLSCtx();
  ~ObSSFailGCLSCtx();
  void reset();
  bool is_valid() const;
  bool record_first_error_code(const int error_code);
  int get_first_error_code() const;
  // Runtime scan state is Schedule-chain-owned. Do not expose it here because
  // sibling Executor tasks may serialize this context concurrently.
  TO_STRING_KV(K_(ls_id), K_(checkpoint_meta_scn),
      "first_error_code", ATOMIC_LOAD(&first_error_code_));

  share::ObLSID ls_id_;
  // Invalid means no checkpoint has been published and checkpoint rows are
  // not reclaimed.
  share::SCN checkpoint_meta_scn_;
  ObSSMetaCtrlPageIterator page_iter_;
  bool iterator_inited_;
  bool scan_finished_;
  bool scan_failed_;
  int first_error_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCLSCtx);
};

struct ObSSFailGCDagCtx final
{
public:
  ObSSFailGCDagCtx();
  ~ObSSFailGCDagCtx();
  int init(const uint64_t tenant_id);
  void reset();
  bool is_valid() const;
  int alloc_ls_ctx(const share::ObLSID &ls_id, ObSSFailGCLSCtx *&ls_ctx);
  bool record_error_code(ObSSFailGCLSCtx &ls_ctx, const int error_code);
  void add_scanned_ctrl_row_count(const int64_t count);
  void add_abandoned_ctrl_row_count(const int64_t count);
  void add_generated_task_count(const int64_t count);
  int64_t get_ls_count() const;
  int get_first_error_code() const;
  int64_t get_failed_ls_count() const;
  int64_t get_scanned_ctrl_row_count() const;
  int64_t get_abandoned_ctrl_row_count() const;
  int64_t get_generated_task_count() const;
  TO_STRING_KV(K_(tenant_id), K_(sql_timeout_us), K_(gc_scn), K_(need_gc),
      "ls_count", ATOMIC_LOAD(&ls_count_),
      "first_error_code", ATOMIC_LOAD(&first_error_code_),
      "failed_ls_count", ATOMIC_LOAD(&failed_ls_count_),
      "scanned_ctrl_row_count", ATOMIC_LOAD(&scanned_ctrl_row_count_),
      "abandoned_ctrl_row_count", ATOMIC_LOAD(&abandoned_ctrl_row_count_),
      "generated_task_count", ATOMIC_LOAD(&generated_task_count_));

  uint64_t tenant_id_;
  int64_t sql_timeout_us_;
  share::SCN gc_scn_;
  bool need_gc_;
  common::ObArray<ObSSFailGCLSCtx *> ls_ctxs_;
  int64_t ls_count_;
  int first_error_code_;
  int64_t failed_ls_count_;
  int64_t scanned_ctrl_row_count_;
  // Rows classified as abandoned and queued into an Executor transaction.
  // This is an attempt counter, so a later transaction rollback does not
  // subtract it. generated_task_count_ below is commit-success based.
  int64_t abandoned_ctrl_row_count_;
  int64_t generated_task_count_;

private:
  friend class ObSSFailGCExecutorTask;
  bool owns_ls_ctx_(const ObSSFailGCLSCtx &ls_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCDagCtx);
};

class ObSSFailGCDagUtil final
{
public:
  static int get_fail_gc_scn(
      const uint64_t tenant_id,
      bool &need_gc,
      share::SCN &gc_scn);
  static int get_palf_kv_checkpoint_meta_scn(
      share::SCN &checkpoint_meta_scn);

private:
  ObSSFailGCDagUtil() = delete;
};

struct ObSSFailGCDagParam final : public share::ObIDagInitParam
{
public:
  ObSSFailGCDagParam();
  virtual ~ObSSFailGCDagParam() = default;
  int init(const uint64_t tenant_id, const int64_t schedule_ts = 0);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(schedule_ts));

  uint64_t tenant_id_;
  int64_t schedule_ts_;
};

class ObSSFailGCDag final : public ObSSGCDagBase
{
public:
  static constexpr share::ObDagType::ObDagTypeEnum DAG_TYPE =
      share::ObDagType::DAG_TYPE_SS_FAIL_GC;

  ObSSFailGCDag();
  virtual ~ObSSFailGCDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int report_result() override;
  void report_start_event();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  virtual uint64_t get_consumer_group_id() const override
  {
    return consumer_group_id_;
  }

  ObSSFailGCDagCtx *get_ctx() { return ctx_; }
  const ObSSFailGCDagCtx *get_ctx() const { return ctx_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_schedule_ts() const { return schedule_ts_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  INHERIT_TO_STRING_KV("ObIDag", share::ObIDag, K_(is_inited),
      K_(tenant_id), K_(schedule_ts), KPC_(ctx));

private:
  int alloc_ctx_();
  void free_ctx_();

private:
  bool is_inited_;
  // Kept outside ctx_ so an identity-only probe does not need init_by_param().
  uint64_t tenant_id_;
  int64_t schedule_ts_;
  int64_t start_event_reported_;
  ObSSFailGCDagCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCDag);
};

class ObSSFailGCPrepareTask final : public ObSharedGCBaseTask
{
public:
  ObSSFailGCPrepareTask();
  virtual ~ObSSFailGCPrepareTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSFailGCPrepareTask", KP(this), K_(is_inited));

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCPrepareTask);
};

class ObSSFailGCScheduleTask final : public ObSharedGCBaseTask
{
public:
  ObSSFailGCScheduleTask();
  virtual ~ObSSFailGCScheduleTask() = default;
  int init(const int64_t ls_idx, const bool is_ls_head);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSFailGCScheduleTask", KP(this),
      K_(is_inited), K_(next_task_generated), K_(ls_idx), K_(is_ls_head),
      KP_(ls_ctx));

private:
  int bind_ls_ctx_(ObSSFailGCDagCtx &ctx);
  int init_ls_iterator_(ObSSFailGCDagCtx &ctx);
  int fetch_ctrl_rows_(
      ObSSFailGCDagCtx &ctx,
      common::ObIArray<ObSSFailGCCtrlRow> &ctrl_rows,
      bool &need_continuation);
  int generate_tasks_(
      ObSSFailGCDag &dag,
      const common::ObIArray<ObSSFailGCCtrlRow> &ctrl_rows,
      const bool need_continuation);
  void fill_page_scan_param_(ObSSMetaCtrlPageScanParam &scan_param) const;

private:
  bool is_inited_;
  bool next_task_generated_;
  int64_t ls_idx_;
  bool is_ls_head_;
  ObSSFailGCLSCtx *ls_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCScheduleTask);
};

class ObSSFailGCExecutorTask : public ObSharedGCBaseTask
{
public:
  ObSSFailGCExecutorTask();
  virtual ~ObSSFailGCExecutorTask();
  int init(
      ObSSFailGCLSCtx *ls_ctx,
      const common::ObIArray<ObSSFailGCCtrlRow> &ctrl_rows);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSFailGCExecutorTask", KP(this),
      K_(is_inited), KP_(ls_ctx), "ctrl_row_count", ctrl_rows_.count());

private:
  int process_ctrl_row_(
      ObSSFailGCDagCtx &ctx,
      const ObSSFailGCCtrlRow &ctrl_row,
      common::ObIArray<ObSSFailGCInfo> &abandoned_key_infos);
  int do_ctrl_row_failed_gc_(
      ObSSFailGCDagCtx &ctx,
      const ObSSFailGCCtrlRow &ctrl_row,
      common::ObIArray<ObSSFailGCInfo> &abandoned_key_infos);
  int check_can_do_ctrl_row_failed_gc_(
      const ObSSMetaCtrlKey &ctrl_key,
      const ObSSMetaCtrlValue &ctrl_value,
      const share::SCN &meta_scn,
      const share::SCN &gc_scn,
      const share::SCN &checkpoint_meta_scn,
      bool &can_do_failed_gc) const;
  int check_need_collect_abandoned_row_(
      const ObSSFailGCDagCtx &ctx,
      const ObSSMetaCtrlKeyInfo &key_info,
      const ObSSMetaCtrlValue &ctrl_value,
      bool &need_collect);
  int check_need_generate_task_(
      const ObSSMetaCtrlKeyInfo &key_info,
      const ObSSMetaCtrlValue &ctrl_value,
      bool &need_generate_task);
  int add_fail_gc_tablet_to_map_(
      const ObSSFailGCDagCtx &ctx,
      const ObSSMetaCtrlKeyInfo &key_info,
      const ObSSMetaCtrlValue &ctrl_value,
      ObSSFailGCTabletMapValue &fail_gc_tablet_value);
  int get_tablet_lock_row_value_(
      const ObSSFailGCDagCtx &ctx,
      const ObSSMetaCtrlKeyInfo &key_info,
      const ObSSMetaCtrlValue &ctrl_value,
      ObSSMetaCtrlLockRowValue &lock_value);
  int check_can_set_abandon_(
      const ObSSFailGCDagCtx &ctx,
      const ObSSMetaCtrlValue &ctrl_value,
      bool &can_abandon) const;
  int generate_fail_gc_task_(
      ObSSFailGCDagCtx &ctx,
      const common::ObIArray<ObSSFailGCInfo> &abandoned_key_infos);

protected:
  virtual int cancel_atomic_protocol_(
      ObAtomicProtocolCtrl &protocol_ctrl);
  virtual int delete_abandoned_key_info_(
      const ObSSFailGCInfo &fail_gc_info,
      common::ObIAllocator &allocator,
      ObSSMetaCtrlTableOperator &table_operator);

private:
  int insert_fail_gc_task_(
      const ObSSFailGCInfo &fail_gc_info,
      common::ObMySQLTransaction &trans);

private:
  bool is_inited_;
  ObSSFailGCLSCtx *ls_ctx_;
  common::ObArray<ObSSFailGCCtrlRow> ctrl_rows_;
  ObSSFailGCTabletMap fail_gc_tablet_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSSFailGCExecutorTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_FAIL_GC_DAG_H_
