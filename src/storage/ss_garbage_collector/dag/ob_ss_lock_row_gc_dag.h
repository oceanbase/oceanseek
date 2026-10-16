/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LOCK_ROW_GC_DAG_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LOCK_ROW_GC_DAG_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_page_iterator.h"
#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
namespace storage
{

struct ObSSLockRowGCLSCtx final
{
public:
  ObSSLockRowGCLSCtx();
  ~ObSSLockRowGCLSCtx();
  void reset();
  bool is_valid() const;
  bool record_first_error_code(const int error_code);
  int get_first_error_code() const;
  void mark_tablet_lock_rows_not_all_deleted();
  bool are_all_tablet_lock_rows_deleted() const;
  // Runtime scan state and the LS lock row value are Schedule-chain-owned.
  // Do not expose them here because sibling Executor tasks may run concurrently.
  TO_STRING_KV(K_(ls_id),
      "first_error_code", ATOMIC_LOAD(&first_error_code_),
      "all_tablet_lock_rows_deleted",
      ATOMIC_LOAD(&all_tablet_lock_rows_deleted_));

  share::ObLSID ls_id_;
  ObSSMetaCtrlKey ls_lock_row_key_;
  ObSSMetaCtrlLockRowValue ls_lock_row_value_;
  ObSSMetaCtrlPageIterator page_iter_;
  bool iterator_inited_;
  bool scan_finished_;
  bool scan_failed_;
  // Published as true and only changed from true to false during execution.
  bool all_tablet_lock_rows_deleted_;
  int first_error_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCLSCtx);
};

struct ObSSLockRowGCDagCtx final
{
  friend class ObSSLockRowGCExecutorTask;

public:
  ObSSLockRowGCDagCtx();
  ~ObSSLockRowGCDagCtx();
  int init(const uint64_t tenant_id);
  void reset();
  bool is_valid() const;
  int alloc_ls_ctx(
      const share::ObLSID &ls_id,
      ObSSLockRowGCLSCtx *&ls_ctx);
  bool record_error_code(
      ObSSLockRowGCLSCtx &ls_ctx,
      const int error_code);
  void add_scanned_tablet_count(const int64_t count);
  void add_deleted_tablet_lock_row_count(const int64_t count);
  void add_deleted_ls_lock_row_count(const int64_t count);
  int64_t get_ls_count() const;
  int get_first_error_code() const;
  int64_t get_failed_ls_count() const;
  int64_t get_scanned_tablet_count() const;
  int64_t get_deleted_tablet_lock_row_count() const;
  int64_t get_deleted_ls_lock_row_count() const;
  TO_STRING_KV(K_(tenant_id),
      "ls_count", ATOMIC_LOAD(&ls_count_),
      "first_error_code", ATOMIC_LOAD(&first_error_code_),
      "failed_ls_count", ATOMIC_LOAD(&failed_ls_count_),
      "scanned_tablet_count", ATOMIC_LOAD(&scanned_tablet_count_),
      "deleted_tablet_lock_row_count",
      ATOMIC_LOAD(&deleted_tablet_lock_row_count_),
      "deleted_ls_lock_row_count",
      ATOMIC_LOAD(&deleted_ls_lock_row_count_));

  uint64_t tenant_id_;
  int64_t sql_timeout_us_;
  common::ObArray<ObSSLockRowGCLSCtx *> ls_ctxs_;
  int64_t ls_count_;
  int first_error_code_;
  int64_t failed_ls_count_;
  int64_t scanned_tablet_count_;
  int64_t deleted_tablet_lock_row_count_;
  int64_t deleted_ls_lock_row_count_;

private:
  bool owns_ls_ctx_(const ObSSLockRowGCLSCtx &ls_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCDagCtx);
};

struct ObSSLockRowGCDagParam final : public share::ObIDagInitParam
{
public:
  ObSSLockRowGCDagParam();
  virtual ~ObSSLockRowGCDagParam() = default;
  int init(const uint64_t tenant_id, const int64_t schedule_ts = 0);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(schedule_ts));

  uint64_t tenant_id_;
  int64_t schedule_ts_;
};

class ObSSLockRowGCDag final : public ObSSGCDagBase
{
public:
  static constexpr share::ObDagType::ObDagTypeEnum DAG_TYPE =
      share::ObDagType::DAG_TYPE_SS_LOCK_ROW_GC;

  ObSSLockRowGCDag();
  virtual ~ObSSLockRowGCDag();
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

  ObSSLockRowGCDagCtx *get_ctx() { return ctx_; }
  const ObSSLockRowGCDagCtx *get_ctx() const { return ctx_; }
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
  ObSSLockRowGCDagCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCDag);
};

class ObSSLockRowGCPrepareTask final : public ObSharedGCBaseTask
{
public:
  ObSSLockRowGCPrepareTask();
  virtual ~ObSSLockRowGCPrepareTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLockRowGCPrepareTask", KP(this),
      K_(is_inited));

private:
  int alloc_ls_ctxs_(
      ObSSLockRowGCDagCtx &ctx,
      const common::ObIArray<share::ObLSID> &ls_ids);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCPrepareTask);
};

class ObSSLockRowGCScheduleTask final : public ObSharedGCBaseTask
{
public:
  ObSSLockRowGCScheduleTask();
  virtual ~ObSSLockRowGCScheduleTask() = default;
  int init(
      const int64_t ls_idx,
      const bool is_ls_head,
      const int64_t batch_idx = 0);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLockRowGCScheduleTask", KP(this),
      K_(is_inited), K_(next_task_generated), K_(ls_idx), K_(is_ls_head),
      K_(batch_idx), KP_(ls_ctx));

private:
  int bind_ls_ctx_(ObSSLockRowGCDagCtx &ctx);
  int init_ls_scan_(ObSSLockRowGCDagCtx &ctx, bool &need_scan);
  int get_ls_lock_row_value_(
      const ObSSLockRowGCDagCtx &ctx,
      bool &ls_lock_row_exist);
  int fetch_tablet_infos_(
      ObSSLockRowGCDagCtx &ctx,
      common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      bool &need_continuation);
  int generate_tasks_(
      ObSSLockRowGCDag &dag,
      const common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      const bool need_continuation);
  void fill_page_scan_param_(ObSSMetaCtrlPageScanParam &scan_param) const;

private:
  bool is_inited_;
  bool next_task_generated_;
  int64_t ls_idx_;
  bool is_ls_head_;
  int64_t batch_idx_;
  ObSSLockRowGCLSCtx *ls_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCScheduleTask);
};

class ObSSLockRowGCExecutorTask : public ObSharedGCBaseTask
{
public:
  ObSSLockRowGCExecutorTask();
  virtual ~ObSSLockRowGCExecutorTask();
  int init(
      ObSSLockRowGCLSCtx *ls_ctx,
      const common::ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
      const int64_t batch_idx = 0);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLockRowGCExecutorTask", KP(this),
      K_(is_inited), K_(batch_idx), KP_(ls_ctx),
      "tablet_info_count", tablet_infos_.count());

private:
  int do_tablet_lock_row_gc_work_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSGCTabletBaseInfo &tablet_info,
      bool &delete_lock_row,
      int64_t &deleted_lock_row_count);
  int check_tablet_ctrl_value_exist_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      bool &ctrl_value_exist);

protected:
  virtual int check_can_delete_tablet_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      const ObSSGCTabletBaseInfo &tablet_info,
      bool &can_delete);
  virtual int delete_tablet_lock_rows_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      int64_t &deleted_lock_row_count);

private:
  bool is_inited_;
  int64_t batch_idx_;
  ObSSLockRowGCLSCtx *ls_ctx_;
  common::ObArray<ObSSGCTabletBaseInfo> tablet_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCExecutorTask);
};

class ObSSLockRowGCDeleteLSLockTask : public ObSharedGCBaseTask
{
public:
  ObSSLockRowGCDeleteLSLockTask();
  virtual ~ObSSLockRowGCDeleteLSLockTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSLockRowGCDeleteLSLockTask", KP(this),
      K_(is_inited));

protected:
  virtual int check_ls_meta_exist_(
      const share::ObLSID &ls_id,
      bool &ls_meta_exist);
  virtual int delete_ls_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSLockRowGCLSCtx &ls_ctx,
      int64_t &deleted_lock_row_count);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLockRowGCDeleteLSLockTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_LOCK_ROW_GC_DAG_H_
