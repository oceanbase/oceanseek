/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SS_GC_EXEC_DAG_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SS_GC_EXEC_DAG_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"

namespace oceanbase
{
namespace storage
{

struct ObSSGCExecutorCostStat final
{
public:
  ObSSGCExecutorCostStat();
  ~ObSSGCExecutorCostStat() = default;
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // Returns finish - start, or 0 if either endpoint is missing.
  int64_t total_cost_us() const;
  int64_t plan_cost_us() const;
  int64_t execute_cost_us() const;
public:
  int64_t start_us_;
  int64_t finish_us_;
  int64_t plan_start_us_;
  int64_t plan_finish_us_;
  int64_t execute_start_us_;
  int64_t execute_finish_us_;
  int64_t succ_row_count_;
  int64_t failed_row_count_;
  // ID counts in successfully completed task rows, including idempotent deletes.
  int64_t succ_sstable_macro_cnt_;
  int64_t succ_tablet_sub_meta_cnt_;
};

struct ObSSGCExecDagInitParam : public share::ObIDagInitParam
{
  ObSSGCExecDagInitParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      gc_type_(ObSharedGCTaskType::INVALID_TYPE),
      allow_gc_round_id_(-1)
  {}
  virtual ~ObSSGCExecDagInitParam() {}
  bool is_valid() const override;

  TO_STRING_KV(K_(tenant_id), K_(gc_type), K_(allow_gc_round_id));

  uint64_t tenant_id_;
  ObSharedGCTaskType gc_type_;
  // MarkSweep: round_id upper bound for rows eligible to GC; read from
  //            __all_shared_gc_control.allow_gc_round_id by the timer.
  // FailGC:    iterator ignores this field; convention: pass 0.
  int64_t allow_gc_round_id_;
};

struct ObSharedGCTask;

class ObSSGCExecDag : public ObSSGCDagBase
{
public:
  ObSSGCExecDag();
  virtual ~ObSSGCExecDag();
  void destroy();

  bool is_valid() const;
  virtual bool operator==(const ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int report_result() override;

  uint64_t get_tenant_id() const { return tenant_id_; }
  ObSharedGCTaskType get_gc_type() const { return gc_type_; }
  int64_t get_allow_gc_round_id() const { return allow_gc_round_id_; }
  void record_start_us();
  void record_plan_us(const int64_t plan_start_us, const int64_t plan_finish_us);
  void record_execute_us(const int64_t execute_start_us, const int64_t execute_finish_us);
  void add_row_stat(const int64_t succ_row_count,
                    const int64_t failed_row_count,
                    const int64_t succ_sstable_macro_cnt,
                    const int64_t succ_tablet_sub_meta_cnt);

private:

  uint64_t tenant_id_;
  ObSharedGCTaskType gc_type_;
  int64_t allow_gc_round_id_;
  ObSSGCExecutorCostStat cost_stat_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSGCExecDag);
};

class ObExecuteBaseTask : public ObSharedGCBaseTask
{
public:
  static constexpr int64_t DEFAULT_SLICE_SIZE = 100;

  explicit ObExecuteBaseTask(const share::ObITask::ObITaskType task_type);
  virtual ~ObExecuteBaseTask() = default;
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;

protected:
  int init_base_(const uint64_t tenant_id,
                 const ObSSGCTaskIdRange &round_range,
                 const ObSSGCTaskIdRange &slice_range);
  int update_server_id_and_trace_id_(ObSharedGCTask &row,
                                     const int64_t server_id,
                                     const common::ObString &trace_id);

  virtual const char *get_page_allocator_label_() const = 0;
  virtual const char *get_succ_rows_label_() const = 0;
  virtual ObSharedGCTaskType get_gc_type_() const = 0;
  virtual int64_t get_iter_round_id_() const = 0;
  virtual int64_t get_next_slice_size_() const { return DEFAULT_SLICE_SIZE; }
  virtual int alloc_next_task_(ObSSGCExecDag &dag,
                               const ObSSGCTaskIdRange &slice_range,
                               share::ObITask *&next_task) = 0;
  virtual int process_row_(ObSharedGCTask &row,
                           const int64_t server_id,
                           const common::ObString &trace_id,
                           int64_t &sstable_macro_cnt,
                           int64_t &tablet_sub_meta_cnt) = 0;

  bool is_inited_;
  uint64_t tenant_id_;
  ObSSGCTaskIdRange round_range_;
  ObSSGCTaskIdRange slice_range_;

private:
  struct ProcessSliceFn {
    explicit ProcessSliceFn(ObExecuteBaseTask &task);
    int operator()();

    ObExecuteBaseTask &task_;
  };

  struct OnRetryFn {
    explicit OnRetryFn(ObExecuteBaseTask &task);
    int operator()();

    ObExecuteBaseTask &task_;
  };

  int process_slice_();

  DISALLOW_COPY_AND_ASSIGN(ObExecuteBaseTask);
};

class ObSSGCPlanBaseTask : public ObSharedGCBaseTask
{
public:
  explicit ObSSGCPlanBaseTask(const share::ObITask::ObITaskType task_type);
  virtual ~ObSSGCPlanBaseTask() = default;

protected:
  int init_base_(const uint64_t tenant_id, const bool is_valid_arg);
  void record_plan_us_(const int64_t start_us);

  bool is_inited_;
  uint64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSGCPlanBaseTask);
};

// ====================================================================================
// MarkSweep plan task for ObSSGCExecDag.
//
//   Step 1: SELECT MIN(round_id) and skip if it exceeds allow_gc_round_id.
//   Step 2: SELECT MIN(task_id), MAX(task_id) for min_round_id and compute the first slice.
//   Step 3: create_task for the first execute task; subsequent slices chain via generate_next_task.
// ====================================================================================
class ObMarkSweepPlanTask : public ObSSGCPlanBaseTask
{
public:
  ObMarkSweepPlanTask();
  virtual ~ObMarkSweepPlanTask() = default;
  int init(const uint64_t tenant_id, const int64_t allow_gc_round_id);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", ObITask,
                       K_(is_inited), K_(tenant_id), K_(allow_gc_round_id));

private:
  int slice_task_id_range_(const int64_t round_id,
                           bool &need_execute_task,
                           ObSSGCTaskIdRange &round_range,
                           ObSSGCTaskIdRange &slice_range);

  int64_t allow_gc_round_id_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepPlanTask);
};

// ====================================================================================
// Execute task for MarkSweep GC. Each instance handles a single
// (round_id, slice_range) slice.
//   1. Streams rows from __all_shared_gc_task within the slice.
//   2. Claims each row with (server_id, dag trace_id).
//   3. Deserialises value -> MacroBlockId list.
//   4. Partitions macros into single-delete and double-delete (sys-tenant
//      tablet meta -- currently deferred; see process_row_ for details).
//   5. Calls batch_del_files for single-delete macros and double_delete for
//      double-delete macros.
//   6. Transactionally DELETE the task row and INSERT into history.
//
// Execute tasks chain serially via generate_next_task: only one execute task exists in
// the dag queue at a time. Each execute task covers one DEFAULT_SLICE_SIZE slice within a
// single round; generate_next_task advances to the next slice and returns
// OB_ITER_END once the round's task_id range is exhausted. The chain does
// NOT advance to another round — one DAG only drains the smallest round_id
// under the watermark; the timer's next tick discovers the next round.
//
// Range invariant:
//   round_range.start_task_id <= slice_range_.start_task_id
//       <= slice_range_.end_task_id <= round_range.end_task_id
// ====================================================================================
class ObMarkSweepExecuteTask : public ObExecuteBaseTask
{
public:
  ObMarkSweepExecuteTask();
  virtual ~ObMarkSweepExecuteTask() = default;
  int init(const uint64_t tenant_id,
           const int64_t round_id,
           const ObSSGCTaskIdRange &round_range,
           const ObSSGCTaskIdRange &slice_range,
           const int64_t allow_gc_round_id);
  INHERIT_TO_STRING_KV("ObITask", ObITask,
                       K_(is_inited), K_(tenant_id),
                       K_(round_id), K_(round_range), K_(slice_range),
                       K_(allow_gc_round_id));

private:
  virtual ObSharedGCTaskType get_gc_type_() const override
  { return ObSharedGCTaskType::MARK_SWEEP_GC; }
  virtual int64_t get_iter_round_id_() const override { return round_id_; }
  virtual int alloc_next_task_(ObSSGCExecDag &dag,
                               const ObSSGCTaskIdRange &slice_range,
                               share::ObITask *&next_task) override;
  virtual const char *get_page_allocator_label_() const override { return "SSGCPage"; }
  virtual const char *get_succ_rows_label_() const override { return "MSSuccRows"; }
  virtual int process_row_(ObSharedGCTask &row,
                           const int64_t server_id,
                           const common::ObString &trace_id,
                           int64_t &sstable_macro_cnt,
                           int64_t &tablet_sub_meta_cnt) override;

  int64_t round_id_;
  int64_t allow_gc_round_id_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepExecuteTask);
};

class ObFailGCExecuteTask;

class ObFailGCPlanTask : public ObSSGCPlanBaseTask
{
public:
  ObFailGCPlanTask();
  virtual ~ObFailGCPlanTask() = default;
  int init(const uint64_t tenant_id);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", ObITask, K_(is_inited), K_(tenant_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObFailGCPlanTask);
};

class ObFailGCExecuteTask : public ObExecuteBaseTask
{
public:
  ObFailGCExecuteTask();
  virtual ~ObFailGCExecuteTask() = default;
  int init(const uint64_t tenant_id,
           const ObSSGCTaskIdRange &round_range,
           const ObSSGCTaskIdRange &slice_range);
  INHERIT_TO_STRING_KV("ObITask", ObITask,
                       K_(is_inited), K_(tenant_id),
                       K_(round_range), K_(slice_range));

private:
  virtual ObSharedGCTaskType get_gc_type_() const override
  { return ObSharedGCTaskType::FAIL_GC; }
  virtual int64_t get_iter_round_id_() const override { return 0; }
  virtual int alloc_next_task_(ObSSGCExecDag &dag,
                               const ObSSGCTaskIdRange &slice_range,
                               share::ObITask *&next_task) override;
  virtual const char *get_page_allocator_label_() const override { return "FailGCPage"; }
  virtual const char *get_succ_rows_label_() const override { return "FailGCSuccRow"; }
  virtual int process_row_(ObSharedGCTask &row,
                           const int64_t server_id,
                           const common::ObString &trace_id,
                           int64_t &sstable_macro_cnt,
                           int64_t &tablet_sub_meta_cnt) override;
  int process_tablet_meta_row_(const ObSharedGCFailInfo &info,
                               const ObSharedGCTask &row,
                               int64_t &sstable_macro_cnt,
                               int64_t &tablet_sub_meta_cnt);
  // 将单条 FAIL_GC 记录中识别并删除的宏块 ID 记录为 FAIL_GC_MACRO_ID 历史记录。
  // 宏块列表会被拆分为多行，以保证序列化后的每行都不超过 ObSharedGCTask::MAX_VALUE_LENGTH。
  int record_fail_gc_macro_ids_(
      const ObSharedGCTask &src_row,
      const common::ObIArray<blocksstable::MacroBlockId> &macro_id_array);

  DISALLOW_COPY_AND_ASSIGN(ObFailGCExecuteTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_DAG_OB_SS_GC_EXEC_DAG_H_
