/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_MARK_SWEEP_DAG_H_
#define OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_MARK_SWEEP_DAG_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "storage/ss_garbage_collector/dag/ob_shared_gc_base_task.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"

namespace oceanbase
{
namespace storage
{

// ====================================================================================
// Forward decls of the task classes (definitions below Dag)
// ====================================================================================
class ObSSMarkSweepBaseTask;
class ObSSMarkSweepPrepareTask;
class ObSSMarkSweepSortTask;
class ObSSMarkSweepMergeStartTask;
class ObSSMarkSweepMergePassCtrlTask;
class ObSSMarkSweepMergeBatchTask;
class ObSSMarkSweepMergeCompleteTask;
class ObSSMarkSweepDiffTask;
struct ObSharedGCMacroIdList;

struct ObSSMarkSweepDagParam;
struct ObSSMarkSweepCtx;

// ====================================================================================
// ObSSMarkSweepCostStat — cost statistics of one ObSSMarkSweepDag, reported into
// the FINISH_MARK_SWEEP_GC server event (extra_info) at report_result. the sort
// and merge stages run concurrent tasks, so their start/end timestamps are accumulated via
// atomic min(start)/max(end) (common::dec_update / common::inc_update). total_/diff_ and the
// per-pass input count are written by single tasks with no concurrency. Kept-meta persist
// costs are atomically summed and normalized by the parallelism saved by PrepareTask.
// ====================================================================================
struct ObSSMarkSweepCostStat final
{
public:
  ObSSMarkSweepCostStat();
  ~ObSSMarkSweepCostStat() = default;
  void reset();
  // whole-dag span: start recorded by PrepareTask, end by report_result (each called once)
  void set_total_start_us(const int64_t total_start_us) { total_start_us_ = total_start_us; }
  void set_total_end_us(const int64_t total_end_us) { total_end_us_ = total_end_us; }
  // sort stage runs concurrently -> atomic min/max
  void record_sort_cost(const int64_t start_us, const int64_t end_us);
  // PrepareTask freezes the effective parallelism before SortTasks run.
  void set_kept_meta_scn_persist_parallelism(const int64_t parallelism)
  { kept_meta_scn_persist_parallelism_ = MAX(parallelism, 1L); }
  // Concurrent SortTasks persist kept_meta_scn; atomically accumulate each call's cost and counts.
  void record_kept_meta_scn_persist_cost(const int64_t start_us,
                                         const int64_t end_us,
                                         const int64_t plan_count,
                                         const int64_t candidate_count);
  // 事务启动成功即计数，失败重试和回滚事务也包含在内。
  void inc_kept_meta_scn_persist_trans_attempt_count()
  { ATOMIC_INC(&kept_meta_scn_persist_trans_attempt_count_); }
  void record_merge_pass_cost(const ObMarkSweepGroupType group,
                              const int64_t pass_idx,
                              const int64_t start_us,
                              const int64_t end_us);
  // each (group, pass) is visited by a unique pass ctrl task, no concurrency -> plain store
  void record_merge_pass_input_count(const ObMarkSweepGroupType group,
                                     const int64_t pass_idx,
                                     const int64_t input_cnt);
  // merged tmp file size of each group, set once by MergeCompleteTask -> plain store
  void set_merged_file_size(const int64_t merged_del_size, const int64_t merged_kept_size)
  { 
    merged_del_size_ = merged_del_size;
    merged_kept_size_ = merged_kept_size;
  }
  // DiffTask is a single task, no concurrency -> plain store
  void record_diff_cost(const int64_t start_us, const int64_t end_us)
  {
    diff_start_us_ = start_us;
    diff_end_us_ = end_us;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static const int64_t GROUP_CNT = 2;         // DEL=0, KEPT=1
  static const int64_t MAX_MERGE_PASS = 32;   // 64-way fan-in, far beyond that is actually needed
  // whole-dag span. start inited to INT64_MAX, end inited to 0
  int64_t total_start_us_;
  int64_t total_end_us_;
  // sort stage: min(start)/max(end) across all sort tasks. start inited to INT64_MAX, end to 0
  int64_t sort_start_us_;
  int64_t sort_end_us_;
  // 多个 SortTask 的持久化调用耗时之和，通过原子操作累加，不包含调用之间的空档。
  int64_t kept_meta_scn_persist_total_cost_us_;
  // PrepareTask 单次写入的有效并发额度，至少为 1；用于折算耗时，不代表实际并行度。
  int64_t kept_meta_scn_persist_parallelism_;
  // 已完成分类并生成的 tablet 位点计划总数，
  // 尚未按 PrepareTask 冻结的 Lock Row 位点判断是否需要推进。
  int64_t kept_meta_scn_persist_plan_count_;
  // 与 PrepareTask 冻结的 Lock Row 位点比较后仍需推进的候选数，
  // 不等同于实际更新行数。
  int64_t kept_meta_scn_persist_candidate_count_;
  // 成功启动的事务数，包含随后 no-op、失败、回滚以及重试产生的事务。
  // start_trans() 自身失败的调用不计入。
  int64_t kept_meta_scn_persist_trans_attempt_count_;
  // merge stage: per [group][pass] min(start)/max(end). start inited to INT64_MAX, end to 0
  int64_t merge_pass_start_us_[GROUP_CNT][MAX_MERGE_PASS];
  int64_t merge_pass_end_us_[GROUP_CNT][MAX_MERGE_PASS];
  // number of tmp files merged in each pass (= input file count when the pass starts). inited to 0
  int64_t merge_pass_input_cnt_[GROUP_CNT][MAX_MERGE_PASS];
  // diff stage: DiffTask span. start inited to INT64_MAX, end to 0
  int64_t diff_start_us_;
  int64_t diff_end_us_;
  // merged tmp file size of each group after complete_merge, set once by
  // MergeCompleteTask (no concurrency). -1 means "failed to read".
  int64_t merged_del_size_;
  int64_t merged_kept_size_;
};

// ====================================================================================
// Per-group merge status of one ObSSMarkSweepDag, used for live progress reporting in
// fill_info_param.
// ====================================================================================
enum class ObSSMarkSweepMergeStatus : uint8_t
{
  PENDING                   = 0,  // merge not started yet (pass ctrl task has not run)
  INTERMEDIATE_PASS_MERGING = 1,  // merging, at least one more pass follows (batch_count > 1)
  FINAL_PASS_MERGING        = 2,  // merging, this pass outputs 1 file, next pass is terminal (batch_count == 1)
  COMPLETE                  = 3,  // group reached terminal complete state (input_cnt <= 1)
};

// ====================================================================================
// ObSSMarkSweepProgress — live progress counters of one ObSSMarkSweepDag, read by
// fill_info_param while the dag is running.
//   * sort stage: total_* set once in init_by_param then read-only; finished_* bumped by
//     concurrent SortTasks via ATOMIC_INC, read via ATOMIC_LOAD.
//   * merge stage (per group): the scalar fields (status/pass_idx/total_files) are written
//     only by the group's current pass ctrl task. A pass ctrl task runs strictly after the
//     previous pass's batch tasks and before the current pass's batch tasks, so there is no
//     concurrency on these scalars. merge_cur_pass_merged_files_ is bumped by the current
//     pass's batch tasks concurrently via ATOMIC_AAF. All reads use ATOMIC_LOAD.
// ====================================================================================
struct ObSSMarkSweepProgress final
{
public:
  ObSSMarkSweepProgress() { reset(); }
  ~ObSSMarkSweepProgress() = default;
  void reset();

  // ---- sort stage ----
  void set_total_sort_task_count(const int64_t total_sort_task_count)
  { total_sort_task_count_ = total_sort_task_count; }
  void set_total_tablet_count(const int64_t total_tablet_count)
  { total_tablet_count_ = total_tablet_count; }
  int64_t get_total_sort_task_count() const { return total_sort_task_count_; }
  int64_t get_total_tablet_count() const { return total_tablet_count_; }
  void inc_finished_sort_task_count() { ATOMIC_INC(&finished_sort_task_count_); }
  void inc_finished_tablet_count() { ATOMIC_INC(&finished_tablet_count_); }
  int64_t get_finished_sort_task_count() const { return ATOMIC_LOAD(&finished_sort_task_count_); }
  int64_t get_finished_tablet_count() const { return ATOMIC_LOAD(&finished_tablet_count_); }

  // ---- merge stage ----
  // Record the start of a real merging pass (input_cnt > 1); resets merged_files to 0.
  void record_merge_pass(const ObMarkSweepGroupType group,
                         const int64_t pass_idx,
                         const int64_t total_files,
                         const bool has_next_pass);
  // Bump the merged-file count of the current pass (called by each batch task).
  void inc_cur_pass_merged_files(const ObMarkSweepGroupType group, const int64_t delta);
  // Mark a group as reaching its terminal pass (input_cnt <= 1).
  void record_merge_group_done(const ObMarkSweepGroupType group);
  // Format a group's merge progress into buf (< buf_len):
  //   PENDING -> "pending"; COMPLETE -> "complete";
  //   *_PASS_MERGING -> "pass[{idx}],file[{merged}/{total}],has_next_pass[{Y|N}]"
  int64_t merge_progress_to_string(const ObMarkSweepGroupType group, char *buf, const int64_t buf_len) const;

  TO_STRING_KV(K_(total_sort_task_count), K_(total_tablet_count),
      "finished_sort_task_count", ATOMIC_LOAD(&finished_sort_task_count_),
      "finished_tablet_count", ATOMIC_LOAD(&finished_tablet_count_),
      "del_merge_status", static_cast<int64_t>(merge_status_[0]),
      "del_merge_pass_idx", merge_cur_pass_idx_[0],
      "del_merge_files", merge_cur_pass_merged_files_[0],
      "del_merge_total_files", merge_cur_pass_total_files_[0],
      "kept_merge_status", static_cast<int64_t>(merge_status_[1]),
      "kept_merge_pass_idx", merge_cur_pass_idx_[1],
      "kept_merge_files", merge_cur_pass_merged_files_[1],
      "kept_merge_total_files", merge_cur_pass_total_files_[1]);

public:
  static const int64_t GROUP_CNT = 2;         // DEL=0, KEPT=1
  // sort stage. total_* set once in init_by_param then read-only;
  // finished_* written by concurrent SortTasks via ATOMIC_INC, read via ATOMIC_LOAD.
  int64_t total_sort_task_count_;
  int64_t total_tablet_count_;
  int64_t finished_sort_task_count_;
  int64_t finished_tablet_count_;
  // merge stage, per group (DEL=0/KEPT=1). status inited to PENDING, pass_idx to -1, rest to 0.
  ObSSMarkSweepMergeStatus merge_status_[GROUP_CNT];
  int64_t merge_cur_pass_idx_[GROUP_CNT];
  int64_t merge_cur_pass_total_files_[GROUP_CNT];
  int64_t merge_cur_pass_merged_files_[GROUP_CNT];
};

// ====================================================================================
// Ctx shared across all tasks within a single ObSSMarkSweepDag invocation. Embedded in the
// Dag object, same lifetime. Tasks access it via static_cast<ObSSMarkSweepDag*>(get_dag())
// ->get_ctx(). Concurrent access rules:
//   * Read-only fields (tablet_with_meta_scn_arr_, *_meta_scn_*, tenant_id_, limits):
//     set once during init_by_param, read by all Worker/Merge/Diff tasks — safe.
//   * tablet_kept_meta_scn_map_: PrepareTask 单线程装载，全部 SortTask 只读访问。
//   * pipeline_: Worker tasks write via the pipeline's internal thread-safe interfaces;
//     Merge/Diff tasks run strictly after all Workers (DAG dependency) — no extra lock.
//   * tablet_del_meta_scn_infos_: written by concurrent SortTasks (one append per tablet)
//     under del_meta_scn_info_lock_; read by DiffTask single-threaded after the
//     SortTask -> Merge -> Diff dependency edge is satisfied.
// ====================================================================================
struct ObSSMarkSweepCtx
{
public:
  ObSSMarkSweepCtx();
  virtual ~ObSSMarkSweepCtx() { reset(); }
  int init_by_param(const ObSSMarkSweepDagParam &param);
  void reset();
  bool is_valid() const;
  int append_tablet_del_meta_scn_info(const ObTabletDelMetaScnInfo &info);
  // mark_delete_meta_scn is invalid if the tablet is not mark-deleted.
  int get_mark_delete_meta_scn(const ObSSGCTablet &gc_tablet,
                               share::SCN &mark_delete_meta_scn) const;
  // 查询 PrepareTask 冻结的 tablet kept_meta_scn；调用开始时重置输出。
  // map 缺少对应 Lock Row 时返回 OB_CANCELED；
  // 成功时输出有效且允许为 SCN::min()。
  int get_tablet_kept_meta_scn(const ObSSGCTablet &gc_tablet,
                               share::SCN &kept_meta_scn) const;
  TO_STRING_KV("tablet_cnt", tablet_with_meta_scn_arr_.count(), K_(snapshot_meta_scn),
      K_(safe_meta_scn), K_(reserved_meta_scns), K_(tenant_id), K_(batch_size), K_(memory_limit),
      K_(file_buf_size), K_(round_id), K_(progress), "del_meta_scn_info_cnt",
      tablet_del_meta_scn_infos_.count(), "mark_deleted_tablet_cnt", mark_deleted_tablets_.size(),
      "ls_ss_checkpoint_cnt", ls_ss_checkpoint_scn_map_.size(),
      "tablet_kept_meta_scn_cnt", tablet_kept_meta_scn_map_.size());

public:
  ObMarkSweepPipeline pipeline_;
  ObGCMacroBlockCompare compare_;
  ObSSGCTabletWithMetaScnArray tablet_with_meta_scn_arr_;
  share::SCN snapshot_meta_scn_;
  share::SCN safe_meta_scn_;
  common::ObSEArray<share::SCN, 8> reserved_meta_scns_;
  // mark-deleted tablet -> meta_scn of its mark-delete tablet meta version
  common::hash::ObHashMap<ObSSGCTablet, share::SCN> mark_deleted_tablets_;
  common::hash::ObHashMap<share::ObLSID, share::SCN> ls_ss_checkpoint_scn_map_;
  // PrepareTask 按相关 LS 全量读取 Tablet Lock Row 位点后冻结；SortTask 期间只读。
  common::hash::ObHashMap<ObSSGCTablet, share::SCN> tablet_kept_meta_scn_map_;
  // Per-tablet del_meta_scn info, produced by SortTask, consumed by DiffTask.
  mutable common::ObSpinLock del_meta_scn_info_lock_;
  common::ObArray<ObTabletDelMetaScnInfo> tablet_del_meta_scn_infos_;
  uint64_t tenant_id_;
  int64_t batch_size_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t round_id_;
  // mark sweep progress, read by fill_info_param while the dag runs.
  ObSSMarkSweepProgress progress_;
  // reported into the FINISH_MARK_SWEEP_GC event at report_result.
  // Not included in TO_STRING_KV to avoid log noise.
  ObSSMarkSweepCostStat cost_stat_;
  // Diagnostic recorder shared by all SortTasks of this round; created in PrepareTask only
  // when _enable_shared_mark_sweep_gc_diagnose is on, finished/aborted in report_result.
  // The manager owns the enabled gate; use diag_mgr_.is_ready_record() to decide whether to
  // record diagnose info. The manager handles its own per-bucket locking.
  ObSSMarkSweepDiagMgr diag_mgr_;
};


// ====================================================================================
// Param passed from the timer-task entry to create_and_add_dag<ObSSMarkSweepDag>. All fields
// are deep-copied into Ctx during init_by_param (Param is a stack object of the caller).
// ====================================================================================
struct ObSSMarkSweepDagParam : public share::ObIDagInitParam
{
public:
  ObSSMarkSweepDagParam()
    : tablet_with_meta_scn_arr_(), snapshot_meta_scn_(), safe_meta_scn_(), reserved_meta_scns_(),
      mark_deleted_tablets_(), tenant_id_(common::OB_INVALID_TENANT_ID), batch_size_(0),
      memory_limit_(0), file_buf_size_(0), round_id_(0)
  {}
  virtual ~ObSSMarkSweepDagParam() = default;
  int init(const uint64_t tenant_id,
           const int64_t round_id,
           const int64_t memory_limit,
           const int64_t file_buf_size,
           const int64_t batch_size);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV("tablet_cnt", tablet_with_meta_scn_arr_.count(), K_(snapshot_meta_scn),
      K_(safe_meta_scn), K_(reserved_meta_scns), "mark_deleted_cnt", mark_deleted_tablets_.count(),
      K_(tenant_id), K_(batch_size), K_(memory_limit), K_(file_buf_size), K_(round_id),
      "ls_ss_checkpoint_cnt", ls_ss_checkpoint_scn_map_.size());

public:
  // decides the size of a tmp file
  static const int64_t DEFAULT_MEMORY_LIMIT = 16L << 20; // 16MB
  // decides the io size of tmp file
  static const int64_t DEFAULT_FILE_BUF_SIZE = 32L << 10; // 32KB
  // 每个 SortTask 处理的 tablet 数量上限。
  static const int64_t DEFAULT_BATCH_SIZE = 100;
  ObSSGCTabletWithMetaScnArray tablet_with_meta_scn_arr_;
  share::SCN snapshot_meta_scn_;
  share::SCN safe_meta_scn_;
  common::ObSEArray<share::SCN, 8> reserved_meta_scns_;
  common::ObSEArray<ObSSGCMarkDeletedTablet, 16> mark_deleted_tablets_;
  common::hash::ObHashMap<share::ObLSID, share::SCN> ls_ss_checkpoint_scn_map_;
  uint64_t tenant_id_;
  int64_t batch_size_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  // round_id of this mark-sweep gc round; fetched by ObSSMarkSweepGCGenTask at the moment it
  // acquire/takeover the executor ownership and persisted into __all_shared_gc_control.gc_round_id.
  int64_t round_id_;
};


// ====================================================================================
// Input bundle for ObSSMarkSweepSortTask::get_del_and_kept_meta_scns_simply_.
// ====================================================================================
struct ObSSGetDelKeptMetaScnParam
{
public:
  ObSSGetDelKeptMetaScnParam(const ObSSGCTablet &tablet,
                             const share::SCN &tablet_meta_scn,
                             const share::SCN &tablet_kept_meta_scn,
                             const share::SCN &safe_meta_scn,
                             const share::SCN &mark_delete_meta_scn,
                             const common::ObIArray<share::SCN> &reserved_meta_scns,
                             const common::ObIArray<share::SCN> &all_meta_scns,
                             const common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_ss_checkpoint_scn_map)
    : tablet_(tablet),
      tablet_meta_scn_(tablet_meta_scn),
      tablet_kept_meta_scn_(tablet_kept_meta_scn),
      safe_meta_scn_(safe_meta_scn),
      mark_delete_meta_scn_(mark_delete_meta_scn),
      reserved_meta_scns_(reserved_meta_scns),
      all_meta_scns_(all_meta_scns),
      ls_ss_checkpoint_scn_map_(ls_ss_checkpoint_scn_map)
  {}
  ~ObSSGetDelKeptMetaScnParam() = default;
  TO_STRING_KV(K_(tablet), K_(tablet_meta_scn), K_(tablet_kept_meta_scn), K_(safe_meta_scn),
      K_(mark_delete_meta_scn), K_(reserved_meta_scns), K_(all_meta_scns),
      "ls_ss_checkpoint_cnt", ls_ss_checkpoint_scn_map_.size());

public:
  const ObSSGCTablet &tablet_;
  const share::SCN &tablet_meta_scn_;
  // PrepareTask 从 Lock Row 冻结的已提交保留位点。
  share::SCN tablet_kept_meta_scn_;
  const share::SCN &safe_meta_scn_;
  // meta_scn of the mark-delete tablet meta version; invalid if not mark-deleted
  const share::SCN &mark_delete_meta_scn_;
  const common::ObIArray<share::SCN> &reserved_meta_scns_;
  const common::ObIArray<share::SCN> &all_meta_scns_;
  const common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_ss_checkpoint_scn_map_;
};


// ====================================================================================
// ObSSMarkSweepDag — one round of mark-sweep GC generation for one tenant.
// Per-tenant deduplicated via hash() / operator==.
//
//   PrepareTask
//      ↓
//   SortTask[0] ─(generate_next_task)─→ SortTask[1] ─→ ... ─→ SortTask[N]
//      ↓  (every SortTask carries MergeStartTask as a child)
//   MergeStartTask ──────────────────────────────────────────────────────────── ─┐
//      ├─→ [DEL]  PassCtrlTask[r=0] ─→ ... ─→ PassCtrlTask[terminal] ─┐            │
//      └─→ [KEPT] PassCtrlTask[r=0] ─→ ... ─→ PassCtrlTask[terminal] ─┴─→ ... ─────┴→ MergeCompleteTask
//                  (EVERY PassCtrlTask wires MergeCompleteTask as its child, see below)   ↓
//                                                                                     DiffTask
//
// Per-group multi-pass merge subgraph (grown dynamically; identical for DEL and KEPT):
//
//   PassCtrlTask[r]   (spawned by MergeStartTask for r=0; by prev PassCtrlTask for r>0)
//      ├─→ BatchTask[r, 0] ─(generate_next_task)─→ BatchTask[r, 1] ─→ ... ─→ BatchTask[r, B-1]
//      │      (batches run concurrently; each merges one slice → one output file for next pass)
//      ├─→ PassCtrlTask[r+1]   (parents: PassCtrlTask[r] + all BatchTask[r,*])
//      └─→ MergeCompleteTask   (relay: every PassCtrlTask wires this at creation time)
//               ...
//           PassCtrlTask[terminal]   (input_count ≤ 1, no further pass needed)
//               └─→ MergeCompleteTask
//
// MergeCompleteTask's indegree is dynamic: 1 (MergeStartTask) + one per currently-live
// (created, not-yet-finished) PassCtrlTask of both groups. The relay keeps it >= 1 across
// every pass handoff so MergeCompleteTask is scheduled only after the later of the DEL/KEPT
// terminal PassCtrlTasks finishes. See create_first_task() in the .cpp for the rationale.
// ====================================================================================
class ObSSMarkSweepDag : public ObSSGCDagBase
{
public:
  ObSSMarkSweepDag();
  virtual ~ObSSMarkSweepDag();

  virtual bool operator==(const ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int report_result() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  
  ObSSMarkSweepCtx *get_ctx() { return ctx_; }
  // tenant_id_ is kept redundantly on the Dag (separate from ctx_->tenant_id_), so that
  // fake_dag identity ops (operator==/hash/fill_dag_key) work before ctx_ is allocated.
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(tenant_id), KPC_(ctx));

private:
  int alloc_ctx_();
  void free_ctx_();
  int check_if_need_report_result(bool &need_report_result);
  int report_result_();
  int update_gc_control_status_();

private:
  bool is_inited_;
  // Identity field, available even on a fake_dag without init_by_param. See get_tenant_id().
  uint64_t tenant_id_;
  // Heap-allocated in init_by_param via mtl_malloc (alloc_ctx_), freed via mtl_free in the
  // destructor (free_ctx_).
  ObSSMarkSweepCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDag);
};


class ObSSMarkSweepBaseTask : public ObSharedGCBaseTask
{
public:
  explicit ObSSMarkSweepBaseTask(const share::ObITask::ObITaskType task_type);
  virtual ~ObSSMarkSweepBaseTask() = default;

protected:
  int get_ctx(ObSSMarkSweepCtx *&ctx) const;

  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepBaseTask);
};


// ====================================================================================
// PrepareTask — initializes pipeline before the SortTask chain runs.
// ====================================================================================
class ObSSMarkSweepPrepareTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepPrepareTask();
  virtual ~ObSSMarkSweepPrepareTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepPrepareTask", KP(this));

private:
  int get_all_kept_meta_scns_(ObSSMarkSweepCtx &ctx);
  int get_ls_kept_meta_scns_(const share::ObLSID &ls_id,
                             const int64_t sql_timeout_us,
                             ObSSMarkSweepCtx &ctx);
  void try_init_diag_mgr_(ObSSMarkSweepCtx &ctx);

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepPrepareTask);
};


// ====================================================================================
// SortTask — processes tablet_with_meta_scn_arr_[start, end). Builds stack-local
// DEL/KEPT ObMarkSweepSorters whose ctor/dtor maintain pipeline.active_sorter_count_.
// ====================================================================================
class ObSSMarkSweepSortTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepSortTask();
  virtual ~ObSSMarkSweepSortTask() = default;
  int init(const int64_t start_idx, const int64_t end_idx);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepSortTask", KP(this),
                       K_(start_tablet_idx), K_(end_tablet_idx));

private:
  struct ObSSPersistKeptMetaScnItem final
  {
  public:
    ObSSPersistKeptMetaScnItem()
      : gc_tablet_(), kept_meta_scn_()
    {}
    ~ObSSPersistKeptMetaScnItem() = default;
    void reset()
    {
      gc_tablet_.reset();
      kept_meta_scn_.reset();
    }
    bool is_valid() const
    {
      return gc_tablet_.is_valid()
          && kept_meta_scn_.is_valid()
          && !kept_meta_scn_.is_min()
          && !kept_meta_scn_.is_max();
    }
    TO_STRING_KV(K_(gc_tablet), K_(kept_meta_scn));
  public:
    ObSSGCTablet gc_tablet_;
    share::SCN kept_meta_scn_;
  };

  struct ObMarkSweepSorterPair final
  {
  public:
    ObMarkSweepSorterPair() : del_sorter_(), kept_sorter_(), pipeline_(nullptr) {}
    ~ObMarkSweepSorterPair() = default;
    int init(ObMarkSweepPipeline &pipeline);
    int finish_and_add_file();
    ObMarkSweepSorter &del_sorter() { return del_sorter_; }
    ObMarkSweepSorter &kept_sorter() { return kept_sorter_; }
  private:
    ObMarkSweepSorter del_sorter_;
    ObMarkSweepSorter kept_sorter_;
    // 非拥有（non-owning）裸指针，指向外部 ObMarkSweepPipeline，
    // 生命周期由 pipeline 持有方（ObSSMarkSweepCtx）保证长于本 pair；本 struct 不负责释放。
    ObMarkSweepPipeline *pipeline_;
    DISALLOW_COPY_AND_ASSIGN(ObMarkSweepSorterPair);
  };

  struct ProcessOneTabletFn final
  {
  public:
    ProcessOneTabletFn(ObSSMarkSweepSortTask &task,
                       ObSSMarkSweepCtx &ctx,
                       const ObSSGCTabletWithMetaScn &tablet,
                       const share::SCN &tablet_kept_meta_scn,
                       ObMarkSweepSorterPair &sorter_pair,
                       ObSSPersistKeptMetaScnItem &persist_item);
    ~ProcessOneTabletFn() = default;
    int operator()();
  private:
    // 以下引用均不持有外部对象，生命周期由调用方 process() 栈帧保证；
    // tablet_kept_meta_scn_ 按值保存。
    ObSSMarkSweepSortTask &task_;
    ObSSMarkSweepCtx &ctx_;
    const ObSSGCTabletWithMetaScn &tablet_;
    share::SCN tablet_kept_meta_scn_;
    ObMarkSweepSorterPair &sorter_pair_;
    ObSSPersistKeptMetaScnItem &persist_item_;
  };

  struct PersistKeptMetaScnFn final
  {
  public:
    PersistKeptMetaScnFn(
        ObSSMarkSweepSortTask &task,
        ObSSMarkSweepCtx &ctx,
        const ObSSPersistKeptMetaScnItem &item)
      : task_(task), ctx_(ctx), item_(item)
    {}
    ~PersistKeptMetaScnFn() = default;
    int operator()()
    {
      return task_.persist_kept_meta_scn_with_trans_(item_, ctx_);
    }
  private:
    ObSSMarkSweepSortTask &task_;
    ObSSMarkSweepCtx &ctx_;
    const ObSSPersistKeptMetaScnItem &item_;
  };

private:
  int get_all_meta_scns_(const ObSSGCTablet &gc_tablet,
                         const share::SCN &snapshot_meta_scn,
                         common::ObIArray<share::SCN> &all_meta_scns);
  int get_del_and_kept_meta_scns_(const share::SCN &tablet_meta_scn,
                                  const share::SCN &safe_meta_scn,
                                  const common::ObIArray<share::SCN> &reserved_meta_scns,
                                  const common::ObIArray<share::SCN> &all_meta_scns,
                                  common::ObIArray<share::SCN> &del_meta_scns,
                                  common::ObIArray<share::SCN> &kept_meta_scns);
  // MIN3 未给出回收边界时，改用 PrepareTask 冻结的 Lock Row 保留位点作为边界。
  int calc_recycle_boundary_from_lock_row_(const ObSSGetDelKeptMetaScnParam &param,
                                           share::SCN &min_meta_scn,
                                           bool &can_del_all);
  // 回收边界取自 MIN(tablet_meta_scn, safe_meta_scn, real_min_reserved_meta_scn)。
  int calc_recycle_boundary_from_constraints_(const ObSSGetDelKeptMetaScnParam &param,
                                              const share::SCN &min_meta_scn,
                                              bool &can_del_all);
  int get_del_and_kept_meta_scns_simply_(const ObSSGetDelKeptMetaScnParam &param,
                                         common::ObIArray<share::SCN> &del_meta_scns,
                                         common::ObIArray<share::SCN> &kept_meta_scns);
  // 判断 gc_tablet 在 cur_meta_scn 这个版本下的 max_replayed_scn 是否 >= 其所在 ls 的
  // ss_checkpoint_scn。is_exceed=true 表示该 meta_scn 应保留(kept)，false 表示可删(del)。
  int check_if_replayed_scn_exceed_checkpoint_scn_(
      const ObSSGCTablet &gc_tablet,
      const share::SCN &cur_meta_scn,
      const common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_ss_checkpoint_scn_map,
      bool &is_exceed);
  int process_one_tablet_(ObSSMarkSweepCtx &ctx,
                          const ObSSGCTabletWithMetaScn &tablet_with_meta_scn,
                          const share::SCN &tablet_kept_meta_scn,
                          ObMarkSweepSorterPair &sorter_pair,
                          ObSSPersistKeptMetaScnItem &persist_item);
  int process_one_group_(const ObMarkSweepGroupType group_type,
                         const ObSSGCTablet &gc_tablet,
                         const common::ObIArray<share::SCN> &group_meta_scns,
                         ObSSMarkSweepCtx &ctx,
                         ObMarkSweepSorter &sorter,
                         common::ObIArray<blocksstable::MacroBlockId> &diag_macro_ids);
  // 计算应持久化到 Lock Row 的保留位点。kept 为空且 del 非空（tablet 所有版本连同
  // mark_delete 版本自身都可删）时，位点必须推进到 mark_delete_meta_scn + 1，
  // 否则后续轮次会沿用旧位点把已删版本重新判定为 kept，导致 kept 集合回退。
  // kept_meta_scn 为 invalid 表示本轮无需推进位点。
  int calc_kept_meta_scn_(const ObSSGetDelKeptMetaScnParam &param,
                          const common::ObIArray<share::SCN> &del_meta_scns,
                          const common::ObIArray<share::SCN> &kept_meta_scns,
                          share::SCN &kept_meta_scn);
  int build_kept_meta_scn_persist_item_(const ObSSGCTablet &gc_tablet,
                                        const share::SCN &kept_meta_scn,
                                        ObSSPersistKeptMetaScnItem &item);
  // SortTask 结束时先批量过滤，再逐 tablet 持久化 kept_meta_scn。
  int persist_kept_meta_scns_(common::ObIArray<ObSSPersistKeptMetaScnItem> &items,
                              ObSSMarkSweepCtx &ctx);
  // 使用 PrepareTask 冻结的只读 map 过滤无需推进的 item。
  int filter_kept_meta_scn_items_(
      const common::ObIArray<ObSSPersistKeptMetaScnItem> &items,
      const ObSSMarkSweepCtx &ctx,
      common::ObIArray<ObSSPersistKeptMetaScnItem> &filtered_items);
  // 为单个 tablet 启动独立事务，失败时由调用方按整个事务重试。
  int persist_kept_meta_scn_with_trans_(const ObSSPersistKeptMetaScnItem &item,
                                        ObSSMarkSweepCtx &ctx);
  int persist_kept_meta_scn_(const ObSSPersistKeptMetaScnItem &item,
                             common::ObISQLClient &trans);
  // Best-effort: record this tablet's DEL/KEPT macro ids and meta_scns decision to the
  // round-level diag manager (object storage). Failures never affect the GC main flow.
  void try_record_diag_info_(ObSSMarkSweepCtx &ctx,
                            const ObSSGCTablet &gc_tablet,
                            const common::ObIArray<blocksstable::MacroBlockId> &del_macro_ids,
                            const common::ObIArray<blocksstable::MacroBlockId> &kept_macro_ids,
                            const common::ObIArray<share::SCN> &del_meta_scns,
                            const common::ObIArray<share::SCN> &kept_meta_scns);
  int append_tablet_del_meta_scn_info_(ObSSMarkSweepCtx &ctx,
                                       const ObSSGCTablet &gc_tablet,
                                       const common::ObIArray<share::SCN> &del_meta_scns,
                                       const common::ObIArray<share::SCN> &kept_meta_scns);

private:
  bool is_inited_;
  int64_t start_tablet_idx_;
  int64_t end_tablet_idx_;
  common::ObArray<ObSSPersistKeptMetaScnItem> kept_meta_scn_persist_items_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepSortTask);
};


// ====================================================================================
// Multi-pass merge subgraph.
//
//   MergeStartTask
//     calls pipeline.prepare_merge() to seed both group mergers, then spawns
//     two MergePassCtrlTasks (one per group). It is the single static parent of
//     the dynamically-grown pass_ctrl/batch chain.
//
//   MergePassCtrlTask
//     One per group, allocated by MergeStartTask or by the previous-round pass ctrl task.
//     On process(): calls merger.prepare_next_pass(); if get_input_count() <= 1,
//     finalizes the group (caller has wired merge_complete as a child); otherwise
//     allocates the first MergeBatchTask of this round + the next-round pass ctrl task,
//     wiring batch[0] -> next_pass_ctrl. Subsequent batches are spawned by
//     MergeBatchTask::generate_next_task() (chain pattern).
//
//   MergeBatchTask
//     Calls merger.batch_merge(start, end). Each batch is independent and
//     concurrently appends one merged file to merger.next_inputs_.
//
//   MergeCompleteTask
//     Called once both DEL and KEPT side pass ctrl tasks reach their terminal round.
//     Calls pipeline.complete_merge() (= init_diff_iter + clear file lists).
// ====================================================================================
class ObSSMarkSweepMergeStartTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepMergeStartTask();
  virtual ~ObSSMarkSweepMergeStartTask() = default;
  int init(ObSSMarkSweepMergeCompleteTask *merge_complete_task);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepMergeStartTask", KP(this), KP_(merge_complete_task));

private:
  // Allocate the first pass ctrl task for `group`, wire its incoming/outgoing edges,
  // and add it to the dag. If the group already has <= 1 input file, the first pass
  // ctrl task is itself the terminal pass ctrl task, so merge_complete is wired as its child.
  int create_pass_ctrl_task_(ObMarkSweepGroupType group);

private:
  bool is_inited_;
  // Carried as a member for wiring the terminal-round pass ctrl -> merge_complete edge.
  ObSSMarkSweepMergeCompleteTask *merge_complete_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepMergeStartTask);
};


class ObSSMarkSweepMergePassCtrlTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepMergePassCtrlTask();
  virtual ~ObSSMarkSweepMergePassCtrlTask() = default;
  // pass_idx: 0     -- this is the first-pass pass ctrl task of a group
  //   (cur_inputs_ has already been populated by MergeStartTask::prepare_merge; no prepare_next_pass needed);
  // pass_idx: >0    -- subsequent passes (must call prepare_next_pass to
  //   promote the previous batch output from next_inputs_ into cur_inputs_).
  int init(const ObMarkSweepGroupType group,
           const int64_t pass_idx,
           ObSSMarkSweepMergeCompleteTask *merge_complete_task);
  virtual int process() override;
  ObMarkSweepGroupType get_group() const { return group_; }
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepMergePassCtrlTask", KP(this), K_(group),
      K_(pass_idx), KP_(merge_complete_task));

private:
  bool is_inited_;
  ObMarkSweepGroupType group_;
  int64_t pass_idx_;
  // Carried as a member for wiring the terminal-round pass ctrl task -> merge_complete edge.
  ObSSMarkSweepMergeCompleteTask *merge_complete_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepMergePassCtrlTask);
};


class ObSSMarkSweepMergeBatchTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepMergeBatchTask();
  virtual ~ObSSMarkSweepMergeBatchTask() = default;
  int init(const ObMarkSweepGroupType group,
           const int64_t batch_idx,
           const int64_t total_batch_count,
           const int64_t pass_idx);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepMergeBatchTask", KP(this),
      K_(group), K_(batch_idx), K_(total_batch_count), K_(pass_idx));

private:
  struct MergeBatchFn {
    MergeBatchFn(ObSSMarkSweepMergeBatchTask &task,
                 ObSSMarkSweepCtx &ctx,
                 const ObMarkSweepGroupType group,
                 const int64_t batch_idx,
                 const int64_t total_batch_count);
    int operator()();

    ObSSMarkSweepMergeBatchTask &task_;
    ObSSMarkSweepCtx &ctx_;
    ObMarkSweepGroupType group_;
    int64_t batch_idx_;
    int64_t total_batch_count_;
  };

  struct MergeBatchOnRetryFn {
    explicit MergeBatchOnRetryFn(ObSSMarkSweepMergeBatchTask &task);
    int operator()();

    ObSSMarkSweepMergeBatchTask &task_;
  };

private:
  bool is_inited_;
  ObMarkSweepGroupType group_;
  int64_t batch_idx_;
  int64_t total_batch_count_;
  // The merge pass this batch belongs to; carried for per-pass cost statistics.
  int64_t pass_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepMergeBatchTask);
};


class ObSSMarkSweepMergeCompleteTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepMergeCompleteTask();
  virtual ~ObSSMarkSweepMergeCompleteTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepMergeCompleteTask", KP(this));

private:
  struct CompleteMergeFn {
    CompleteMergeFn(ObSSMarkSweepMergeCompleteTask &task, ObSSMarkSweepCtx &ctx);
    int operator()();

    ObSSMarkSweepMergeCompleteTask &task_;
    ObSSMarkSweepCtx &ctx_;
  };

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepMergeCompleteTask);
};


// ====================================================================================
// DiffTask — iterates the del-kept sorted-set difference and performs reclamation.
// ====================================================================================
class ObSSMarkSweepDiffTask : public ObSSMarkSweepBaseTask
{
public:
  ObSSMarkSweepDiffTask();
  virtual ~ObSSMarkSweepDiffTask() = default;
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV("task", "ObSSMarkSweepDiffTask", KP(this));

private:
  struct DiffFn {
  public:
    DiffFn(ObSSMarkSweepDiffTask &task, ObSSMarkSweepCtx &ctx,
           const ObGCMacroBlockItem *&item, int64_t &del_macro_cnt,
           int64_t &kept_macro_cnt, int64_t &inserted_gc_task_rows);
    int operator()();

  private:
    int insert_task_(const ObSharedGCMacroIdList &macro_id_list);
    int insert_task_and_reset_list_(ObSharedGCMacroIdList &macro_id_list,
                                    int64_t &cur_list_serialize_size);
    int delete_meta_scns_();
    int delete_meta_scns_for_tablet_(const ObTabletDelMetaScnInfo &info);
    int check_executor_and_round_id_();

  private:
    ObSSMarkSweepDiffTask &task_;
    ObSSMarkSweepCtx &ctx_;
    const ObGCMacroBlockItem *&item_;
    int64_t &del_macro_cnt_;
    int64_t &kept_macro_cnt_;
    int64_t &inserted_gc_task_rows_;
  };

  struct DiffOnRetryFn {
    DiffOnRetryFn(ObSSMarkSweepDiffTask &task, ObSSMarkSweepCtx &ctx,
                  int64_t &del_macro_cnt, int64_t &inserted_gc_task_rows);
    int operator()();

    ObSSMarkSweepDiffTask &task_;
    ObSSMarkSweepCtx &ctx_;
    int64_t &del_macro_cnt_;
    int64_t &inserted_gc_task_rows_;
  };

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDiffTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_GARBAGE_COLLECTOR_DAG_OB_SS_MARK_SWEEP_DAG_H_
