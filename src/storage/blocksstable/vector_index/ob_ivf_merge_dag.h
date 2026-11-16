/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_DAG_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_DAG_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObVsagMemContext;
struct ObVectorNormalizeInfo;
}
namespace blocksstable
{
class ObDataStoreDesc;
class ObIVFMergeSSTableWriter;
}
namespace storage
{
class ObIVFDDLHcaAdapter;
class ObIVFBuildMonitorInfo;
class ObDDLDagMonitorNode;
}
namespace compaction
{

struct ObAsyncIndexMergeCtx;
class ObAsyncIndexMergeDag;
class ObIVFMergeInputAdapter;
class ObIVFMergeTask;
class ObIVFMergeFinishTask;
class ObIVFMergeDagCtx;
class ObIVFMergeCaptureCtx;
class ObIVFMergeInputReadyTask;

// IVF-specific graph control composed into the generic async-index merge DAG.
// It is not an independently scheduled ObIDag. This context owns the IVF
// input/runtime adapters and planner/processor for the whole DAG lifetime,
// while ObAsyncIndexMergeDag owns only the materialized scheduler tasks.
class ObIVFMergeDagCtx final : public storage::ObIVFMergePerfObserver
{
public:
  ObIVFMergeDagCtx();
  ~ObIVFMergeDagCtx();
  int start(
      ObAsyncIndexMergeDag &merge_dag,
      ObAsyncIndexMergeCtx &merge_ctx,
      share::ObITask &prepare_task);
  bool has_monitor() const { return nullptr != monitor_node_; }

private:
  friend class ObIVFMergeInputReadyTask;
  int finish_capture(share::ObITask &ready_task);
  friend class ObIVFMergeTask;
  friend class ObIVFMergeFinishTask;
  friend class ObIVFMergeCacheTask;

  // Internal test seam: replace only the source vectors while keeping the
  // production HCA/writer lifecycle inside this IVF context.
  int start_with_input(
      ObAsyncIndexMergeDag &merge_dag,
      ObAsyncIndexMergeCtx &merge_ctx,
      const storage::ObIVFMergeInput &merge_input,
      share::ObITask &prepare_task);
  // Internal framework-test seam: replace all runtime adapters.
  int start_with_runtime(
      ObAsyncIndexMergeDag &merge_dag,
      ObAsyncIndexMergeCtx &merge_ctx,
      const storage::ObIVFMergeInput &merge_input,
      storage::ObIVFMergeAlgorithm &algorithm,
      storage::ObIVFMergeQuantizer &physical_output,
      share::ObITask &prepare_task,
      const storage::ObIVFProcessType process_type = storage::ObIVFProcessType::FULL_REBUILD,
      const storage::ObIVFMergePrior *prior = nullptr);

  void init_monitor(ObAsyncIndexMergeDag &dag, ObAsyncIndexMergeCtx &ctx);
  virtual void on_step_perf(const storage::ObIVFMergeStepPerf &perf) override;
  void init_task_perf(storage::ObIVFBuildMonitorInfo &info, const int64_t task_idx);
  void finish_build_perf(const int ret_code);
  void release_samples_after_step_(const storage::ObIVFMergeStepPerf &finished_step);
  const char *strategy_name() const;

  int check_single_inc_reuse_(ObAsyncIndexMergeCtx &merge_ctx, bool &single_inc) const;
  int prepare_reuse_layout_(ObAsyncIndexMergeCtx &merge_ctx, const bool clustered);
  int init_merge_input_(ObAsyncIndexMergeCtx &merge_ctx, bool &reused);
  int check_input_reuse_(ObAsyncIndexMergeCtx &merge_ctx, bool &can_reuse, const bool single_inc) const;
  int try_reuse_input_(ObAsyncIndexMergeCtx &merge_ctx, bool &reused, const bool single_inc);
  int init_merge_runtime_(
      ObAsyncIndexMergeCtx &merge_ctx,
      const storage::ObIVFMergeInput &input);
  void destroy_runtime_();
  void release_finished_input_();

  int start_runtime(share::ObITask &prepare_task);
  int process_task(
      const storage::ObIVFTaskPlan &task_plan,
      const int64_t task_idx);
  int finish_task_plan(
      share::ObITask &finish_task,
      const storage::ObIVFTaskPlan &task_plan);
  int materialize_task_plan(
      share::ObITask &parent_task,
      const storage::ObIVFTaskPlan &task_plan);
  int materialize_finish(share::ObITask &parent_task);

  bool is_inited_;
  bool reuse_input_;
  storage::ObIVFProcessType process_type_;
  ObAsyncIndexMergeDag *owner_dag_;
  ObAsyncIndexMergeCtx *merge_ctx_;
  storage::ObIVFDagPlanner *planner_;
  // Declared before the processor so it outlives all processor-owned buffers.
  storage::ObIVFMergeRuntimeAllocator processor_allocator_;
  storage::ObIVFMergeProcessor merge_processor_;
  storage::ObIVFFullRebuildPlanner full_rebuild_planner_;
  storage::ObIVFConcatPlanner concat_planner_;
  storage::ObIVFInsertionPlanner insertion_planner_;
  blocksstable::ObDataStoreDesc *data_store_desc_;
  storage::ObIVFDDLHcaAdapter *hca_algorithm_;
  share::ObVectorNormalizeInfo *center_norm_info_;
  share::ObVsagMemContext *hgraph_allocator_;
  ObIVFMergeInputAdapter *input_adapter_;
  ObIVFMergeCaptureCtx *capture_ctx_;
  blocksstable::ObIVFMergeSSTableWriter *sstable_writer_;
  common::ObIAllocator *runtime_allocator_;
  common::ObIAllocator *input_allocator_; // labeled compaction arena, owns sealed input/prior buffers
  storage::ObDDLDagMonitorNode *monitor_node_;
  storage::ObIVFBuildMonitorInfo *build_monitor_;
  storage::ObIVFBuildMonitorInfo *step_monitor_;
  storage::ObIVFMergeStepPerf current_step_perf_;
  int64_t build_start_us_;
  int64_t finished_steps_;
  bool build_perf_finished_;
  bool build_finished_;

};

// Fan-in of the capture tasks. Once sealed, only the input adapter is used by build.
class ObIVFMergeInputReadyTask final : public share::ObITask
{
public:
  ObIVFMergeInputReadyTask() : ObITask(TASK_TYPE_SSTABLE_MERGE_PREPARE), ctx_(nullptr) {}
  int init(ObIVFMergeDagCtx *ctx);
protected:
  virtual int process() override;
private:
  ObIVFMergeDagCtx *ctx_;
};

// One task executes one task_idx in an immutable IVF TaskPlan. It only keeps
// the IVF Merge DagCtx plus its position in that batch.
class ObIVFMergeTask final : public share::ObITask
{
public:
  ObIVFMergeTask();
  virtual ~ObIVFMergeTask();
  int init(
      ObIVFMergeDagCtx *ivf_merge_dag_ctx,
      const storage::ObIVFTaskPlan *task_plan,
      const int64_t task_idx);
  virtual int64_t get_sub_task_id() const override { return task_idx_; }

protected:
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;

private:
  bool is_inited_;
  ObIVFMergeDagCtx *ivf_merge_dag_ctx_;
  const storage::ObIVFTaskPlan *task_plan_;
  int64_t task_idx_;
  storage::ObIVFBuildMonitorInfo *monitor_info_;
};

// This is the fan-in barrier for one IVF TaskPlan. Physical finish graph
// selection remains inside the IVF Merge DagCtx.
class ObIVFMergeFinishTask final : public share::ObITask
{
public:
  ObIVFMergeFinishTask();
  virtual ~ObIVFMergeFinishTask() = default;
  int init(
      ObIVFMergeDagCtx *ivf_merge_dag_ctx,
      const storage::ObIVFTaskPlan *task_plan);
  virtual int64_t get_sub_task_id() const override
  {
    return OB_ISNULL(task_plan_) ? -1 : task_plan_->get_plan_id();
  }

protected:
  virtual int process() override;

private:
  bool is_inited_;
  ObIVFMergeDagCtx *ivf_merge_dag_ctx_;
  const storage::ObIVFTaskPlan *task_plan_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_DAG_H_
