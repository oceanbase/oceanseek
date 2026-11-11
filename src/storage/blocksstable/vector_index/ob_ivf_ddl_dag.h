/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_DAG_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_DAG_H_

#include "common/ob_tablet_id.h"
#include "lib/allocator/page_arena.h"
#include "share/scheduler/ob_independent_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_sstable_writer.h"

namespace oceanbase
{
namespace share
{
class ObVsagMemContext;
struct ObVectorNormalizeInfo;
}
namespace blocksstable
{
class ObIVFDDLSSTableWriter;
}
namespace storage
{

class ObColumnClusteredDag;
struct ObIVFBuildParam;
class ObIVFBuildSchemaCtx;
class ObIVFDDLDagCtx;
class ObIVFDDLInput;
class ObIVFDDLTask;
class ObIVFDDLFinishTask;
class ObIVFDDLHcaAdapter;
class ObIVFBuildMonitorInfo;

// Input is prepared by the upstream module before this DAG is scheduled. This
// task only starts the runtime owned by the IVF DDL DagCtx.
class ObIVFDDLPrepareTask final : public share::ObITaskWithMonitor
{
public:
  ObIVFDDLPrepareTask();
  virtual ~ObIVFDDLPrepareTask() = default;
  int init(ObIVFDDLDagCtx *ivf_ddl_dag_ctx);

protected:
  virtual int process() override;

private:
  bool is_inited_;
  ObIVFDDLDagCtx *ivf_ddl_dag_ctx_;
};

// One instance is the fan-in barrier for one immutable TaskPlan. It only
// keeps the IVF DDL DagCtx and TaskPlan used to advance the runtime.
class ObIVFDDLFinishTask final : public share::ObITask
{
public:
  ObIVFDDLFinishTask();
  virtual ~ObIVFDDLFinishTask() = default;
  int init(
      ObIVFDDLDagCtx *ivf_ddl_dag_ctx,
      const ObIVFTaskPlan *task_plan);
  virtual int64_t get_sub_task_id() const override
  {
    return OB_ISNULL(task_plan_) ? -1 : task_plan_->get_plan_id();
  }

protected:
  virtual int process() override;

private:
  bool is_inited_;
  ObIVFDDLDagCtx *ivf_ddl_dag_ctx_;
  const ObIVFTaskPlan *task_plan_;
};

// Each node keeps one logical task and its monitor/retry identity. A successful
// node replenishes its execution lane only after processing, keeping the live
// graph bounded by the DDL/PX worker count rather than the logical plan size.
class ObIVFDDLTask final : public share::ObITaskWithMonitor
{
public:
  ObIVFDDLTask();
  virtual ~ObIVFDDLTask();
  int init(
      ObIVFDDLDagCtx *ivf_ddl_dag_ctx,
      const ObIVFTaskPlan *task_plan,
      const int64_t task_idx);
  virtual int64_t get_sub_task_id() const override { return task_idx_; }
  virtual int post_generate_next_task() override;

protected:
  virtual int process() override;
  virtual int inner_add_monitor_info(ObDDLDagMonitorNode &node) override;
  virtual int post_generate_next_task(share::ObITask *&next_task) override;

private:
  bool is_inited_;
  ObIVFDDLDagCtx *ivf_ddl_dag_ctx_;
  const ObIVFTaskPlan *task_plan_;
  int64_t task_idx_;
};

// This object describes and materializes one tablet's IVF DDL task graph, but
// it is not itself submitted to the scheduler. Its lifetime is owned by the
// corresponding ObIVFTabletContext, while scheduled tasks only borrow it.
// ObColumnClusteredDag remains the scheduler DAG and provides task allocation,
// publication, and the PX lifecycle. The DagCtx also owns the selected process
// type's planner.
class ObIVFDDLDagCtx final : public ObIVFMergePerfObserver,
    public blocksstable::ObIVFDDLRawWriterProvider
{
public:
  ObIVFDDLDagCtx();
  ~ObIVFDDLDagCtx();
  int init(
      ObColumnClusteredDag &owner_dag,
      const ObIVFProcessType process_type,
      const common::ObTabletID &tablet_id);
  int generate_initial_tasks(
      common::ObIArray<share::ObITask *> &need_schedule_tasks);
  ObIVFProcessType get_process_type() const { return process_type_; }
  TO_STRING_KV(K_(is_inited), K_(runtime_initialized), K_(process_type),
      K_(tablet_id), KP_(owner_dag), KP_(planner));

private:
  friend class ObIVFDDLPrepareTask;
  friend class ObIVFDDLTask;
  friend class ObIVFDDLFinishTask;

  virtual void on_step_perf(const ObIVFMergeStepPerf &perf) override;
  void init_task_perf(ObIVFBuildMonitorInfo &info, const int64_t task_idx);
  void finish_build_perf(const int ret_code);
  int start(share::ObITask &prepare_task);
  int process_task(const ObIVFTaskPlan &task_plan, const int64_t task_idx);
  int finish_task_plan(
      share::ObITask &finish_task,
      const ObIVFTaskPlan &task_plan);
  int materialize_task_plan(
      share::ObITask &parent_task,
      const ObIVFTaskPlan &task_plan);
  int generate_next_task(const ObIVFTaskPlan &task_plan, share::ObITask *&next_task);
  int check_task_dispatch_status() const;
  void cancel_task_dispatch(const int ret_code);
  int materialize_physical_finish(share::ObITask &parent_task);
  virtual int create_center_writer(ObITabletSliceWriter *&writer) override;
  virtual int create_raw_writer(
      const int64_t task_idx, ObITabletSliceWriter *&writer) override;
  virtual int create_posting_writer(int64_t task_idx, ObITabletSliceWriter *&writer) override;
  virtual void destroy_writer(ObITabletSliceWriter *writer) override;
  int create_writer_(const int64_t slice_idx, ObITabletSliceWriter *&writer);
  int init_full_build_runtime_();
  int prepare_build_param_(ObIVFBuildParam &build_param);
  void destroy_runtime_();
  int release_build_resources();
  bool is_owner_inited() const;
  common::ObIAllocator &get_allocator();

  bool is_inited_;
  bool runtime_initialized_;
  bool build_finished_;
  ObColumnClusteredDag *owner_dag_;
  ObIVFProcessType process_type_;
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator runtime_arena_;
  common::ObSafeArenaAllocator runtime_allocator_;
  ObIVFBuildSchemaCtx *schema_ctx_;
  ObIVFDDLInput *input_adapter_;
  ObIVFDDLHcaAdapter *hca_algorithm_;
  share::ObVectorNormalizeInfo *center_norm_info_;
  share::ObVsagMemContext *hgraph_allocator_;
  blocksstable::ObIVFDDLSSTableWriter *sstable_writer_;
  ObIVFDagPlanner *planner_;
  // The plan pointer changes only at the drained phase barrier. Task indexes
  // are claimed atomically; the first failure stops replenishment in all lanes.
  const ObIVFTaskPlan *dispatch_task_plan_;
  int64_t next_task_idx_;
  int dispatch_ret_;
  ObIVFMergeProcessor *full_build_processor_;
  ObIVFFullBuildPlanner *full_build_planner_;
  ObIVFBuildMonitorInfo *step_monitor_;
  ObIVFBuildMonitorInfo *build_monitor_;
  ObIVFMergeStepPerf current_step_perf_;
  int64_t build_start_us_;
  int64_t finished_steps_;
  bool build_perf_finished_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLDagCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_DAG_H_
