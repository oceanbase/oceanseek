/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_ddl_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_monitor.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_schema_ctx.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_sstable_writer.h"
#include "storage/ddl/ob_async_index_macro_writer.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"

#include "share/ob_debug_sync.h"
#include "lib/utility/ob_tracepoint.h"

#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "lib/lock/ob_spin_lock.h"
#include <utility>

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace storage
{

ERRSIM_POINT_DEF(EN_ASYNC_IVF_INITIAL_BUILD_FAIL);

namespace
{

template <typename T, typename... Args>
int alloc_ddl_runtime_object(
    ObIAllocator &allocator,
    T *&object,
    Args&&... args)
{
  int ret = OB_SUCCESS;
  object = nullptr;
  void *buf = allocator.alloc(sizeof(T));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    object = new (buf) T(std::forward<Args>(args)...);
  }
  return ret;
}

} // namespace



ObIVFDDLPrepareTask::ObIVFDDLPrepareTask()
  : ObITaskWithMonitor(ObITask::TASK_TYPE_DDL_PREPARE_SCAN),
    is_inited_(false),
    ivf_ddl_dag_ctx_(nullptr)
{
}

int ObIVFDDLPrepareTask::init(ObIVFDDLDagCtx *ivf_ddl_dag_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVFDDL prepare task init twice", K(ret));
  } else if (OB_ISNULL(ivf_ddl_dag_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVFDDL dag ctx is null", K(ret));
  } else {
    ivf_ddl_dag_ctx_ = ivf_ddl_dag_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFDDLPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_ddl_dag_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL prepare task is not initialized", K(ret), KP_(ivf_ddl_dag_ctx));
  } else if (OB_FAIL(ivf_ddl_dag_ctx_->start(*this))) {
    LOG_WARN("fail to start IVFDDL process", K(ret));
  } else if (ivf_ddl_dag_ctx_->build_finished_
      && OB_FAIL(ivf_ddl_dag_ctx_->release_build_resources())) {
    LOG_WARN("failed to release empty IVF build resources", K(ret));
  }
  return ret;
}

ObIVFDDLFinishTask::ObIVFDDLFinishTask()
  : ObITask(ObITask::TASK_TYPE_DDL_MERGE_PREPARE),
    is_inited_(false),
    ivf_ddl_dag_ctx_(nullptr),
    task_plan_(nullptr)
{
}

int ObIVFDDLFinishTask::init(
    ObIVFDDLDagCtx *ivf_ddl_dag_ctx,
    const ObIVFTaskPlan *task_plan)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVFDDL finish task init twice", K(ret));
  } else if (OB_ISNULL(ivf_ddl_dag_ctx) || OB_ISNULL(task_plan) || !task_plan->is_valid()
      || task_plan->get_process_type() != ivf_ddl_dag_ctx->get_process_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVFDDL finish task argument",
        K(ret), KP(ivf_ddl_dag_ctx), KP(task_plan));
  } else {
    ivf_ddl_dag_ctx_ = ivf_ddl_dag_ctx;
    task_plan_ = task_plan;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFDDLFinishTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_ddl_dag_ctx_) || OB_ISNULL(task_plan_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL finish task is not initialized",
        K(ret), KP_(ivf_ddl_dag_ctx), KP_(task_plan));
  } else if (OB_FAIL(ivf_ddl_dag_ctx_->finish_task_plan(*this, *task_plan_))) {
    LOG_WARN("fail to finish IVFDDL task plan", K(ret), KPC_(task_plan));
  } else if (ivf_ddl_dag_ctx_->build_finished_) {
    // The planner call has returned and all output tasks crossed this barrier.
    // Drop the borrowed plan before reclaiming its runtime storage.
    task_plan_ = nullptr;
    if (OB_FAIL(ivf_ddl_dag_ctx_->release_build_resources())) {
      LOG_WARN("failed to release completed IVF build resources", K(ret));
    }
  }
  return ret;
}

ObIVFDDLTask::ObIVFDDLTask()
  : ObITaskWithMonitor(ObITask::TASK_TYPE_DDL_VECTOR_INDEX_BUILD_AND_WRITE_PIPELINE),
    is_inited_(false),
    ivf_ddl_dag_ctx_(nullptr),
    task_plan_(nullptr),
    task_idx_(-1)
{
}

ObIVFDDLTask::~ObIVFDDLTask()
{
  if (nullptr != monitor_info_) {
    static_cast<ObIVFBuildMonitorInfo *>(monitor_info_)->cancel_unfinished();
  }
}

int ObIVFDDLTask::init(
    ObIVFDDLDagCtx *ivf_ddl_dag_ctx,
    const ObIVFTaskPlan *task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVFDDL task init twice", K(ret));
  } else if (OB_ISNULL(ivf_ddl_dag_ctx) || OB_ISNULL(task_plan)
      || !task_plan->is_valid()
      || task_plan->get_process_type() != ivf_ddl_dag_ctx->get_process_type()
      || !task_plan->is_valid_task_idx(task_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVFDDL task argument",
        K(ret), KP(ivf_ddl_dag_ctx), KP(task_plan), K(task_idx));
  } else {
    ivf_ddl_dag_ctx_ = ivf_ddl_dag_ctx;
    task_plan_ = task_plan;
    task_idx_ = task_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFDDLTask::process()
{
  int ret = OB_SUCCESS;
  ObIVFBuildMonitorInfo *info = static_cast<ObIVFBuildMonitorInfo *>(monitor_info_);
  const int64_t begin_us = ObTimeUtility::current_time();
  if (nullptr != info) { info->begin_task(); }
  if (nullptr != ivf_ddl_dag_ctx_) {
    if (nullptr != ivf_ddl_dag_ctx_->step_monitor_) { ivf_ddl_dag_ctx_->step_monitor_->task_started(); }
    if (nullptr != ivf_ddl_dag_ctx_->build_monitor_) { ivf_ddl_dag_ctx_->build_monitor_->task_started(); }
  }
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_ddl_dag_ctx_) || OB_ISNULL(task_plan_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL task is not initialized",
        K(ret), KP_(ivf_ddl_dag_ctx), KP_(task_plan), K_(task_idx));
  } else if (OB_FAIL(ivf_ddl_dag_ctx_->process_task(*task_plan_, task_idx_))) {
    LOG_WARN("fail to process IVFDDL task", K(ret), KPC_(task_plan), K_(task_idx));
  }
  const int64_t elapsed_us = MAX(0, ObTimeUtility::current_time() - begin_us);
  ObIVFMergeWorkPerf work;
  if (nullptr != info) { work = info->get_work(); }
  if (nullptr != ivf_ddl_dag_ctx_ && nullptr != ivf_ddl_dag_ctx_->full_build_processor_) {
    ObIVFMergeWorkPerf actual;
    if (OB_SUCCESS == ivf_ddl_dag_ctx_->full_build_processor_->get_work_perf(task_idx_, actual)) {
      work = actual;
    }
  }
  if (nullptr != info) { info->end_task(ret, elapsed_us, &work); }
  if (nullptr != ivf_ddl_dag_ctx_) {
    if (nullptr != ivf_ddl_dag_ctx_->step_monitor_) {
      ivf_ddl_dag_ctx_->step_monitor_->add_task(ret, elapsed_us, work);
    }
    if (nullptr != ivf_ddl_dag_ctx_->build_monitor_) {
      ivf_ddl_dag_ctx_->build_monitor_->add_task(ret, elapsed_us, work);
    }
  }
  return ret;
}

int ObIVFDDLTask::post_generate_next_task()
{
  // The framework copies the finish dependency before retiring this node.
  // Suspended attempts do not enter this hook, so they retain their task_idx.
  const int ret = ObITask::post_generate_next_task();
  if (OB_SUCCESS != ret && nullptr != ivf_ddl_dag_ctx_) {
    ivf_ddl_dag_ctx_->cancel_task_dispatch(ret);
  }
  return ret;
}

int ObIVFDDLTask::post_generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_ddl_dag_ctx_) || OB_ISNULL(task_plan_))) {
    ret = OB_NOT_INIT;
  } else {
    ret = ivf_ddl_dag_ctx_->generate_next_task(*task_plan_, next_task);
  }
  return ret;
}

int ObIVFDDLTask::inner_add_monitor_info(ObDDLDagMonitorNode &node)
{
  ObIVFBuildMonitorInfo *info = nullptr;
  const int ret = node.alloc_monitor_info(this, info);
  if (OB_SUCCESS == ret && nullptr != info) {
    ivf_ddl_dag_ctx_->init_task_perf(*info, task_idx_);
    monitor_info_ = info;
  } else {
    LOG_WARN("failed to allocate IVF task monitoring", K(ret));
  }
  // Observability allocation failure must not fail the build.
  return OB_SUCCESS;
}

ObIVFDDLDagCtx::ObIVFDDLDagCtx()
  : is_inited_(false),
    runtime_initialized_(false),
    build_finished_(false),
    owner_dag_(nullptr),
    process_type_(ObIVFProcessType::INVALID),
    tablet_id_(),
    runtime_arena_("IVFDDLRuntime", OB_MALLOC_NORMAL_BLOCK_SIZE,
        MTL_ID(), ObCtxIds::VECTOR_CTX_ID),
    runtime_allocator_(runtime_arena_),
    schema_ctx_(nullptr),
    input_adapter_(nullptr),
    hca_algorithm_(nullptr),
    center_norm_info_(nullptr),
    hgraph_allocator_(nullptr),
    sstable_writer_(nullptr),
    planner_(nullptr),
    dispatch_task_plan_(nullptr),
    next_task_idx_(0),
    dispatch_ret_(OB_SUCCESS),
    full_build_processor_(nullptr),
    full_build_planner_(nullptr),
    step_monitor_(nullptr),
    build_monitor_(nullptr),
    current_step_perf_(),
    build_start_us_(0),
    finished_steps_(0),
    build_perf_finished_(false)
{
}

ObIVFDDLDagCtx::~ObIVFDDLDagCtx()
{
  // All borrowed task users have drained before the owning tablet context dies.
  if (build_start_us_ > 0 && !build_perf_finished_) {
    const int ret_code = nullptr != owner_dag_ && OB_SUCCESS != owner_dag_->get_dag_ret()
        ? owner_dag_->get_dag_ret() : OB_CANCELED;
    finish_build_perf(ret_code);
  }
  destroy_runtime_();
}

int ObIVFDDLDagCtx::init(
    ObColumnClusteredDag &owner_dag,
    const ObIVFProcessType process_type,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVFDDL dag ctx init twice", K(ret));
  } else if (OB_UNLIKELY(!owner_dag.is_inited_
      || !tablet_id.is_valid()
      || (ObIVFProcessType::INITIAL_BUILD != process_type
          && ObIVFProcessType::REFRESH != process_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVFDDL dag ctx argument",
        K(ret), K(static_cast<int64_t>(process_type)), K(tablet_id),
        K(owner_dag));
  } else if (ObIVFProcessType::REFRESH == process_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVFDDL refresh planner is not wired",
        K(ret), K(static_cast<int64_t>(process_type)));
  } else {
    owner_dag_ = &owner_dag;
    process_type_ = process_type;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFDDLDagCtx::generate_initial_tasks(
    common::ObIArray<share::ObITask *> &need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  ObIVFDDLPrepareTask *prepare_task = nullptr;
  need_schedule_tasks.reset();
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (OB_FAIL(owner_dag_->alloc_task(prepare_task))) {
    LOG_WARN("fail to allocate IVFDDL prepare task", K(ret));
  } else if (OB_FAIL(prepare_task->init(this))) {
    LOG_WARN("fail to init IVFDDL prepare task", K(ret));
  } else if (OB_FAIL(need_schedule_tasks.push_back(prepare_task))) {
    LOG_WARN("fail to save IVFDDL prepare task", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(prepare_task)) {
    prepare_task->reset_node();
    (void)owner_dag_->remove_task(*prepare_task);
  }
  return ret;
}

int ObIVFDDLDagCtx::prepare_build_param_(ObIVFBuildParam &build_param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_schema = nullptr;
  ObSEArray<ObTabletID, 16> index_tablets;
  int64_t tablet_pos = -1;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t index_table_id =
      owner_dag_->get_ddl_table_schema().table_id_;
  const int64_t schema_version =
      owner_dag_->get_ddl_task_param().schema_version_;
  if (!is_owner_inited() || !tablet_id_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DDL dag context is not initialized", K(ret),
        K(tablet_id_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
      .get_tenant_schema_guard(tenant_id, schema_guard, schema_version))) {
    LOG_WARN("failed to get IVF DDL schema guard", K(ret),
        K(tenant_id), K(schema_version));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id, index_table_id, index_schema))) {
    LOG_WARN("failed to get IVF DDL index schema", K(ret),
        K(index_table_id));
  } else if (OB_ISNULL(index_schema)
      || !index_schema->is_async_ivf_index()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected IVF DDL index schema", K(ret),
        K(index_table_id), KP(index_schema));
  } else if (OB_FAIL(index_schema->get_tablet_ids(index_tablets))) {
    LOG_WARN("failed to get IVF DDL index tablets", K(ret),
        K(index_table_id));
  } else {
    for (int64_t i = 0; i < index_tablets.count() && tablet_pos < 0; ++i) {
      if (index_tablets.at(i) == tablet_id_) {
        tablet_pos = i;
      }
    }
    if (tablet_pos < 0) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("IVF DDL tablet is absent from index schema", K(ret),
          K(tablet_id_), K(index_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(
        tenant_id, index_schema->get_data_table_id(), data_schema))) {
      LOG_WARN("failed to get IVF DDL data schema", K(ret),
          "data_table_id", index_schema->get_data_table_id());
    } else if (OB_ISNULL(data_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("IVF DDL data schema does not exist", K(ret),
          KPC(index_schema));
    } else {
      build_param.tenant_id_ = tenant_id;
      build_param.data_table_id_ = data_schema->get_table_id();
      build_param.index_table_id_ = index_table_id;
      build_param.index_tablet_id_ = tablet_id_;
      build_param.schema_version_ = schema_version;
    }
  }
  return ret;
}

int ObIVFDDLDagCtx::init_full_build_runtime_()
{
  int ret = OB_SUCCESS;
  bool runtime_init_started = false;
  ObDDLTabletContext *tablet_context = nullptr;
  ObPluginVectorIndexService *vector_index_service = nullptr;
  ObIVFBuildParam build_param;
  ObWriteMacroParam write_param;
  ObIVFHcaAlgorithmParam algorithm_param;
  if (!is_inited_ || !is_owner_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF DDL dag context is not initialized", K(ret),
        K(tablet_id_));
  } else if (build_finished_ || runtime_initialized_ || OB_NOT_NULL(schema_ctx_)
      || OB_NOT_NULL(input_adapter_)
      || OB_NOT_NULL(sstable_writer_) || OB_NOT_NULL(hca_algorithm_)
      || OB_NOT_NULL(full_build_processor_)
      || OB_NOT_NULL(full_build_planner_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DDL runtime init twice", K(ret), K(tablet_id_));
  } else if (OB_FAIL(owner_dag_->get_tablet_context(
      tablet_id_, tablet_context))) {
    LOG_WARN("failed to get IVF DDL tablet context", K(ret),
        K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)
      || OB_ISNULL(tablet_context->ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL tablet context is invalid", K(ret),
        K(tablet_id_), KP(tablet_context));
  } else if (OB_FAIL(prepare_build_param_(build_param))) {
    LOG_WARN("failed to prepare IVF DDL build parameter", K(ret),
        K(tablet_id_));
  } else if (FALSE_IT(runtime_init_started = true)) {
  } else if (OB_FAIL(alloc_ddl_runtime_object(
      runtime_allocator_, schema_ctx_, runtime_allocator_))) {
    LOG_WARN("failed to allocate IVF DDL schema context", K(ret),
        K(tablet_id_));
  } else if (OB_FAIL(schema_ctx_->init(build_param))) {
    LOG_WARN("failed to initialize IVF DDL schema context", K(ret),
        K(build_param));
  } else {
    const ObVectorIndexParam &vector_param = schema_ctx_->get_vector_param();
    if (vector_param.type_ != VIAT_DISK_BBQ
        || vector_param.cluster_size_
            < ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE
        || vector_param.cluster_size_
            > ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE
        || vector_param.dist_algorithm_ < VIDA_L2
        || vector_param.dist_algorithm_ >= VIDA_MAX
        || schema_ctx_->get_vector_dim()
            != tablet_context->ivf_ctx_->get_rotation_dim()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("invalid IVF DDL INITIAL_BUILD schema parameter", K(ret),
          K(vector_param), "schema_dim", schema_ctx_->get_vector_dim(),
          "rotation_dim", tablet_context->ivf_ctx_->get_rotation_dim());
    } else if (OB_FAIL(alloc_ddl_runtime_object(
        runtime_allocator_, input_adapter_, runtime_allocator_))) {
      LOG_WARN("failed to allocate IVF DDL input adapter", K(ret),
          K(tablet_id_));
    } else if (OB_FAIL(input_adapter_->init(
        *tablet_context, vector_param.cluster_size_,
        vector_param.dist_algorithm_))) {
      LOG_WARN("failed to initialize IVF DDL input adapter", K(ret),
          K(tablet_id_), K(vector_param));
    } else {
      // Capture has crossed its tablet barrier. PX autoinc offsets retain
      // their input-slice meaning; subsequent DDL writer/merge callbacks use
      // this output-slot count for redo and the final index macro sequence.
      // All physical writers are created by their output tasks; unused slots
      // allocate no buffers/stores and completed tasks retain no writers.
      tablet_context->slice_count_ = input_adapter_->is_empty()
          ? 1 : ObIVFMergeProcessor::DDL_OUTPUT_SLICE_COUNT;
      blocksstable::ObIvfSSTableMeta initial_meta;
      if (input_adapter_->is_empty()) {
        initial_meta.set_empty();
      } else {
        initial_meta.index_type_ = VIAT_DISK_BBQ;
        initial_meta.dist_type_ = vector_param.dist_algorithm_;
        initial_meta.dim_ = static_cast<int32_t>(
            schema_ctx_->get_vector_dim());
        initial_meta.format_version_ =
            blocksstable::IVF_SSTABLE_FORMAT_VERSION;
        initial_meta.nbits_ = 1;
        initial_meta.fht_algo_version_ =
            quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
      }
      if (!initial_meta.is_valid()) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("invalid initial IVF DDL SSTable metadata", K(ret),
            K(initial_meta), K(vector_param));
      } else if (OB_FAIL(tablet_context->ivf_ctx_->ivf_meta_.assign(
          tablet_context->arena_, initial_meta))) {
        LOG_WARN("failed to install initial IVF DDL metadata", K(ret),
            K(tablet_id_), K(initial_meta));
      } else if (input_adapter_->is_empty()) {
        // Empty output has no later write task to own this writer.
        ObAsyncIndexMacroWriterAdapter writer;
        if (OB_FAIL(ObDDLUtil::fill_writer_param(
            tablet_id_, 0 /* slice_idx */, -1 /* cg_idx */, owner_dag_, write_param))) {
          LOG_WARN("failed to fill empty IVF DDL writer parameter", K(ret), K(tablet_id_));
        } else if (OB_FAIL(writer.init(write_param))) {
          LOG_WARN("failed to initialize empty IVF DDL macro writer", K(ret), K(tablet_id_));
        } else if (OB_FAIL(writer.close())) {
          LOG_WARN("failed to close empty IVF DDL macro writer", K(ret),
              K(tablet_id_));
        } else {
          tablet_context->ivf_ctx_->is_built_ = true;
          runtime_initialized_ = true;
          LOG_INFO("initialized empty IVF DDL INITIAL_BUILD output",
              K(tablet_id_));
        }
      } else if (OB_FAIL(alloc_ddl_runtime_object(
          runtime_allocator_, sstable_writer_, runtime_allocator_))) {
        LOG_WARN("failed to allocate IVF DDL SSTable writer", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(sstable_writer_->init(
          *this, owner_dag_->get_ddl_task_param().snapshot_version_,
          schema_ctx_->get_vector_dim(), vector_param.dist_algorithm_,
          true /* input_vectors_are_rotated */))) {
        LOG_WARN("failed to initialize IVF DDL SSTable writer", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(tablet_context->ivf_ctx_->ivf_meta_.assign(
          tablet_context->arena_, sstable_writer_->get_ivf_meta()))) {
        LOG_WARN("failed to install IVF DDL writer metadata", K(ret),
            K(tablet_id_), "ivf_meta", sstable_writer_->get_ivf_meta());
      } else if (OB_ISNULL(vector_index_service =
          MTL(ObPluginVectorIndexService *))
          || OB_ISNULL(vector_index_service->get_all_vsag_use_mem())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF DDL vector index service is unavailable", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(alloc_ddl_runtime_object(
          runtime_allocator_, hgraph_allocator_,
          vector_index_service->get_all_vsag_use_mem()))) {
        LOG_WARN("failed to allocate IVF DDL HGraph allocator", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(hgraph_allocator_->init(
          vector_index_service->get_memory_context(),
          vector_index_service->get_all_vsag_use_mem(), MTL_ID(),
          "IVFDDLHGraph"))) {
        LOG_WARN("failed to initialize IVF DDL HGraph allocator", K(ret),
            K(tablet_id_));
      } else if (VIDA_COS == vector_param.dist_algorithm_
          && OB_FAIL(alloc_ddl_runtime_object(
              runtime_allocator_, center_norm_info_))) {
        LOG_WARN("failed to allocate IVF DDL normalization info", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(alloc_ddl_runtime_object(
          runtime_allocator_, hca_algorithm_))) {
        LOG_WARN("failed to allocate IVF DDL HCA adapter", K(ret),
            K(tablet_id_));
      } else if (FALSE_IT(
          algorithm_param.center_norm_info_ = center_norm_info_)) {
      } else if (OB_FAIL(hca_algorithm_->init(
          algorithm_param, hgraph_allocator_))) {
        LOG_WARN("failed to initialize IVF DDL HCA adapter", K(ret),
            K(tablet_id_));
      } else if (OB_FAIL(alloc_ddl_runtime_object(
          runtime_allocator_, full_build_processor_))) {
        LOG_WARN("failed to allocate IVF DDL INITIAL_BUILD processor",
            K(ret), K(tablet_id_));
      } else if (OB_FAIL(full_build_processor_->init(
          runtime_allocator_, input_adapter_->get_full_build_input(),
          *hca_algorithm_, *sstable_writer_, process_type_))) {
        LOG_WARN("failed to initialize IVF DDL INITIAL_BUILD processor",
            K(ret), K(tablet_id_));
      } else if (OB_FAIL(full_build_processor_->configure_ddl_runtime(*hca_algorithm_,
          MAX(1L, owner_dag_->get_ddl_thread_count())))) {
        LOG_WARN("failed to configure IVF DDL source execution", K(ret));
      } else if (OB_FAIL(alloc_ddl_runtime_object(
          runtime_allocator_, full_build_planner_))) {
        LOG_WARN("failed to allocate IVF DDL INITIAL_BUILD planner",
            K(ret), K(tablet_id_));
      } else if (OB_FAIL(full_build_planner_->init(
          runtime_allocator_, *full_build_processor_, process_type_))) {
        LOG_WARN("failed to initialize IVF DDL INITIAL_BUILD planner",
            K(ret), K(tablet_id_));
      } else {
        full_build_planner_->set_perf_observer(this);
        planner_ = full_build_planner_;
        runtime_initialized_ = true;
        LOG_INFO("initialized IVF DDL INITIAL_BUILD runtime", K(tablet_id_),
            "vector_count", input_adapter_->get_vector_count(),
            K(vector_param.cluster_size_));
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != input_adapter_) {
    // Keep captured VID metadata until every runtime initializer succeeds;
    // a failed initialization must leave the sealed inputs reusable.
    input_adapter_->release_capture_metadata();
    if (nullptr != build_monitor_) {
      build_monitor_->set_input_vectors(input_adapter_->get_vector_count());
    }
  }
  if (OB_FAIL(ret) && runtime_init_started) {
    destroy_runtime_();
  }
  return ret;
}

int ObIVFDDLDagCtx::create_writer_(
    const int64_t slice_idx, ObITabletSliceWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  ObWriteMacroParam param;
  ObAsyncIndexMacroWriterAdapter *macro_writer = nullptr;
  if (!runtime_initialized_ || slice_idx < 0
      || slice_idx >= ObIVFMergeProcessor::DDL_OUTPUT_SLICE_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL writer request", K(ret), K(slice_idx));
  } else if (OB_FAIL(ObDDLUtil::fill_writer_param(
      tablet_id_, slice_idx, -1 /* cg_idx */, owner_dag_, param))) {
    LOG_WARN("failed to fill IVF DDL writer parameter", K(ret), K(slice_idx));
  } else if (param.slice_count_ != ObIVFMergeProcessor::DDL_OUTPUT_SLICE_COUNT) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF DDL output slice count changed", K(ret), K(param.slice_count_));
  } else if (OB_ISNULL(macro_writer = OB_NEW(
      ObAsyncIndexMacroWriterAdapter, ObMemAttr(MTL_ID(), "IVFDDLWriter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF DDL writer", K(ret), K(slice_idx));
  } else if (OB_FAIL(macro_writer->init(param))) {
    LOG_WARN("failed to initialize IVF DDL writer", K(ret), K(slice_idx));
  } else if (slice_idx > 0 && slice_idx <= ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT
      && OB_FAIL(macro_writer->set_ivf_posting_counts(sstable_writer_->get_leaf_posting_counts(),
          sstable_writer_->get_ivf_meta().leaf_cid_base()))) {
    LOG_WARN("failed to set IVF DDL posting counts", K(ret), K(slice_idx));
  } else {
    writer = macro_writer;
    macro_writer = nullptr;
  }
  if (nullptr != macro_writer) {
    OB_DELETE(ObAsyncIndexMacroWriterAdapter, ObMemAttr(MTL_ID(), "IVFDDLWriter"), macro_writer);
  }
  return ret;
}

int ObIVFDDLDagCtx::create_center_writer(ObITabletSliceWriter *&writer)
{
  return create_writer_(0 /* slice_idx */, writer);
}

int ObIVFDDLDagCtx::create_raw_writer(
    const int64_t task_idx, ObITabletSliceWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (task_idx < 0 || task_idx >= ObIVFMergeProcessor::MAX_RAW_TASK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = create_writer_(task_idx + 1 + ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT, writer);
  }
  return ret;
}

int ObIVFDDLDagCtx::create_posting_writer(
    const int64_t task_idx, const int64_t cid_begin, const int64_t cid_end, ObITabletSliceWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (!runtime_initialized_ || cid_begin < 0 || cid_end <= cid_begin || task_idx < 0
      || task_idx >= ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = create_writer_(task_idx + 1, writer);
  }
  return ret;
}

void ObIVFDDLDagCtx::destroy_writer(ObITabletSliceWriter *writer)
{
  // Output tasks close and validate their private writer before returning it.
  // Macro metadata belongs to the tablet's store manager, not this writer.
  ObAsyncIndexMacroWriterAdapter *macro_writer = static_cast<ObAsyncIndexMacroWriterAdapter *>(writer);
  OB_DELETE(ObAsyncIndexMacroWriterAdapter, ObMemAttr(MTL_ID(), "IVFDDLWriter"), macro_writer);
}

void ObIVFDDLDagCtx::destroy_runtime_()
{
  planner_ = nullptr;
  dispatch_task_plan_ = nullptr;
  if (OB_NOT_NULL(full_build_planner_)) {
    full_build_planner_->~ObIVFFullBuildPlanner();
    full_build_planner_ = nullptr;
  }
  if (OB_NOT_NULL(full_build_processor_)) {
    full_build_processor_->~ObIVFMergeProcessor();
    full_build_processor_ = nullptr;
  }
  if (OB_NOT_NULL(hca_algorithm_)) {
    hca_algorithm_->cancel(OB_CANCELED);
    hca_algorithm_->~ObIVFDDLHcaAdapter();
    hca_algorithm_ = nullptr;
  }
  if (OB_NOT_NULL(hgraph_allocator_)) {
    hgraph_allocator_->~ObVsagMemContext();
    hgraph_allocator_ = nullptr;
  }
  if (OB_NOT_NULL(sstable_writer_)) {
    sstable_writer_->~ObIVFDDLSSTableWriter();
    sstable_writer_ = nullptr;
  }
  if (OB_NOT_NULL(input_adapter_)) {
    input_adapter_->~ObIVFDDLInput();
    input_adapter_ = nullptr;
  }
  if (OB_NOT_NULL(center_norm_info_)) {
    center_norm_info_->~ObVectorNormalizeInfo();
    center_norm_info_ = nullptr;
  }
  if (OB_NOT_NULL(schema_ctx_)) {
    schema_ctx_->~ObIVFBuildSchemaCtx();
    schema_ctx_ = nullptr;
  }
  runtime_initialized_ = false;
  runtime_allocator_.clear();
  runtime_arena_.reset();
}

int ObIVFDDLDagCtx::release_build_resources()
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  if (!is_owner_inited() || !build_finished_ || !build_perf_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF build has not completed its publication barrier", K(ret), K_(tablet_id));
  } else if (!runtime_initialized_) {
    // Already released by the terminal task.
  } else if (OB_FAIL(owner_dag_->get_tablet_context(tablet_id_, tablet_context))) {
    LOG_WARN("failed to get completed IVF tablet context", K(ret), K_(tablet_id));
  } else if (nullptr == tablet_context || nullptr == tablet_context->ivf_ctx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing completed IVF tablet context", K(ret), K_(tablet_id));
  } else {
    // Retire readers before their stores, and stores before returning quota.
    // IVF metadata and macro-meta stores remain owned by the tablet for merge.
    destroy_runtime_();
    tablet_context->ivf_ctx_->release_capture_inputs();
  }
  return ret;
}

int ObIVFDDLDagCtx::start(share::ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  const ObIVFTaskPlan *first_task_plan = nullptr;
  build_start_us_ = ObTimeUtility::current_time();
  if (nullptr != owner_dag_ && nullptr != owner_dag_->get_monitor_node()) {
    ObDDLDagMonitorNode *node = owner_dag_->get_monitor_node();
    if (OB_SUCCESS == node->alloc_monitor_info(nullptr, build_monitor_) && nullptr != build_monitor_) {
      ObIVFMergeStepPerf perf;
      perf.start_us_ = build_start_us_;
      build_monitor_->init("IVF_BUILD", owner_dag_->get_ddl_task_param(), tablet_id_.id(), perf);
    }
  }
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (FALSE_IT(DEBUG_SYNC(BEFORE_ASYNC_IVF_INITIAL_BUILD))) {
  } else if (OB_FAIL(init_full_build_runtime_())) {
    LOG_WARN("fail to initialize IVFDDL runtime", K(ret), K(tablet_id_));
  } else if (OB_FAIL(OB_E(EN_ASYNC_IVF_INITIAL_BUILD_FAIL) OB_SUCCESS)) {
    LOG_WARN("injected async IVF initial build failure after runtime initialization", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(input_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVFDDL input adapter is null", K(ret), K(tablet_id_));
  } else if (input_adapter_->is_empty()) {
    if (OB_FAIL(materialize_physical_finish(prepare_task))) {
      LOG_WARN("fail to materialize empty IVFDDL output", K(ret),
          K(tablet_id_));
    }
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVFDDL process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)), K(tablet_id_));
  } else if (!planner_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL planner is not initialized", K(ret), K(tablet_id_));
  } else if (OB_FAIL(planner_->start(first_task_plan))) {
    LOG_WARN("fail to start IVFDDL planner", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(first_task_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVFDDL planner returned null first task plan", K(ret),
        K(tablet_id_));
  } else if (OB_FAIL(materialize_task_plan(prepare_task, *first_task_plan))) {
    LOG_WARN("fail to materialize first IVFDDL task plan", K(ret),
        K(tablet_id_), KPC(first_task_plan));
    planner_->cancel(ret);
  }
  if (OB_FAIL(ret) || (nullptr != input_adapter_ && input_adapter_->is_empty())) {
    finish_build_perf(ret);
  }
  return ret;
}

void ObIVFDDLDagCtx::on_step_perf(const ObIVFMergeStepPerf &perf)
{
  const bool is_new = current_step_perf_.plan_id_ != perf.plan_id_;
  current_step_perf_ = perf;
  if (is_new) {
    step_monitor_ = nullptr;
    ObDDLDagMonitorNode *node = owner_dag_->get_monitor_node();
    if (nullptr != node && OB_SUCCESS == node->alloc_monitor_info(nullptr, step_monitor_)
        && nullptr != step_monitor_) {
      step_monitor_->init("IVF_STEP", owner_dag_->get_ddl_task_param(), tablet_id_.id(), perf);
    }
    LOG_INFO("[IVF_BUILD_PERF]", "event", "step_start", K_(tablet_id),
        "ddl_task_id", owner_dag_->get_ddl_task_param().ddl_task_id_,
        "stage", ivf_merge_stage_name(perf.stage_),
        "step", ivf_merge_step_name(perf), "depth", perf.depth_, "plan_id", perf.plan_id_);
  }
  if (nullptr != step_monitor_) {
    step_monitor_->update_step(perf, perf.end_us_ > 0);
  } else if (perf.end_us_ > 0) {
    LOG_INFO("[IVF_BUILD_PERF]", "event", "step_finish", K_(tablet_id),
        "stage", ivf_merge_stage_name(perf.stage_), "step", ivf_merge_step_name(perf),
        "depth", perf.depth_, "plan_id", perf.plan_id_, "total_us", MAX(0, perf.end_us_ - perf.start_us_),
        "ret_code", perf.ret_code_, "monitor_missing", true);
  }
  if (perf.end_us_ > 0) {
    if (OB_SUCCESS == perf.ret_code_) {
      ++finished_steps_;
      if (nullptr != build_monitor_) { build_monitor_->step_completed(); }
    }
    step_monitor_ = nullptr; // finished records can be reclaimed immediately
  }
}

void ObIVFDDLDagCtx::init_task_perf(ObIVFBuildMonitorInfo &info, const int64_t task_idx)
{
  ObIVFMergeWorkPerf work;
  if (nullptr != full_build_processor_) {
    const int ret = full_build_processor_->get_work_perf(task_idx, work);
    if (OB_SUCCESS != ret) { LOG_WARN("failed to describe IVF work item", K(ret), K(task_idx)); }
  }
  info.init("IVF_TASK", owner_dag_->get_ddl_task_param(), tablet_id_.id(),
      current_step_perf_, task_idx, work);
}

void ObIVFDDLDagCtx::finish_build_perf(const int ret_code)
{
  if (!build_perf_finished_) {
    const int64_t end_us = ObTimeUtility::current_time();
    if (current_step_perf_.start_us_ > 0 && current_step_perf_.end_us_ == 0) {
      current_step_perf_.end_us_ = end_us;
      current_step_perf_.ret_code_ = ret_code;
      on_step_perf(current_step_perf_);
    }
    if (nullptr != build_monitor_) {
      build_monitor_->finish_build(end_us, finished_steps_, ret_code);
      build_monitor_ = nullptr;
    } else {
      LOG_INFO("[IVF_BUILD_PERF]", "event", "build_finish", "scope", "ivf_initial_build_runtime",
          K_(tablet_id), "total_us", MAX(0, end_us - build_start_us_), K_(finished_steps), K(ret_code),
          "monitor_missing", true);
    }
    build_perf_finished_ = true;
  }
}

bool ObIVFDDLDagCtx::is_owner_inited() const
{
  return OB_NOT_NULL(owner_dag_) && owner_dag_->is_inited_;
}

common::ObIAllocator &ObIVFDDLDagCtx::get_allocator()
{
  return runtime_allocator_;
}

int ObIVFDDLDagCtx::materialize_task_plan(
    share::ObITask &parent_task,
    const ObIVFTaskPlan &task_plan)
{
  int ret = OB_SUCCESS;
  bool graph_published = false;
  int64_t worker_count = 0;
  ObIVFDDLFinishTask *finish_task = nullptr;
  ObArray<ObITask *> task_plan_tasks;
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVFDDL task plan", K(ret), K(task_plan));
  } else if (OB_FAIL(check_task_dispatch_status())) {
    LOG_WARN("IVFDDL task dispatch stopped", K(ret), K(task_plan));
  } else {
    // DDL and PX workers share the parallelism allocated to this build.
    // PX helpers do not grant another budget. Successful tasks replenish
    // these lanes instead of materializing the complete logical plan.
    worker_count = MIN(task_plan.get_task_count(),
        MAX(1L, owner_dag_->get_ddl_thread_count()));
    dispatch_task_plan_ = &task_plan;
    ATOMIC_STORE(&next_task_idx_, worker_count);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(owner_dag_->alloc_task(finish_task))) {
    LOG_WARN("fail to allocate IVFDDL finish task", K(ret), K(task_plan));
  } else if (OB_FAIL(finish_task->init(this, &task_plan))) {
    LOG_WARN("fail to init IVFDDL finish task", K(ret), K(task_plan));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < worker_count; ++i) {
    ObIVFDDLTask *task = nullptr;
    if (OB_FAIL(check_task_dispatch_status())) {
      LOG_WARN("IVFDDL task dispatch stopped", K(ret), K(task_plan), K(i));
    } else if (OB_FAIL(owner_dag_->alloc_task(task))) {
      LOG_WARN("fail to allocate IVFDDL task", K(ret), K(task_plan), K(i));
    } else if (OB_FAIL(task->init(this, &task_plan, i))) {
      LOG_WARN("fail to init IVFDDL task", K(ret), K(task_plan), K(i));
    } else if (OB_FAIL(parent_task.add_child(*task))) {
      LOG_WARN("fail to connect IVFDDL task plan parent", K(ret), K(task_plan), K(i));
    } else if (OB_FAIL(task->add_child(*finish_task))) {
      LOG_WARN("fail to connect IVFDDL finish task", K(ret), K(task_plan), K(i));
    } else if (OB_FAIL(task_plan_tasks.push_back(task))) {
      LOG_WARN("fail to save IVFDDL task", K(ret), K(task_plan), K(i));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
      task->reset_node();
      (void)owner_dag_->remove_task(*task);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(owner_dag_->add_task(*finish_task))) {
    LOG_WARN("fail to publish IVFDDL finish task", K(ret), K(task_plan));
  } else if (FALSE_IT(graph_published = true)) {
  } else if (OB_FAIL(owner_dag_->batch_add_task(task_plan_tasks))) {
    LOG_WARN("fail to publish IVFDDL task plan", K(ret), K(task_plan));
  }
  if (OB_FAIL(ret) && !graph_published) {
    for (int64_t i = 0; i < task_plan_tasks.count(); ++i) {
      ObITask *task = task_plan_tasks.at(i);
      if (OB_NOT_NULL(task)) {
        task->reset_node();
        (void)owner_dag_->remove_task(*task);
      }
    }
    if (OB_NOT_NULL(finish_task)) {
      finish_task->reset_node();
      (void)owner_dag_->remove_task(*finish_task);
    }
  }
  return ret;
}

int ObIVFDDLDagCtx::check_task_dispatch_status() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !is_owner_inited()) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = ATOMIC_LOAD(&dispatch_ret_))) {
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
  } else {
    // Observe externally stopped/failed DAGs under their lock. No planner or
    // worker operation runs inside this critical section.
    lib::ObMutexGuard guard(owner_dag_->lock_);
    if (OB_SUCCESS != (ret = ATOMIC_LOAD(&owner_dag_->dag_ret_))) {
    } else if (owner_dag_->has_set_stop()) {
      ret = OB_CANCELED;
    }
  }
  return ret;
}

void ObIVFDDLDagCtx::cancel_task_dispatch(const int ret_code)
{
  if (OB_SUCCESS != ret_code && OB_ITER_END != ret_code
      && OB_DAG_TASK_IS_SUSPENDED != ret_code
      && ATOMIC_BCAS(&dispatch_ret_, OB_SUCCESS, ret_code)) {
    if (nullptr != planner_) {
      planner_->cancel(ret_code);
    }
    if (nullptr != owner_dag_) {
      owner_dag_->simply_set_stop(ret_code);
    }
  }
}

int ObIVFDDLDagCtx::generate_next_task(
    const ObIVFTaskPlan &task_plan, share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = -1;
  ObIVFDDLTask *task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(dispatch_task_plan_ != &task_plan || !task_plan.is_valid())) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    while (OB_SUCC(ret) && task_idx < 0) {
      const int64_t candidate = ATOMIC_LOAD(&next_task_idx_);
      if (OB_FAIL(check_task_dispatch_status())) {
      } else if (candidate >= task_plan.get_task_count()) {
        ret = OB_ITER_END;
      } else if (ATOMIC_BCAS(&next_task_idx_, candidate, candidate + 1)) {
        task_idx = candidate;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(owner_dag_->alloc_task(task))) {
    LOG_WARN("failed to allocate next IVFDDL task", K(ret), K(task_idx));
  } else if (OB_FAIL(task->init(this, &task_plan, task_idx))) {
    LOG_WARN("failed to initialize next IVFDDL task", K(ret), K(task_idx));
  } else {
    next_task = task;
    task = nullptr;
  }
  if (nullptr != task) {
    task->reset_node();
    (void)owner_dag_->remove_task(*task);
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    cancel_task_dispatch(ret);
  }
  return ret;
}

int ObIVFDDLDagCtx::process_task(
    const ObIVFTaskPlan &task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_
      || !task_plan.is_valid_task_idx(task_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVFDDL task", K(ret), K(task_plan), K(task_idx));
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVFDDL process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)));
  } else if (OB_FAIL(check_task_dispatch_status())) {
    LOG_WARN("IVFDDL task dispatch stopped", K(ret), K(task_idx));
  } else if (OB_FAIL(planner_->process_task(task_plan, task_idx))) {
    if (OB_EAGAIN == ret) {
      ret = OB_DAG_TASK_IS_SUSPENDED;
    } else {
      LOG_WARN("fail to execute and publish IVFDDL task", K(ret), K(task_plan), K(task_idx));
    }
  }
  if (OB_SUCCESS != ret && OB_DAG_TASK_IS_SUSPENDED != ret) {
    cancel_task_dispatch(ret);
  }
  return ret;
}

int ObIVFDDLDagCtx::finish_task_plan(
    share::ObITask &finish_task,
    const ObIVFTaskPlan &task_plan)
{
  int ret = OB_SUCCESS;
  bool build_finished = false;
  const bool root_cluster_finished = current_step_perf_.stage_ == ObIVFMergeStage::HIERARCHY
      && current_step_perf_.step_ == ObIVFMergeStep::HIERARCHY_CLUSTER
      && current_step_perf_.depth_ == 0;
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid finished IVFDDL task plan", K(ret), K(task_plan));
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVFDDL process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)));
  } else {
    const ObIVFTaskPlan *next_task_plan = nullptr;
    if (OB_FAIL(check_task_dispatch_status())) {
      cancel_task_dispatch(ret);
      LOG_WARN("IVFDDL task dispatch stopped before phase finish", K(ret));
    } else if (OB_FAIL(planner_->finish_task_plan(task_plan, next_task_plan))) {
      LOG_WARN("fail to finish IVFDDL task plan", K(ret), K(task_plan));
    } else if (OB_NOT_NULL(next_task_plan)) {
      if (OB_FAIL(materialize_task_plan(finish_task, *next_task_plan))) {
        LOG_WARN("fail to materialize next IVFDDL task plan",
            K(ret), KPC(next_task_plan));
        planner_->cancel(ret);
      }
    } else {
      build_finished = true;
      // TODO: Fetch and pass the planner artifact after its physical consumer
      // and ownership contract are finalized. The framework currently uses
      // planner completion only as the handoff boundary.
      if (OB_FAIL(materialize_physical_finish(finish_task))) {
        LOG_WARN("fail to materialize final IVFDDL graph", K(ret));
        planner_->cancel(ret);
      }
    }
  }
  if (OB_FAIL(ret) || build_finished) { finish_build_perf(ret); }
  if (OB_SUCC(ret) && root_cluster_finished && nullptr != input_adapter_) {
    // INITIAL_BUILD never returns to root clustering. Later hierarchy levels
    // and RAW_WRITE need replay payloads, but not the captured root sample set.
    input_adapter_->release_first_level_samples();
  }
  return ret;
}

int ObIVFDDLDagCtx::materialize_physical_finish(
    share::ObITask &parent_task)
{
  int ret = OB_SUCCESS;
  bool graph_published = false;
  ObDDLTabletContext *tablet_context = nullptr;
  ObITask *data_merge_task = nullptr;
  ObITask *lob_meta_merge_task = nullptr;
  ObITask *lob_piece_merge_task = nullptr;
  ObArray<ObITask *> data_merge_tasks;
  ObArray<ObITask *> lob_meta_merge_tasks;
  ObArray<ObITask *> lob_piece_merge_tasks;
  ObArray<ObITask *> merge_tasks;
  if (OB_UNLIKELY(!is_inited_ || !is_owner_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL dag ctx is not initialized", K(ret));
  } else if (!runtime_initialized_ || OB_ISNULL(input_adapter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVFDDL runtime is not initialized", K(ret), K(tablet_id_));
  } else if (OB_FAIL(owner_dag_->get_tablet_context(
      tablet_id_, tablet_context))) {
    LOG_WARN("fail to get IVFDDL tablet context", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)
      || OB_ISNULL(tablet_context->ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVFDDL tablet context", K(ret), K(tablet_id_),
        KP(tablet_context));
  } else if (!input_adapter_->is_empty()
      && (OB_ISNULL(sstable_writer_) || !sstable_writer_->is_finished())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVFDDL physical output is incomplete", K(ret), K(tablet_id_),
        KP(sstable_writer_));
  } else if (!input_adapter_->is_empty()
      && OB_FAIL(tablet_context->ivf_ctx_->ivf_meta_.assign(
          tablet_context->arena_, sstable_writer_->get_ivf_meta()))) {
    LOG_WARN("fail to publish IVFDDL SSTable metadata", K(ret),
        K(tablet_id_), "ivf_meta", sstable_writer_->get_ivf_meta());
  } else if (FALSE_IT(tablet_context->ivf_ctx_->is_built_ = true)) {
  } else if (OB_FAIL(owner_dag_->init_tablet_merge_task(
      tablet_id_, true /* for_major */, data_merge_task,
      lob_meta_merge_task, lob_piece_merge_task))) {
    LOG_WARN("fail to init IVFDDL final merge task", K(ret),
        K(tablet_id_));
  } else if (OB_ISNULL(data_merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVFDDL data merge task is null", K(ret), K(tablet_id_));
  } else if (OB_FAIL(data_merge_tasks.push_back(data_merge_task))) {
    LOG_WARN("fail to retain IVFDDL data merge task", K(ret),
        K(tablet_id_));
  } else if (OB_NOT_NULL(lob_meta_merge_task)
      && OB_FAIL(lob_meta_merge_tasks.push_back(lob_meta_merge_task))) {
    LOG_WARN("fail to retain IVFDDL lob meta merge task", K(ret),
        K(tablet_id_));
  } else if (OB_NOT_NULL(lob_piece_merge_task)
      && OB_FAIL(lob_piece_merge_tasks.push_back(lob_piece_merge_task))) {
    LOG_WARN("fail to retain IVFDDL lob piece merge task", K(ret),
        K(tablet_id_));
  } else if (OB_FAIL(owner_dag_->add_merge_tasks(
      data_merge_tasks, lob_meta_merge_tasks, lob_piece_merge_tasks, merge_tasks))) {
    LOG_WARN("fail to collect IVFDDL final merge tasks", K(ret),
        K(tablet_id_));
  } else if (OB_FAIL(owner_dag_->set_merge_tasks(
      data_merge_tasks, lob_meta_merge_tasks, lob_piece_merge_tasks,
      &parent_task, nullptr /* next_task */))) {
    LOG_WARN("fail to connect IVFDDL final merge tasks", K(ret),
        K(tablet_id_));
  } else if (FALSE_IT(graph_published = true)) {
  } else if (OB_FAIL(owner_dag_->batch_add_task(merge_tasks))) {
    LOG_WARN("fail to publish IVFDDL final merge tasks", K(ret),
        K(tablet_id_));
  }
  if (OB_FAIL(ret) && !graph_published) {
    for (int64_t i = 0; i < data_merge_tasks.count(); ++i) {
      ObITask *task = data_merge_tasks.at(i);
      if (OB_NOT_NULL(task)) {
        task->reset_node();
        (void)owner_dag_->remove_task(*task);
      }
    }
    for (int64_t i = 0; i < lob_meta_merge_tasks.count(); ++i) {
      ObITask *task = lob_meta_merge_tasks.at(i);
      if (OB_NOT_NULL(task)) {
        task->reset_node();
        (void)owner_dag_->remove_task(*task);
      }
    }
    for (int64_t i = 0; i < lob_piece_merge_tasks.count(); ++i) {
      ObITask *task = lob_piece_merge_tasks.at(i);
      if (OB_NOT_NULL(task)) {
        task->reset_node();
        (void)owner_dag_->remove_task(*task);
      }
    }
  }
  if (OB_SUCC(ret)) {
    build_finished_ = true;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
