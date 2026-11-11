/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/blocksstable/vector_index/ob_ivf_merge_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_monitor.h"
#include "storage/ddl/ob_ddl_dag_monitor_mgr.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_sstable_writer.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_async_index_merge_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace compaction
{

ObIVFMergeDagCtx::ObIVFMergeDagCtx()
  : is_inited_(false),
    reuse_input_(false),
    process_type_(storage::ObIVFProcessType::INVALID),
    owner_dag_(nullptr),
    merge_ctx_(nullptr),
    planner_(nullptr),
    processor_allocator_(),
    merge_processor_(),
    full_rebuild_planner_(),
    concat_planner_(),
    insertion_planner_(),
    data_store_desc_(nullptr),
    hca_algorithm_(nullptr),
    center_norm_info_(nullptr),
    hgraph_allocator_(nullptr),
    input_adapter_(nullptr),
    capture_ctx_(nullptr),
    sstable_writer_(nullptr),
    runtime_allocator_(nullptr),
    input_allocator_(nullptr),
    monitor_node_(nullptr),
    build_monitor_(nullptr),
    step_monitor_(nullptr),
    current_step_perf_(),
    build_start_us_(0),
    finished_steps_(0),
    build_perf_finished_(false),
    build_finished_(false)
{
}

ObIVFMergeDagCtx::~ObIVFMergeDagCtx()
{
  // At this point the owning DAG has stopped and destroyed its tasks.
  const int result = nullptr != owner_dag_ && OB_SUCCESS != owner_dag_->get_dag_ret()
      ? owner_dag_->get_dag_ret() : OB_CANCELED;
  finish_build_perf(result);
  if (nullptr != monitor_node_) {
    monitor_node_->mark_finished();
    monitor_node_->dec_ref();
    monitor_node_ = nullptr;
  }
  destroy_runtime_();
}

int ObIVFMergeDagCtx::start(
    ObAsyncIndexMergeDag &merge_dag,
    ObAsyncIndexMergeCtx &merge_ctx,
    share::ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  bool reused = false;
  ObIVFMergeCapturePipeline *capture_task = nullptr;
  ObIVFMergeInputReadyTask *ready_task = nullptr;
  if (is_inited_ || nullptr != runtime_allocator_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF capture DAG context already initialized", K(ret), K(is_inited_), KP(runtime_allocator_));
  } else if (!is_major_merge_type(merge_ctx.get_merge_type())
      && !is_inc_major_merge(merge_ctx.get_merge_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported IVF capture merge type", K(ret), K(merge_ctx.get_merge_type()));
  } else if (FALSE_IT(init_monitor(merge_dag, merge_ctx))) {
  } else if (OB_FAIL(init_merge_input_(merge_ctx, reused))) {
    LOG_WARN("failed to prepare IVF capture", K(ret));
  } else {
    owner_dag_ = &merge_dag;
    merge_ctx_ = &merge_ctx;
    if (reused) {
      is_inited_ = true;
      if (nullptr != build_monitor_) {
        build_monitor_->set_merge_strategy(strategy_name());
        build_monitor_->set_input_vectors(0);
      }
      ret = materialize_finish(prepare_task);
      finish_build_perf(ret);
    } else if (OB_FAIL(merge_dag.create_task(&prepare_task, capture_task, capture_ctx_, 0))) {
      LOG_WARN("failed to create IVF capture task", K(ret));
    } else if (OB_FAIL(merge_dag.create_task(capture_task, ready_task, this))) {
      LOG_WARN("failed to create IVF input-ready task", K(ret));
    }
  }
  if (OB_FAIL(ret)) { finish_build_perf(ret); }
  return ret;
}

int ObIVFMergeInputReadyTask::init(ObIVFMergeDagCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (nullptr != ctx_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF capture DAG already initialized", K(ret));
  } else if (nullptr == ctx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF capture DAG arguments", K(ret));
  } else {
    ctx_ = ctx;
  }
  return ret;
}

int ObIVFMergeInputReadyTask::process()
{
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  const int ret = nullptr == ctx_ ? OB_NOT_INIT : ctx_->finish_capture(*this);
  if (OB_SUCCESS != ret) {
    LOG_WARN("failed to process IVF input-ready task", K(ret));
  }
  return ret;
}

int ObIVFMergeDagCtx::finish_capture(share::ObITask &ready_task)
{
  int ret = OB_SUCCESS;
  if (nullptr == capture_ctx_ || nullptr == input_adapter_ || nullptr == merge_ctx_ || nullptr == owner_dag_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF capture finish context is not initialized", K(ret), KP(capture_ctx_), KP(input_adapter_),
        KP(merge_ctx_), KP(owner_dag_));
  } else if (OB_FAIL(capture_ctx_->finish())) {
    LOG_WARN("failed to seal IVF captured input", K(ret));
  } else {
    capture_ctx_->~ObIVFMergeCaptureCtx();
    runtime_allocator_->free(capture_ctx_);
    capture_ctx_ = nullptr;
    if (input_adapter_->is_empty()) {
      // All-deleted input must publish an empty replacement, not leave old segments.
      blocksstable::ObSSTableIndexBuilder *index_builder = merge_ctx_->get_merge_info().get_index_builder();
      blocksstable::ObIvfSSTableMeta empty_meta;
      empty_meta.set_empty();
      if (nullptr == index_builder) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing IVF index builder for empty captured input", K(ret));
      } else if (OB_FAIL(index_builder->set_ivf_meta(empty_meta))) {
        LOG_WARN("failed to set empty IVF metadata", K(ret));
      } else {
        is_inited_ = true;
        if (nullptr != build_monitor_) { build_monitor_->set_input_vectors(0); }
        ret = materialize_finish(ready_task);
        finish_build_perf(ret);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to materialize empty IVF capture finish", K(ret));
        } else {
          build_finished_ = true;
          release_finished_input_();
        }
      }
    } else if (OB_FAIL(input_adapter_->capture_prior_(*merge_ctx_))) {
      LOG_WARN("failed to capture IVF merge priors", K(ret));
    } else if (OB_FAIL(start_with_input(*owner_dag_, *merge_ctx_, input_adapter_->get_merge_input(), ready_task))) {
      LOG_WARN("failed to start IVF build with sealed input", K(ret));
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::start_with_input(
    ObAsyncIndexMergeDag &merge_dag,
    ObAsyncIndexMergeCtx &merge_ctx,
    const storage::ObIVFMergeInput &merge_input,
    share::ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  storage::ObIVFProcessType selected = storage::ObIVFProcessType::FULL_REBUILD;
  const storage::ObIVFMergePrior *prior = input_adapter_ == nullptr ? nullptr : &input_adapter_->get_prior();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge dag ctx init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_input.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge input", K(ret), K(merge_input));
  } else if (!is_major_merge_type(merge_ctx.get_merge_type())
      && !is_inc_major_merge(merge_ctx.get_merge_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF merge DAG only supports major or inc major merge input",
        K(ret), "merge_type", merge_ctx.get_merge_type());
  } else if (prior != nullptr && OB_FAIL(storage::ObIVFMergeStrategy::select(
      *prior, merge_input.cluster_size_, selected))) {
    LOG_WARN("failed to select IVF merge strategy", K(ret), KPC(prior));
  } else if (OB_FAIL(init_merge_runtime_(
      merge_ctx, merge_input))) {
    LOG_WARN("failed to initialize IVF merge runtime",
        K(ret), K(merge_ctx));
  } else {
    if ((selected == storage::ObIVFProcessType::INSERTION) && prior != nullptr && prior->posting_source_ == nullptr) {
      selected = storage::ObIVFProcessType::FULL_REBUILD;
    }
    if ((selected == storage::ObIVFProcessType::INSERTION) && prior != nullptr
        && prior->dominant_segment_ >= 0
        && prior->dominant_segment_ < input_adapter_->source_tables_.get_count()) {
      const auto *table = input_adapter_->source_tables_.get_table(prior->dominant_segment_);
      hca_algorithm_->prepare_insertion_source(*input_adapter_, *prior, input_adapter_->dist_type_,
          static_cast<const blocksstable::ObIvfSSTable *>(table)->get_ivf_meta().leaf_cid_base());
    }
    if (OB_FAIL(start_with_runtime(
      merge_dag, merge_ctx, merge_input, *hca_algorithm_,
      *sstable_writer_, prepare_task, selected, prior))) {
      LOG_WARN("failed to start IVF merge runtime", K(ret), K(merge_ctx));
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::start_with_runtime(
    ObAsyncIndexMergeDag &merge_dag,
    ObAsyncIndexMergeCtx &merge_ctx,
    const storage::ObIVFMergeInput &merge_input,
    storage::ObIVFMergeAlgorithm &algorithm,
    storage::ObIVFMergeQuantizer &physical_output,
    share::ObITask &prepare_task,
    const storage::ObIVFProcessType process_type,
    const storage::ObIVFMergePrior *prior)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge dag ctx init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_input.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge input", K(ret), K(merge_input));
  } else if (!is_major_merge_type(merge_ctx.get_merge_type())
      && !is_inc_major_merge(merge_ctx.get_merge_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF merge DAG only supports major or inc major merge input",
        K(ret), "merge_type", merge_ctx.get_merge_type());
  } else {
    owner_dag_ = &merge_dag;
    merge_ctx_ = &merge_ctx;
    process_type_ = process_type;
    init_monitor(merge_dag, merge_ctx);
    if (nullptr != build_monitor_) {
      build_monitor_->set_merge_strategy(strategy_name());
      build_monitor_->set_input_vectors(merge_input.vector_store_->get_vector_count());
    }
    common::ObIAllocator &allocator = merge_ctx.mem_ctx_.get_safe_arena();
    processor_allocator_.set_attr(ObMemAttr(MTL_ID(), "IVFMergeRun", merge_ctx.mem_ctx_.get_ctx_id()));
    if (OB_FAIL(merge_processor_.init(
        processor_allocator_, merge_input, algorithm, physical_output, process_type, prior))) {
      LOG_WARN("fail to init IVF merge processor",
          K(ret), K(merge_input));
    } else if (hca_algorithm_ != nullptr && &algorithm == hca_algorithm_
        && OB_FAIL(merge_processor_.configure_ddl_runtime(*hca_algorithm_,
            MAX(1L, merge_input.vector_store_->get_source_read_parallelism())))) {
      LOG_WARN("failed to configure IVF merge source execution", K(ret));
    } else {
      switch (process_type_) {
        case storage::ObIVFProcessType::FULL_REBUILD:
          ret = full_rebuild_planner_.init(allocator, merge_processor_);
          planner_ = &full_rebuild_planner_;
          break;
        case storage::ObIVFProcessType::CONCATENATION:
          ret = concat_planner_.init(allocator, merge_processor_);
          planner_ = &concat_planner_;
          break;
        // Retain immutable leaves and compute only the delta.
        case storage::ObIVFProcessType::INSERTION:
          ret = insertion_planner_.init(allocator, merge_processor_);
          planner_ = &insertion_planner_;
          break;
        default:
          LOG_WARN("unsupported IVF merge process type", "ret", OB_INVALID_ARGUMENT, K_(process_type),
              "merge_type", merge_ctx.get_merge_type());
          ret = OB_INVALID_ARGUMENT;
          break;
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        static_cast<storage::ObIVFMergePlanner *>(planner_)->set_perf_observer(this);
        LOG_INFO("starting IVF merge strategy", K_(process_type),
            "merge_type", merge_ctx.get_merge_type(), KPC(prior));
        if (OB_FAIL(start_runtime(prepare_task))) {
          LOG_WARN("failed to start IVF merge runtime", K(ret), K_(process_type));
        }
      }
    }
  }
  if (OB_FAIL(ret)) { finish_build_perf(ret); }
  return ret;
}

const char *ObIVFMergeDagCtx::strategy_name() const
{
  if (reuse_input_) { return "SINGLE_INC_REUSE"; }
  switch (process_type_) {
    case storage::ObIVFProcessType::FULL_REBUILD: return "FULL_REBUILD";
    case storage::ObIVFProcessType::CONCATENATION: return "CONCATENATION";
    case storage::ObIVFProcessType::INSERTION: return "INSERTION";
    default: return "INVALID";
  }
}

void ObIVFMergeDagCtx::init_monitor(ObAsyncIndexMergeDag &dag, ObAsyncIndexMergeCtx &ctx)
{
  if (build_start_us_ == 0) {
    owner_dag_ = &dag;
    merge_ctx_ = &ctx;
    build_start_us_ = ObTimeUtility::current_time();
    storage::ObDDLDagMonitorMgr *mgr = MTL(storage::ObDDLDagMonitorMgr *);
    if (nullptr != mgr) {
      const int ret = mgr->register_node(&dag, dag.get_dag_id(), monitor_node_);
      if (OB_SUCCESS != ret) {
        LOG_WARN("failed to register IVF merge monitoring", K(ret), K(dag.get_dag_id()));
      } else if (nullptr != monitor_node_) {
        monitor_node_->inc_ref();
        storage::ObIVFMergeStepPerf perf;
        perf.start_us_ = build_start_us_;
        if (OB_SUCCESS == monitor_node_->alloc_monitor_info(nullptr, build_monitor_)
            && nullptr != build_monitor_) {
          build_monitor_->init_merge("IVF_MERGE", build_start_us_, ctx.get_ls_id().id(),
              ctx.get_tablet_id().id(), ctx.get_snapshot(), merge_type_to_str(ctx.get_merge_type()),
              strategy_name(), dag.get_dag_id(), perf);
        }
      }
    }
  }
}

void ObIVFMergeDagCtx::on_step_perf(const storage::ObIVFMergeStepPerf &perf)
{
  const bool is_new = current_step_perf_.plan_id_ != perf.plan_id_;
  current_step_perf_ = perf;
  if (is_new) {
    step_monitor_ = nullptr;
    if (nullptr != monitor_node_
        && OB_SUCCESS == monitor_node_->alloc_monitor_info(nullptr, step_monitor_)
        && nullptr != step_monitor_) {
      step_monitor_->init_merge("IVF_MERGE_STEP", build_start_us_, merge_ctx_->get_ls_id().id(),
          merge_ctx_->get_tablet_id().id(), merge_ctx_->get_snapshot(),
          merge_type_to_str(merge_ctx_->get_merge_type()), strategy_name(), owner_dag_->get_dag_id(), perf);
    }
    LOG_INFO("[IVF_MERGE_PERF]", "event", "step_start", "execution_id", build_start_us_,
        "trace_id", owner_dag_->get_dag_id(), "tablet_id", merge_ctx_->get_tablet_id(),
        "strategy", strategy_name(), "stage", storage::ivf_merge_stage_name(perf.stage_),
        "step", storage::ivf_merge_step_name(perf), "depth", perf.depth_, "plan_id", perf.plan_id_);
  }
  if (nullptr != step_monitor_) {
    step_monitor_->update_step(perf, perf.end_us_ > 0);
  } else if (perf.end_us_ > 0) {
    LOG_INFO("[IVF_MERGE_PERF]", "event", "step_finish", "execution_id", build_start_us_,
        "trace_id", owner_dag_->get_dag_id(), "tablet_id", merge_ctx_->get_tablet_id(),
        "stage", storage::ivf_merge_stage_name(perf.stage_), "step", storage::ivf_merge_step_name(perf),
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

void ObIVFMergeDagCtx::init_task_perf(storage::ObIVFBuildMonitorInfo &info, const int64_t task_idx)
{
  storage::ObIVFMergeWorkPerf work;
  const int ret = merge_processor_.get_work_perf(task_idx, work);
  if (OB_SUCCESS != ret) { LOG_WARN("failed to describe IVF merge work item", K(ret), K(task_idx)); }
  info.init_merge("IVF_MERGE_TASK", build_start_us_, merge_ctx_->get_ls_id().id(),
      merge_ctx_->get_tablet_id().id(), merge_ctx_->get_snapshot(),
      merge_type_to_str(merge_ctx_->get_merge_type()), strategy_name(), owner_dag_->get_dag_id(),
      current_step_perf_, task_idx, work);
}

void ObIVFMergeDagCtx::finish_build_perf(const int ret_code)
{
  if (build_start_us_ > 0 && !build_perf_finished_) {
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
      LOG_INFO("[IVF_MERGE_PERF]", "event", "merge_finish", "scope", "ivf_merge_runtime",
          "execution_id", build_start_us_, "trace_id", owner_dag_->get_dag_id(),
          "tablet_id", merge_ctx_->get_tablet_id(), "strategy", strategy_name(),
          "total_us", MAX(0, end_us - build_start_us_), K_(finished_steps), K(ret_code),
          "monitor_missing", true);
    }
    build_perf_finished_ = true;
  }
}

int ObIVFMergeDagCtx::init_merge_input_(ObAsyncIndexMergeCtx &merge_ctx, bool &reused)
{
  int ret = OB_SUCCESS;
  bool single_inc = false;
  reused = false;
  common::ObIAllocator &allocator = merge_ctx.mem_ctx_.get_safe_arena();
  if (nullptr != runtime_allocator_ || nullptr != input_adapter_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge input already initialized", K(ret), KP(runtime_allocator_), KP(input_adapter_));
  } else if (OB_FAIL(check_single_inc_reuse_(merge_ctx, single_inc))) {
    LOG_WARN("failed to check single IVF increment", K(ret));
  } else if (single_inc && OB_FAIL(try_reuse_input_(merge_ctx, reuse_input_, true))) {
    LOG_WARN("failed to reuse single IVF increment", K(ret));
  } else if (reuse_input_) {
    reused = true;
  } else if (FALSE_IT(runtime_allocator_ = &allocator)) {
  } else if (OB_ISNULL(input_allocator_ = OB_NEWx(
      ObLocalArena, &allocator, merge_ctx.mem_ctx_, "IVFMergeInput"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF input arena", K(ret));
  } else if (OB_ISNULL(input_adapter_ = OB_NEWx(
      ObIVFMergeInputAdapter, &allocator, *input_allocator_, &merge_ctx.mem_ctx_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF merge input adapter", K(ret));
  } else if (OB_FAIL(input_adapter_->prepare_schema_and_meta_(merge_ctx))) {
    LOG_WARN("failed to prepare IVF source schema", K(ret));
  } else if (OB_FAIL(input_adapter_->prepare_source_(merge_ctx))) {
    LOG_WARN("failed to prepare IVF source handles", K(ret));
  } else if (OB_ISNULL(capture_ctx_ = OB_NEWx(ObIVFMergeCaptureCtx, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF capture context", K(ret));
  } else if (OB_FAIL(capture_ctx_->init(merge_ctx, *input_adapter_))) {
    LOG_WARN("failed to initialize IVF capture", K(ret));
  }
  if (OB_FAIL(ret)) { destroy_runtime_(); }
  return ret;
}

int ObIVFMergeDagCtx::check_single_inc_reuse_(ObAsyncIndexMergeCtx &merge_ctx, bool &single_inc) const
{
  using namespace storage;
  int ret = OB_SUCCESS;
  single_inc = false;
  const ObTablesHandleArray &tables = merge_ctx.static_param_.tables_handle_;
  if (is_inc_major_merge(merge_ctx.get_merge_type()) && tables.get_count() == 1) {
    const ObITable *table = tables.get_table(0);
    ObITable::TableKey output_key;
    // Match the IncMajor key generated by ObTabletCreateSSTableParam::init_for_merge.
    output_key.table_type_ = merge_ctx.get_merged_table_type(nullptr, false);
    output_key.tablet_id_ = merge_ctx.get_tablet_id();
    output_key.column_group_idx_ = 0;
    output_key.scn_range_ = merge_ctx.static_param_.scn_range_;
    if (OB_ISNULL(table) || !table->is_inc_ivf_sstable() || !output_key.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input for single IVF increment reuse", K(ret), KPC(table), K(output_key));
    } else {
      single_inc = table->get_key() == output_key
          && !static_cast<const ObIvfSSTable *>(table)->is_empty();
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::prepare_reuse_layout_(ObAsyncIndexMergeCtx &merge_ctx, const bool clustered)
{
  using namespace blocksstable;
  int ret = OB_SUCCESS;
  auto &desc = merge_ctx.get_merge_info().get_sstable_build_desc().get_desc();
  ObSSTableIndexBuilder *builder = merge_ctx.get_merge_info().get_index_builder();
  if (OB_ISNULL(builder) || !builder->is_inited() || builder->is_closed() || OB_ISNULL(desc.static_desc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF reuse builder is not ready", K(ret), KP(builder));
  } else {
    // No output writer has been opened yet. Reinitialize the empty builder so
    // it and the writer both preserve the source macro layout under the same key.
    const auto mode = builder->get_optimization_mode();
    desc.static_desc_->micro_index_clustered_ = clustered;
    builder->reset();
    if (OB_FAIL(builder->init(desc, mode))) {
      LOG_WARN("failed to prepare IVF reuse builder with source layout", K(ret), K(clustered));
    } else {
      builder->set_is_async_index(true);
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::check_input_reuse_(
    ObAsyncIndexMergeCtx &merge_ctx, bool &can_reuse, const bool single_inc) const
{
  using namespace blocksstable;
  using namespace storage;
  int ret = OB_SUCCESS;
  can_reuse = false;
  const ObTablesHandleArray &tables = merge_ctx.get_tables_handle();
  const bool major = is_major_merge_type(merge_ctx.get_merge_type());
  if ((tables.get_count() < 2 && !single_inc)
      || (!major && !is_inc_major_merge(merge_ctx.get_merge_type()))) {
  } else if (major && (!merge_ctx.deletion_bitmap_handle_.is_valid()
      || !merge_ctx.deletion_bitmap_handle_.is_empty())) {
    // Major must apply deletions; INC retains the separate delete-index semantics.
  } else {
    can_reuse = true;
    for (int64_t i = 0; OB_SUCC(ret) && can_reuse && i < tables.get_count(); ++i) {
      const ObITable *table = tables.get_table(i);
      if (OB_ISNULL(table) || !table->is_ivf_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid empty-increment IVF input table", K(ret), K(i), KP(table));
      } else if (i > 0 && !static_cast<const ObIvfSSTable *>(table)->is_empty()) {
        can_reuse = false;
      }
    }
    if (OB_SUCC(ret) && can_reuse) {
      const auto &base = *static_cast<const ObIvfSSTable *>(tables.get_table(0));
      // Only input zero has nested-offset protection during major publication.
      // INC small SSTables must still use the normal rewrite path.
      can_reuse = !base.is_empty() && !base.contain_uncommitted_row()
          && (major ? base.is_major_sstable() : base.is_inc_ivf_sstable() && !base.is_small_sstable());
      if (single_inc && !can_reuse) {
        ret = OB_INVALID_DATA;
        LOG_WARN("cannot reuse single IVF input under the same key", K(ret), K(base));
      }
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::try_reuse_input_(
    ObAsyncIndexMergeCtx &merge_ctx, bool &reused, const bool single_inc)
{
  using namespace blocksstable;
  using namespace storage;
  int ret = OB_SUCCESS;
  bool can_reuse = false;
  reused = false;
  if (OB_FAIL(check_input_reuse_(merge_ctx, can_reuse, single_inc))) {
    LOG_WARN("failed to check empty-increment IVF reuse", K(ret));
  } else if (can_reuse) {
    const ObTablesHandleArray &tables = merge_ctx.get_tables_handle();
    const ObITableReadInfo &read_info = merge_ctx.read_info_;
    auto &base = *static_cast<ObIvfSSTable *>(tables.get_table(0));
    ObSSTableIndexBuilder *builder = merge_ctx.get_merge_info().get_index_builder();
    ObDataStoreDesc desc;
    ObMacroSeqParam seq;
    ObDatumRange range;
    range.set_whole_range();
    ObIMacroBlockIterator *iter = nullptr;
    ObDataMacroBlockMeta macro_meta;
    ObMacroBlockDesc macro;
    macro.macro_meta_ = &macro_meta;
    ObMacroBlockWriter writer;
    ObIAllocator &allocator = merge_ctx.mem_ctx_.get_safe_arena();
    if (nullptr == builder || !builder->is_inited() || nullptr == builder->get_object_cleaner()) {
      ret = OB_NOT_INIT;
      LOG_WARN("empty-increment IVF index builder is not initialized", K(ret), KP(builder));
    } else if (OB_FAIL(desc.shallow_copy(merge_ctx.get_merge_info().get_sstable_build_desc().get_desc()))) {
      LOG_WARN("failed to copy empty-increment writer descriptor", K(ret));
    } else if (OB_FAIL(base.scan_macro_block(range, read_info, allocator,
        iter, false, true, true /* secondary metadata required for macro reuse */))) {
      LOG_WARN("failed to scan IVF macro metadata", K(ret));
    } else if (OB_FAIL(iter->get_next_macro_block(macro))) {
      if (OB_ITER_END == ret) { ret = OB_INVALID_DATA; }
      LOG_WARN("nonempty IVF source has no readable macro metadata", K(ret));
    } else if (!single_inc && macro.is_clustered_index_tree_ != desc.micro_index_clustered()) {
      // A different output identity may use the normal layout conversion path.
    } else if (single_inc && macro.is_clustered_index_tree_ != desc.micro_index_clustered()
        && OB_FAIL(prepare_reuse_layout_(merge_ctx, macro.is_clustered_index_tree_))) {
      LOG_WARN("failed to preserve single IVF input layout", K(ret));
    } else if (OB_FAIL(merge_ctx.generate_macro_seq_info(0, seq.start_))) {
      LOG_WARN("failed to allocate reused IVF macro sequence", K(ret));
    } else {
      // Use the regular merge-writer range, as inverted-index macro reuse does.
      // An IncMajor start may include a nonzero SSTable sequence prefix.
      seq.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      desc.sstable_index_builder_ = builder;
      const ObPreWarmerParam pre_warm(PRE_WARM_TYPE_NONE);
      if (OB_FAIL(writer.open(desc, 0, seq, pre_warm, *builder->get_object_cleaner()))) {
        LOG_WARN("failed to open reused IVF macro writer", K(ret));
      }
      while (OB_SUCC(ret)) {
        const ObMicroBlockData *clustered_info = nullptr;
        if (macro.contain_uncommitted_row_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("committed IVF source contains an uncommitted macro", K(ret), K(macro));
        } else if (macro.is_clustered_index_tree_
            && OB_FAIL(iter->get_current_clustered_index_info(clustered_info))) {
          LOG_WARN("failed to read reused IVF clustered index", K(ret));
        } else if (OB_FAIL(writer.append_macro_block(macro, clustered_info))) {
          LOG_WARN("failed to reuse IVF macro block", K(ret));
        } else if (OB_FAIL(share::dag_yield())) {
          LOG_WARN("empty-increment IVF reuse interrupted", K(ret));
        } else if (OB_FAIL(iter->get_next_macro_block(macro))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to read next reused IVF macro block", K(ret), K(base.get_key()));
          }
          break;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(writer.close())) {
        LOG_WARN("failed to close reused IVF macro writer", K(ret));
      }
      if (OB_SUCC(ret) && (writer.get_merge_block_info().macro_block_count_ != base.get_data_macro_block_count()
          || writer.get_merge_block_info().total_row_count_ != base.get_row_count())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("reused IVF macro metadata does not cover source", K(ret),
            K(writer.get_merge_block_info()), K(base.get_data_macro_block_count()), K(base.get_row_count()));
      }
      if (OB_SUCC(ret) && OB_FAIL(builder->set_ivf_meta(base.get_ivf_meta()))) {
        LOG_WARN("failed to preserve reused IVF metadata", K(ret));
      }
      if (OB_SUCC(ret)) {
        merge_ctx.get_merge_info().get_merge_history().update_block_info(writer.get_merge_block_info(), false);
        reused = true;
        LOG_INFO("reuse IVF data macros without reclustering", K(base.get_key()), K(single_inc),
            "snapshot", merge_ctx.get_snapshot(), "input_count", tables.get_count());
      }
    }
    if (nullptr != iter) {
      iter->~ObIMacroBlockIterator();
      allocator.free(iter);
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::init_merge_runtime_(
    ObAsyncIndexMergeCtx &merge_ctx,
    const storage::ObIVFMergeInput &input)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableIndexBuilder *index_builder = nullptr;
  share::ObPluginVectorIndexService *vector_index_service = nullptr;
  common::ObIAllocator &allocator = merge_ctx.mem_ctx_.get_safe_arena();
  storage::ObIVFHcaAlgorithmParam algorithm_param;
  blocksstable::ObMacroSeqWindow data_macro_seq_window;
  if (!input.is_valid()
      || OB_NOT_NULL(data_store_desc_)
      || OB_NOT_NULL(hca_algorithm_)
      || OB_NOT_NULL(center_norm_info_)
      || OB_NOT_NULL(hgraph_allocator_)
      || OB_NOT_NULL(sstable_writer_)
      || (OB_NOT_NULL(runtime_allocator_)
          && runtime_allocator_ != &allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge runtime state", K(ret), K(input));
  } else if (FALSE_IT(runtime_allocator_ = &allocator)) {
  } else if (OB_ISNULL(index_builder =
      merge_ctx.get_merge_info().get_index_builder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF SSTable index builder is null", K(ret), K(merge_ctx));
  } else if (!index_builder->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF SSTable index builder is not initialized", K(ret));
  } else if (OB_ISNULL(data_store_desc_ = OB_NEWx(
      blocksstable::ObDataStoreDesc, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF merge data store descriptor", K(ret));
  } else if (OB_FAIL(data_store_desc_->shallow_copy(
      merge_ctx.get_merge_info().get_sstable_build_desc().get_desc()))) {
    LOG_WARN("failed to copy IVF merge data store descriptor", K(ret));
  } else if (FALSE_IT(data_store_desc_->sstable_index_builder_ =
      index_builder)) {
  } else if (OB_FAIL(merge_ctx.generate_macro_seq_info(
      0 /* task_idx */, data_macro_seq_window.start_))) {
    LOG_WARN("failed to allocate IVF data macro sequence", K(ret));
  } else if (OB_FAIL(merge_ctx.get_index_tree_macro_start_seq(
      data_macro_seq_window.end_))) {
    LOG_WARN("failed to get IVF data macro sequence boundary", K(ret));
  } else if (OB_ISNULL(sstable_writer_ = OB_NEWx(
      blocksstable::ObIVFMergeSSTableWriter, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF merge SSTable writer", K(ret));
  } else if (OB_FAIL(sstable_writer_->init(
      *data_store_desc_, *index_builder, data_macro_seq_window, merge_ctx.get_snapshot(),
      input.vector_store_->get_dimension(), input.dist_type_))) {
    LOG_WARN("failed to initialize IVF merge SSTable writer", K(ret));
  } else if (OB_ISNULL(vector_index_service =
      MTL(share::ObPluginVectorIndexService *))
      || OB_ISNULL(vector_index_service->get_all_vsag_use_mem())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF vector index service is unavailable", K(ret));
  } else if (OB_ISNULL(hgraph_allocator_ = OB_NEWx(
      share::ObVsagMemContext, &allocator,
      vector_index_service->get_all_vsag_use_mem()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF merge HGraph allocator", K(ret));
  } else if (OB_FAIL(hgraph_allocator_->init(
      vector_index_service->get_memory_context(),
      vector_index_service->get_all_vsag_use_mem(), MTL_ID(),
      "IVFMergeHGraph"))) {
    LOG_WARN("failed to initialize IVF merge HGraph allocator", K(ret));
  } else if (share::VIDA_COS == input.dist_type_
      && OB_ISNULL(center_norm_info_ = OB_NEWx(
          share::ObVectorNormalizeInfo, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF center normalization info", K(ret));
  } else if (OB_ISNULL(hca_algorithm_ = OB_NEWx(
      storage::ObIVFDDLHcaAdapter, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF HCA algorithm adapter", K(ret));
  } else {
    algorithm_param.center_norm_info_ = center_norm_info_;
    if (OB_FAIL(hca_algorithm_->init(
        algorithm_param, hgraph_allocator_))) {
      LOG_WARN("failed to initialize IVF HCA algorithm adapter", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    destroy_runtime_();
  }
  return ret;
}

void ObIVFMergeDagCtx::release_finished_input_()
{
  if (build_finished_ && build_perf_finished_ && nullptr != input_adapter_
      && nullptr != input_allocator_) {
    // Preserve the adapter shell and the shared index builder. Runtime views
    // borrow this object, but no task can read its vector payload after RAW.
    input_adapter_->release_input_buffers();
    input_allocator_->reset();
  }
}

void ObIVFMergeDagCtx::destroy_runtime_()
{
  if (OB_NOT_NULL(hca_algorithm_)) {
    hca_algorithm_->cancel(OB_CANCELED);
    hca_algorithm_->~ObIVFDDLHcaAdapter();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(hca_algorithm_);
    }
    hca_algorithm_ = nullptr;
  }
  if (OB_NOT_NULL(hgraph_allocator_)) {
    hgraph_allocator_->~ObVsagMemContext();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(hgraph_allocator_);
    }
    hgraph_allocator_ = nullptr;
  }
  if (OB_NOT_NULL(sstable_writer_)) {
    sstable_writer_->~ObIVFMergeSSTableWriter();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(sstable_writer_);
    }
    sstable_writer_ = nullptr;
  }
  if (nullptr != capture_ctx_) {
    capture_ctx_->~ObIVFMergeCaptureCtx();
    if (nullptr != runtime_allocator_) { runtime_allocator_->free(capture_ctx_); }
    capture_ctx_ = nullptr;
  }

  if (OB_NOT_NULL(input_adapter_)) {
    input_adapter_->~ObIVFMergeInputAdapter();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(input_adapter_);
    }
    input_adapter_ = nullptr;
  }
  if (nullptr != input_allocator_) {
    input_allocator_->~ObIAllocator();
    if (nullptr != runtime_allocator_) { runtime_allocator_->free(input_allocator_); }
    input_allocator_ = nullptr;
  }
  if (OB_NOT_NULL(center_norm_info_)) {
    center_norm_info_->~ObVectorNormalizeInfo();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(center_norm_info_);
    }
    center_norm_info_ = nullptr;
  }
  if (OB_NOT_NULL(data_store_desc_)) {
    data_store_desc_->~ObDataStoreDesc();
    if (OB_NOT_NULL(runtime_allocator_)) {
      runtime_allocator_->free(data_store_desc_);
    }
    data_store_desc_ = nullptr;
  }
  runtime_allocator_ = nullptr;
}

int ObIVFMergeDagCtx::start_runtime(share::ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  const storage::ObIVFTaskPlan *first_task_plan = nullptr;
  // The caller has already installed immutable input and the numerical
  // adapter. Planner/Processor own the complete logical merge flow.
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(owner_dag_) || OB_ISNULL(merge_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge dag ctx is not initialized", K(ret));
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF merge process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)));
  } else if (!planner_->is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge planner is not initialized", K(ret));
  } else if (OB_FAIL(planner_->start(first_task_plan))) {
    LOG_WARN("fail to start IVF merge planner", K(ret));
  } else if (OB_ISNULL(first_task_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF merge planner returned null first task plan", K(ret));
  } else if (OB_FAIL(materialize_task_plan(prepare_task, *first_task_plan))) {
    LOG_WARN("fail to materialize first IVF merge task plan", K(ret), KPC(first_task_plan));
    planner_->cancel(ret);
  }
  return ret;
}

ObIVFMergeTask::ObIVFMergeTask()
  : ObITask(ObITask::TASK_TYPE_MACROMERGE),
    is_inited_(false),
    ivf_merge_dag_ctx_(nullptr),
    task_plan_(nullptr),
    task_idx_(-1),
    monitor_info_(nullptr)
{
}

ObIVFMergeTask::~ObIVFMergeTask()
{
  if (nullptr != monitor_info_) {
    monitor_info_->cancel_unfinished();
    monitor_info_->mark_finished();
    monitor_info_ = nullptr;
  }
}

int ObIVFMergeTask::init(
    ObIVFMergeDagCtx *ivf_merge_dag_ctx,
    const storage::ObIVFTaskPlan *task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge task init twice", K(ret));
  } else if (OB_ISNULL(ivf_merge_dag_ctx) || OB_ISNULL(task_plan) || !task_plan->is_valid()
      || task_plan->get_process_type() != ivf_merge_dag_ctx->process_type_
      || !task_plan->is_valid_task_idx(task_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge task argument",
        K(ret), KP(ivf_merge_dag_ctx), KP(task_plan), K(task_idx));
  } else {
    ivf_merge_dag_ctx_ = ivf_merge_dag_ctx;
    task_plan_ = task_plan;
    task_idx_ = task_idx;
    is_inited_ = true;
    if (nullptr != ivf_merge_dag_ctx_->monitor_node_) {
      const int monitor_ret = ivf_merge_dag_ctx_->monitor_node_->alloc_monitor_info(this, monitor_info_);
      if (OB_SUCCESS == monitor_ret && nullptr != monitor_info_) {
        ivf_merge_dag_ctx_->init_task_perf(*monitor_info_, task_idx_);
      } else {
        LOG_WARN("failed to allocate IVF merge task monitoring", K(monitor_ret));
      }
    }
  }
  return ret;
}

int ObIVFMergeTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t begin_us = ObTimeUtility::current_time();
  if (nullptr != monitor_info_) { monitor_info_->begin_task(); }
  if (nullptr != ivf_merge_dag_ctx_) {
    if (nullptr != ivf_merge_dag_ctx_->step_monitor_) { ivf_merge_dag_ctx_->step_monitor_->task_started(); }
    if (nullptr != ivf_merge_dag_ctx_->build_monitor_) { ivf_merge_dag_ctx_->build_monitor_->task_started(); }
  }
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_merge_dag_ctx_)
      || OB_ISNULL(task_plan_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge task is not initialized",
        K(ret), KP_(ivf_merge_dag_ctx), KP_(task_plan), K_(task_idx));
  } else if (OB_FAIL(ivf_merge_dag_ctx_->process_task(*task_plan_, task_idx_))) {
    LOG_WARN("fail to process IVF merge task",
        K(ret), "plan_id", task_plan_->get_plan_id(), K_(task_idx));
  }
  const int64_t elapsed_us = MAX(0, ObTimeUtility::current_time() - begin_us);
  storage::ObIVFMergeWorkPerf work;
  if (nullptr != monitor_info_) { work = monitor_info_->get_work(); }
  if (nullptr != ivf_merge_dag_ctx_) {
    // The plan remains alive through this barrier. Output counts become known
    // only after execution; publish them together with the task's final state.
    storage::ObIVFMergeWorkPerf actual;
    if (OB_SUCCESS == ivf_merge_dag_ctx_->merge_processor_.get_work_perf(task_idx_, actual)) {
      work = actual;
    }
  }
  if (nullptr != monitor_info_) { monitor_info_->end_task(ret, elapsed_us, &work); }
  if (nullptr != ivf_merge_dag_ctx_) {
    if (nullptr != ivf_merge_dag_ctx_->step_monitor_) {
      ivf_merge_dag_ctx_->step_monitor_->add_task(ret, elapsed_us, work);
    }
    if (nullptr != ivf_merge_dag_ctx_->build_monitor_) {
      ivf_merge_dag_ctx_->build_monitor_->add_task(ret, elapsed_us, work);
    }
  }
  return ret;
}

int ObIVFMergeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_merge_dag_ctx_)
      || OB_ISNULL(task_plan_)
      || OB_ISNULL(ivf_merge_dag_ctx_->owner_dag_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge task is not initialized", K(ret), KP_(task_plan), K_(task_idx));
  } else if (task_idx_ + 1 == task_plan_->get_task_count()) {
    ret = OB_ITER_END;
  } else {
    ObIVFMergeTask *merge_task = nullptr;
    if (OB_FAIL(ivf_merge_dag_ctx_->owner_dag_->alloc_task(merge_task))) {
      LOG_WARN("fail to allocate next IVF merge task", K(ret), KP_(task_plan), K_(task_idx));
    } else if (OB_FAIL(merge_task->init(
        ivf_merge_dag_ctx_, task_plan_, task_idx_ + 1))) {
      LOG_WARN("fail to init next IVF merge task", K(ret), KP_(task_plan), K_(task_idx));
    } else {
      next_task = merge_task;
    }
  }
  return ret;
}

ObIVFMergeFinishTask::ObIVFMergeFinishTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
    is_inited_(false),
    ivf_merge_dag_ctx_(nullptr),
    task_plan_(nullptr)
{
}

int ObIVFMergeFinishTask::init(
    ObIVFMergeDagCtx *ivf_merge_dag_ctx,
    const storage::ObIVFTaskPlan *task_plan)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge finish task init twice", K(ret));
  } else if (OB_ISNULL(ivf_merge_dag_ctx) || OB_ISNULL(task_plan) || !task_plan->is_valid()
      || task_plan->get_process_type() != ivf_merge_dag_ctx->process_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge finish task argument",
        K(ret), KP(ivf_merge_dag_ctx), KP(task_plan));
  } else {
    ivf_merge_dag_ctx_ = ivf_merge_dag_ctx;
    task_plan_ = task_plan;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ivf_merge_dag_ctx_) || OB_ISNULL(task_plan_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge finish task is not initialized",
        K(ret), KP_(ivf_merge_dag_ctx), KP_(task_plan));
  } else if (OB_FAIL(ivf_merge_dag_ctx_->finish_task_plan(*this, *task_plan_))) {
    LOG_WARN("fail to finish IVF merge task plan", K(ret), KPC_(task_plan));
  } else {
    // The planner and its final callbacks have returned, all build tasks have
    // drained, and physical publication uses the already installed metadata.
    ivf_merge_dag_ctx_->release_finished_input_();
  }
  return ret;
}

int ObIVFMergeDagCtx::materialize_task_plan(
    share::ObITask &parent_task,
    const storage::ObIVFTaskPlan &task_plan)
{
  int ret = OB_SUCCESS;
  ObIVFMergeTask *first_merge_task = nullptr;
  ObIVFMergeFinishTask *finish_task = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(owner_dag_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge task plan", K(ret), K(task_plan));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(owner_dag_->create_task(
        &parent_task, first_merge_task,
        this, &task_plan, 0 /* task_idx */))) {
      LOG_WARN("fail to create first IVF merge task", K(ret), K(task_plan));
    } else if (OB_FAIL(owner_dag_->create_task(
        first_merge_task, finish_task, this, &task_plan))) {
      LOG_WARN("fail to create IVF merge finish task", K(ret), K(task_plan));
    }
  }
  return ret;
}

int ObIVFMergeDagCtx::process_task(
    const storage::ObIVFTaskPlan &task_plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(owner_dag_) || OB_ISNULL(merge_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_
      || !task_plan.is_valid_task_idx(task_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF task plan or task index", K(ret), K(task_plan), K(task_idx));
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF merge process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)));
  } else {
    if (OB_FAIL(planner_->process_task(task_plan, task_idx))) {
      if (OB_EAGAIN == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF merge processor returned unsupported suspend result",
            K(ret), K(task_plan), K(task_idx));
        planner_->cancel(ret);
      } else {
        LOG_WARN("fail to execute and publish IVF merge task",
            K(ret), K(task_plan), K(task_idx));
      }
    }
  }
  return ret;
}

void ObIVFMergeDagCtx::release_samples_after_step_(const storage::ObIVFMergeStepPerf &finished_step)
{
  if (nullptr != input_adapter_ && finished_step.stage_ == storage::ObIVFMergeStage::HIERARCHY
      && finished_step.depth_ == 0) {
    const bool full_rebuild_root_done = finished_step.step_ == storage::ObIVFMergeStep::HIERARCHY_CLUSTER;
    const bool reused_root_accepted = finished_step.step_ == storage::ObIVFMergeStep::HIERARCHY_ASSIGN
        && current_step_perf_.stage_ == storage::ObIVFMergeStage::HIERARCHY
        && current_step_perf_.step_ == storage::ObIVFMergeStep::HIERARCHY_UPDATE
        && current_step_perf_.depth_ == 0;
    // CONCATENATION/INSERTION may fall back during PRIOR or root ASSIGN.
    // Keep samples if the next operation is root CLUSTER for that fallback.
    if (full_rebuild_root_done || reused_root_accepted) {
      input_adapter_->release_first_level_samples();
    }
  }
}

int ObIVFMergeDagCtx::finish_task_plan(
    share::ObITask &finish_task,
    const storage::ObIVFTaskPlan &task_plan)
{
  int ret = OB_SUCCESS;
  const storage::ObIVFMergeStepPerf finished_step = current_step_perf_;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(owner_dag_) || OB_ISNULL(merge_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge dag ctx is not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_plan.is_valid()
      || task_plan.get_process_type() != process_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid finished IVF merge task plan", K(ret), K(task_plan));
  } else if (OB_ISNULL(planner_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("IVF merge process is not wired",
        K(ret), K(static_cast<int64_t>(process_type_)));
  } else {
    const storage::ObIVFTaskPlan *next_task_plan = nullptr;
    if (OB_FAIL(planner_->finish_task_plan(task_plan, next_task_plan))) {
      LOG_WARN("fail to finish IVF merge task plan", K(ret), K(task_plan));
    } else if (OB_NOT_NULL(next_task_plan)) {
      if (OB_FAIL(materialize_task_plan(finish_task, *next_task_plan))) {
        LOG_WARN("fail to materialize next IVF merge task plan",
            K(ret), KPC(next_task_plan));
        planner_->cancel(ret);
      }
    } else {
      // RAW_WRITE is the physical-output barrier. At this point the shared
      // writer has closed every data root and published IVF metadata to the
      // index builder; the generic async finish task can close the builder and
      // create the final ObIvfSSTable.
      ret = materialize_finish(finish_task);
      finish_build_perf(ret);
      if (OB_SUCC(ret)) { build_finished_ = true; }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to materialize final IVF merge graph", K(ret));
        planner_->cancel(ret);
      }
    }
    LOG_INFO("IVF merge runtime memory", K(ret), "plan_id", task_plan.get_plan_id(),
        "runtime_live_bytes", processor_allocator_.used(),
        "runtime_peak_bytes", processor_allocator_.peak(),
        "runtime_allocations", processor_allocator_.allocation_count(),
        "root_arena_hold", merge_ctx_->mem_ctx_.get_safe_arena().total());
  }
  if (OB_SUCC(ret)) {
    // Processor initialization consumed membership detail. Retain it until a
    // successful phase commit so failed preparation keeps its original input.
    if (nullptr != input_adapter_) { input_adapter_->release_prior_postings(); }
    release_samples_after_step_(finished_step);
  }
  return ret;
}

int ObIVFMergeDagCtx::materialize_finish(
    share::ObITask &parent_task)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexMergeFinishTask *finish_task = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(owner_dag_)
      || OB_ISNULL(merge_ctx_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF merge dag ctx is not initialized", K(ret));
  } else if (OB_FAIL(owner_dag_->create_task(
      &parent_task, finish_task))) {
    LOG_WARN("fail to create final async index merge finish task", K(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
