/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_build_and_write_pipeline.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ob_storage_schema.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_async_index_macro_writer.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_algorithm.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_index_builder_util.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace
{
int calc_ivf_physical_row_count(const int64_t center_count,
                                const int64_t vector_count,
                                const int64_t posting_count,
                                int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  if (OB_UNLIKELY(center_count < 0 || vector_count < 0
      || posting_count < vector_count || posting_count - vector_count > vector_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF physical row count input", K(ret),
             K(center_count), K(vector_count), K(posting_count));
  } else if (OB_UNLIKELY(vector_count > INT64_MAX - center_count
      || posting_count > INT64_MAX - center_count - vector_count)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF physical row count overflow", K(ret),
             K(center_count), K(vector_count), K(posting_count));
  } else {
    row_count = center_count + vector_count + posting_count;
  }
  return ret;
}
} // namespace

int ObIVFBuildOperator::init(
    ObDDLIndependentDag *dag,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF build operator init twice", K(ret), K_(tablet_id));
  } else if (OB_ISNULL(dag) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF build operator argument", K(ret), KP(dag), K(tablet_id));
  } else {
    dag_ = dag;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFBuildOperator::prepare_build_param_(ObIVFBuildParam &param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema *index_schema = nullptr;
  ObSEArray<ObTabletID, 16> index_tablets;
  int64_t tablet_pos = -1;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t index_table_id = dag_->get_ddl_table_schema().table_id_;
  const ObStorageSchema *storage_schema = dag_->get_ddl_table_schema().storage_schema_;
  int64_t schema_version = 0;
  if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF target storage schema is null", K(ret), K(index_table_id));
  } else if (OB_FALSE_IT(schema_version = OB_MAX(
      dag_->get_ddl_task_param().schema_version_, storage_schema->get_schema_version()))) {
    // The task version is the precise data-table version used by its SQL hint.
    // Partition DDL can give the index a later version. Resolve both tables in
    // a view covering the target storage schema already captured by this DAG.
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
          tenant_id, guard, schema_version))) {
    LOG_WARN("failed to get IVF build schema guard", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
    LOG_WARN("failed to get IVF index schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(index_schema) || !index_schema->is_async_ivf_index()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected IVF index schema", K(ret), K(index_table_id), KP(index_schema));
  } else if (OB_FAIL(index_schema->get_tablet_ids(index_tablets))) {
    LOG_WARN("failed to get IVF index tablets", K(ret));
  } else {
    for (int64_t i = 0; i < index_tablets.count() && tablet_pos < 0; ++i) {
      if (index_tablets.at(i) == tablet_id_) {
        tablet_pos = i;
      }
    }
    const ObTableSchema *data_schema = nullptr;
    if (tablet_pos < 0) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("IVF tablet is absent from index schema", K(ret), K(tablet_id_));
    } else if (OB_FAIL(guard.get_table_schema(
                   tenant_id, index_schema->get_data_table_id(), data_schema))) {
      LOG_WARN("failed to get IVF data schema", K(ret));
    } else if (OB_ISNULL(data_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("IVF data schema is null", K(ret), KPC(index_schema));
    } else {
      param.tenant_id_ = tenant_id;
      param.data_table_id_ = data_schema->get_table_id();
      param.index_table_id_ = index_table_id;
      param.index_tablet_id_ = tablet_id_;
      param.schema_version_ = schema_version;
    }
  }
  return ret;
}

int ObIVFBuildOperator::prepare_slice_replay_reader_(
    ObDDLTabletContext &tablet_context,
    ObIVFMultiSliceReplayReader &replay_reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_context.ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF tablet context is null while preparing slice replay",
             K(ret), K(tablet_id_));
  } else {
    ObArray<ObIVFSliceInput *> &inputs =
        tablet_context.ivf_ctx_->slice_inputs_;
    ObSEArray<ObIVFReplayInput *, 8> replay_inputs;
    for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
      if (OB_ISNULL(inputs.at(i)) || !inputs.at(i)->replay_.is_sealed()
          || inputs.at(i)->replay_.get_vector_dim()
              != tablet_context.ivf_ctx_->get_rotation_dim()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("IVF capture slice is not sealed", K(ret), K(i),
                 "slice_input", inputs.at(i),
                 "rotation_dim", tablet_context.ivf_ctx_->get_rotation_dim());
      }
    }
    struct SliceCmp
    {
      bool operator()(const ObIVFSliceInput *left,
                      const ObIVFSliceInput *right) const
      {
        return left->slice_idx_ < right->slice_idx_;
      }
    } cmp;
    if (OB_SUCC(ret)) {
      lib::ob_sort(inputs.begin(), inputs.end(), cmp);
      for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
        if (OB_FAIL(replay_inputs.push_back(&inputs.at(i)->replay_))) {
          LOG_WARN("failed to retain IVF slice replay pointer", K(ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replay_reader.init(
                   tablet_context.ivf_ctx_->get_rotation_dim(), replay_inputs))) {
      LOG_WARN("failed to initialize IVF multi-slice replay reader", K(ret),
               K(tablet_id_), "slice_count", replay_inputs.count());
    } else {
      int64_t avoided_payload_bytes = 0;
      const int64_t vector_dim = replay_reader.get_vector_dim();
      if (replay_reader.get_row_count() > 0
          && vector_dim > INT64_MAX / static_cast<int64_t>(sizeof(float))
                            / replay_reader.get_row_count()) {
        avoided_payload_bytes = INT64_MAX;
      } else {
        avoided_payload_bytes = replay_reader.get_row_count()
            * vector_dim * static_cast<int64_t>(sizeof(float));
      }
      LOG_INFO("IVF build uses slice replay directly", K(tablet_id_),
               "slice_count", replay_reader.get_input_count(),
               "row_count", replay_reader.get_row_count(), K(vector_dim),
               "slice_replay_mem_hold", replay_reader.get_source_mem_hold(),
               "slice_replay_file_size", replay_reader.get_source_file_size(),
               K(avoided_payload_bytes));
    }
  }
  return ret;
}

int ObIVFBuildOperator::collect_sample_refs_(
    ObDDLTabletContext &tablet_context,
    const ObIVFReplayReader &replay_reader)
{
  int ret = OB_SUCCESS;
  int64_t max_sample_count = 0;
  int64_t total_row_count = 0;
  const ObVectorIndexParam &vector_param = build_ctx_.schema_ctx_.get_vector_param();
  ObArenaAllocator sample_merge_allocator(ObMemAttr(MTL_ID(), "IVFRootSample"));
  ObIVFSampleRefMaxCmp sample_cmp;
  common::ObBinaryHeap<ObIVFSampleRef,
                       ObIVFSampleRefMaxCmp, 64> sample_heap(
                           sample_cmp, &sample_merge_allocator);
  if (OB_ISNULL(tablet_context.ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF tablet context is null while collecting samples", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(vector_param.nlist_ <= 0
             || vector_param.sample_per_nlist_ <= 0
             || INT64_MAX / vector_param.sample_per_nlist_
                < vector_param.nlist_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF build sample parameters", K(ret),
             K(vector_param));
  } else {
    // TODO(lyl512932): Derive the center count from the future cluster_size
    // parameter and use it for sampling instead of relying on nlist_.
    max_sample_count =
        vector_param.nlist_ * vector_param.sample_per_nlist_;
    ObArray<ObIVFSliceInput *> &inputs =
        tablet_context.ivf_ctx_->slice_inputs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
      ObIVFSliceInput *input = inputs.at(i);
      if (OB_ISNULL(input) || !input->replay_.is_sealed()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("IVF sample source is not sealed", K(ret), K(i),
                 KP(input));
      } else if (OB_UNLIKELY(!input->replay_.is_sampling_enabled()
                 || input->replay_.get_max_sample_count() != max_sample_count
                 || input->replay_.get_sample_scan_count()
                    != input->replay_.get_row_count()
                 || input->replay_.get_sample_count()
                    != MIN(input->replay_.get_row_count(), max_sample_count))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF captured sample state mismatch", K(ret), K(i),
                 K(input->slice_idx_),
                 "row_count", input->replay_.get_row_count(),
                 "sample_scan_count", input->replay_.get_sample_scan_count(),
                 "sample_count", input->replay_.get_sample_count(),
                 "captured_max_sample_count",
                     input->replay_.get_max_sample_count(),
                 K(max_sample_count));
      } else if (OB_UNLIKELY(INT64_MAX - total_row_count
                             < input->replay_.get_row_count())) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("IVF captured row count overflow", K(ret), K(i),
                 K(input->slice_idx_), K(total_row_count),
                 "slice_row_count", input->replay_.get_row_count());
      } else {
        total_row_count += input->replay_.get_row_count();
        const ObIArray<ObIVFSampleCandidate> &candidates =
            input->replay_.get_sample_candidates();
        for (int64_t j = 0; OB_SUCC(ret) && j < candidates.count(); ++j) {
          const ObIVFSampleCandidate &candidate = candidates.at(j);
          const ObIVFSampleRef sample_ref(candidate, &input->replay_);
          if (OB_UNLIKELY(!sample_ref.is_valid()
              || candidate.slice_idx_ != input->slice_idx_
              || candidate.row_id_ >= input->replay_.get_row_count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid captured IVF sample locator", K(ret),
                     K(i), K(j), K(sample_ref), K(input->slice_idx_),
                     "row_count", input->replay_.get_row_count());
          } else if (sample_heap.count() < max_sample_count) {
            if (OB_FAIL(sample_heap.push(sample_ref))) {
              LOG_WARN("failed to merge IVF sample locator", K(ret),
                       K(sample_ref));
            }
          } else if (sample_cmp(sample_ref, sample_heap.top())
                     && OB_FAIL(sample_heap.replace_top(sample_ref))) {
            LOG_WARN("failed to replace merged IVF sample locator",
                     K(ret), K(sample_ref));
          }
        }
      }
    }
    const int64_t expected_sample_count =
        MIN(total_row_count, max_sample_count);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(total_row_count
                           != replay_reader.get_row_count()
               || sample_heap.count() != expected_sample_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF global sample merge count mismatch", K(ret),
               K(total_row_count),
               "replay_row_count", replay_reader.get_row_count(),
               K(expected_sample_count),
               "sample_count", sample_heap.count());
    } else if (OB_FAIL(build_ctx_.sample_refs_.assign(
                   sample_heap.get_heap_data()))) {
      LOG_WARN("failed to retain merged IVF sample locators", K(ret),
               K(expected_sample_count));
    } else {
      struct SampleReadOrderCmp
      {
        bool operator()(const ObIVFSampleRef &left,
                        const ObIVFSampleRef &right) const
        {
          return left.candidate_.slice_idx_ != right.candidate_.slice_idx_
              ? left.candidate_.slice_idx_ < right.candidate_.slice_idx_
              : (left.candidate_.row_id_ != right.candidate_.row_id_
                  ? left.candidate_.row_id_ < right.candidate_.row_id_
                  : ObIVFSampleCandidate::is_better(
                      left.candidate_, right.candidate_));
        }
      } read_order_cmp;
      // The bottom-K set is already fixed. Reading it in slice/row order keeps
      // spilled replay access sequential instead of turning K sample reads
      // into random I/O.
      lib::ob_sort(build_ctx_.sample_refs_.begin(),
                   build_ctx_.sample_refs_.end(), read_order_cmp);
      LOG_INFO("IVF capture samples merged", K(tablet_id_),
               "slice_count", inputs.count(), K(total_row_count),
               K(expected_sample_count));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_IVF_BASELINE_BUILD_FAILED);
ERRSIM_POINT_DEF(EN_ASYNC_IVF_REFRESH_WRITE_FAIL);

int ObIVFBuildOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = NEED_MORE_INPUT;
  output_chunk.reset();
  ObDDLTabletContext *tablet_context = nullptr;
  ObIVFBuildParam build_param;
  ObIVFMultiSliceReplayReader replay_reader;
  int64_t expected_row_count = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF build operator is not initialized", K(ret), K(tablet_id_));
  } else if (input_chunk.is_end_chunk()) {
    // The pipeline framework propagates the end chunk in try_execute_finish().
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("failed to get IVF build tablet context", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)
             || OB_ISNULL(tablet_context->ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF build tablet context", K(ret), K(tablet_id_), KP(tablet_context));
  } else if (tablet_context->ivf_ctx_->is_built_ || build_ctx_.is_ready_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF segment is already built", K(ret), K(tablet_id_));
  } else if (OB_FAIL(prepare_build_param_(build_param))) {
    LOG_WARN("failed to prepare IVF build param", K(ret), K(tablet_id_));
  } else if (OB_FAIL(build_ctx_.schema_ctx_.init(build_param))) {
    LOG_WARN("failed to initialize IVF schema context", K(ret));
  } else if (FALSE_IT(build_ctx_.refresh_monitor_ = tablet_context->ivf_ctx_->refresh_monitor_)) {
  } else if (OB_FAIL(prepare_slice_replay_reader_(*tablet_context, replay_reader))) {
    LOG_WARN("failed to prepare IVF capture slices", K(ret), K(tablet_id_));
  } else if (!dag_->get_ddl_task_param().is_async_index_refresh_
             && OB_FAIL(OB_E(ERRSIM_IVF_BASELINE_BUILD_FAILED) OB_SUCCESS)) {
    LOG_WARN("inject IVF baseline failure after capture", K(ret), K(tablet_id_));
  } else if (0 == replay_reader.get_row_count()) {
    // Empty refreshes still publish their window, but need neither training
    // nor a physical-row sorter. Pass the empty output to the write operator.
    if (nullptr != build_ctx_.refresh_monitor_) {
      build_ctx_.refresh_monitor_->set_total(ObIVFRefreshProgress::CAPTURE, 0);
    }
    build_ctx_.ivf_meta_.set_empty();
    build_ctx_.center_count_ = 0;
    build_ctx_.vector_count_ = 0;
    build_ctx_.posting_count_ = 0;
    if (OB_FAIL(tablet_context->ivf_ctx_->ivf_meta_.assign(
                   build_ctx_.allocator_, build_ctx_.ivf_meta_))) {
      LOG_WARN("failed to persist empty IVF meta in tablet context", K(ret));
    } else {
      build_ctx_.is_ready_ = true;
      output_chunk = input_chunk;
      LOG_INFO("IVF build operator completed empty refresh", K(tablet_id_));
    }
  } else if (FALSE_IT(nullptr != build_ctx_.refresh_monitor_
      ? (build_ctx_.refresh_monitor_->set_total(ObIVFRefreshProgress::CAPTURE, replay_reader.get_row_count()),
         build_ctx_.refresh_monitor_->begin_stage(ObIVFRefreshProgress::SAMPLE_MERGE), 0) : 0)) {
  } else if (OB_FAIL(collect_sample_refs_(*tablet_context, replay_reader))) {
    LOG_WARN("failed to merge captured IVF samples", K(ret),
             K(tablet_id_));
  } else if (OB_FAIL(build_ctx_.sorter_.init(MTL_ID()))) {
    LOG_WARN("failed to initialize IVF sorter", K(ret));
  } else if (OB_FAIL(ObDiskBBQAlgorithm::build(
                 MTL_ID(), build_ctx_.schema_ctx_, replay_reader,
                 tablet_context->ivf_ctx_->get_rotator(),
                 build_ctx_.sample_refs_,
                 build_ctx_.sorter_, build_ctx_.ivf_meta_,
                 build_ctx_.center_count_, build_ctx_.vector_count_,
                 build_ctx_.posting_count_, build_ctx_.refresh_monitor_))) {
    LOG_WARN("failed to build IVF physical rows", K(ret));
  } else if (FALSE_IT(nullptr != build_ctx_.refresh_monitor_
      ? (build_ctx_.refresh_monitor_->set_center_count(build_ctx_.center_count_),
         build_ctx_.refresh_monitor_->begin_stage(ObIVFRefreshProgress::SORT), 0) : 0)) {
  } else if (OB_FAIL(build_ctx_.sorter_.finish())) {
    LOG_WARN("failed to finish IVF sorter", K(ret));
  } else if (OB_FAIL(calc_ivf_physical_row_count(
                 build_ctx_.center_count_, build_ctx_.vector_count_, build_ctx_.posting_count_,
                 expected_row_count))) {
    LOG_WARN("failed to calculate IVF physical row count", K(ret),
             K(build_ctx_.center_count_), K(build_ctx_.vector_count_));
  } else if (OB_UNLIKELY(build_ctx_.sorter_.get_row_count()
                         != expected_row_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF physical row count violates disk contract", K(ret),
             K(build_ctx_.center_count_), K(build_ctx_.vector_count_), K(build_ctx_.posting_count_),
             K(expected_row_count),
             "row_count", build_ctx_.sorter_.get_row_count());
  } else if (OB_FAIL(tablet_context->ivf_ctx_->ivf_meta_.assign(
                 build_ctx_.allocator_, build_ctx_.ivf_meta_))) {
    LOG_WARN("failed to persist IVF meta in tablet context", K(ret));
  } else {
    build_ctx_.is_ready_ = true;
    output_chunk = input_chunk;
    LOG_INFO("IVF build operator completed", K(tablet_id_),
             K(build_ctx_.center_count_), K(build_ctx_.vector_count_), K(build_ctx_.posting_count_),
             "row_count", build_ctx_.sorter_.get_row_count());
  }
  if (OB_SUCC(ret) && build_ctx_.is_ready_ && nullptr != tablet_context) {
    // The sorted output now owns every payload. Independent DAG errors abort
    // the execution; Write does not restart Build from these capture stores.
    build_ctx_.sample_refs_.reset();
    replay_reader.reset();
    tablet_context->ivf_ctx_->release_capture_inputs();
  }
  return ret;
}

int ObIVFWriteMacroOperator::init(
    ObDDLIndependentDag *dag,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF write operator init twice", K(ret), K_(tablet_id));
  } else if (OB_ISNULL(dag) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF write operator argument", K(ret), KP(dag), K(tablet_id));
  } else {
    dag_ = dag;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFWriteMacroOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  ObIVFRefreshMonitor *progress = build_ctx_.refresh_monitor_;
  if (!input_chunk.is_end_chunk() && nullptr != progress) {
    progress->begin_stage(ObIVFRefreshProgress::WRITE);
    progress->set_total(ObIVFRefreshProgress::WRITE, build_ctx_.vector_count_);
  }
  int64_t written_vectors = 0;
  result_state = NEED_MORE_INPUT;
  output_chunk.reset();
  ObDDLTabletContext *tablet_context = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF write operator is not initialized", K(ret), K(tablet_id_));
  } else if (input_chunk.is_end_chunk()) {
    // The pipeline framework propagates the end chunk in try_execute_finish().
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("failed to get IVF write tablet context", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)
             || OB_ISNULL(tablet_context->ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF write tablet context", K(ret), K(tablet_id_), KP(tablet_context));
  } else if (!build_ctx_.is_ready_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF write operator ran before build completed", K(ret), K(tablet_id_));
  } else if (tablet_context->ivf_ctx_->is_built_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF tablet segment is already written", K(ret), K(tablet_id_));
  } else if (build_ctx_.ivf_meta_.is_empty()) {
    // Preserve macro-writer setup/close and the common incremental merge and
    // commit path without reading the uninitialized empty-output sorter.
    ObWriteMacroParam write_param;
    ObAsyncIndexMacroWriterAdapter writer;
    if (OB_UNLIKELY(0 != build_ctx_.center_count_
        || 0 != build_ctx_.vector_count_
        || 0 != build_ctx_.posting_count_
        || !tablet_context->ivf_ctx_->ivf_meta_.is_empty())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid empty IVF output state", K(ret), K(tablet_id_),
               K(build_ctx_.center_count_), K(build_ctx_.vector_count_));
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, 0, -1, dag_, write_param))) {
      LOG_WARN("failed to fill empty IVF writer param", K(ret));
    } else if (OB_FAIL(writer.init(write_param))) {
      LOG_WARN("failed to initialize empty IVF macro writer", K(ret));
    } else if (OB_FAIL(writer.close())) {
      LOG_WARN("failed to close empty IVF macro writer", K(ret));
    } else {
      tablet_context->ivf_ctx_->is_built_ = true;
      LOG_INFO("IVF write operator completed empty refresh", K(tablet_id_));
    }
  } else {
    ObWriteMacroParam write_param;
    ObAsyncIndexMacroWriterAdapter writer;
    ObDatumRow row;
    ObIvfSSTableRowDesc row_desc;
    const ObIVFSortItem *item = nullptr;
    const int64_t dim = build_ctx_.ivf_meta_.is_empty()
        ? tablet_context->ivf_ctx_->get_rotation_dim()
        : build_ctx_.ivf_meta_.dim_;
    const int32_t format_version = build_ctx_.ivf_meta_.is_empty()
        ? IVF_SSTABLE_FORMAT_VERSION
        : build_ctx_.ivf_meta_.format_version_;
    quantization::ObQuantizationParams quantization_params = {};
    int64_t vector_payload_size = 0;
    int64_t expected_code_size = 0;
    int64_t expected_row_count = 0;
    int64_t centroid_row_count = 0;
    int64_t low_precision_vector_row_count = 0;
    int64_t full_precision_vector_row_count = 0;
    const int64_t physical_column_count =
        build_ctx_.schema_ctx_.get_index_column_count()
        + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    const int64_t schema_rowkey_column_count =
        build_ctx_.schema_ctx_.get_index_rowkey_column_count();
    const int64_t trans_version_col_idx =
        ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
            schema_rowkey_column_count, true /*is_multi_version*/);
    const int64_t sql_sequence_col_idx =
        ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
            schema_rowkey_column_count, true /*is_multi_version*/);
    if (OB_UNLIKELY(dim <= 0 || dim > INT64_MAX / static_cast<int64_t>(sizeof(float)))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("invalid IVF vector payload dimension", K(ret), K(dim));
    } else if (FALSE_IT(vector_payload_size =
                 dim * static_cast<int64_t>(sizeof(float)))) {
    } else if (FALSE_IT(quantization_params.dim_ = dim)) {
    } else if (FALSE_IT(quantization_params.bits_ =
                 static_cast<int8_t>(build_ctx_.ivf_meta_.is_empty()
                     ? 1 : build_ctx_.ivf_meta_.nbits_))) {
    } else if (!build_ctx_.ivf_meta_.is_empty()
               && OB_FAIL(quantization::ObRaBitQQuantizer::calc_code_size(
                   quantization_params, expected_code_size))) {
      LOG_WARN("failed to calculate IVF RABITQ payload size", K(ret),
               K(dim), K(build_ctx_.ivf_meta_));
    } else if (OB_FAIL(calc_ivf_physical_row_count(
                   build_ctx_.center_count_, build_ctx_.vector_count_, build_ctx_.posting_count_,
                   expected_row_count))) {
      LOG_WARN("failed to calculate IVF writer row count", K(ret),
               K(build_ctx_.center_count_), K(build_ctx_.vector_count_));
    } else if (OB_FAIL(row_desc.init(format_version))) {
      LOG_WARN("failed to initialize IVF SSTable row descriptor", K(ret),
               K(build_ctx_.ivf_meta_));
    } else if (OB_UNLIKELY(!row_desc.is_valid_for_row(physical_column_count)
                           || row_desc.trans_version_col_idx_
                                != trans_version_col_idx
                           || row_desc.sql_sequence_col_idx_
                                != sql_sequence_col_idx
                           || trans_version_col_idx < 0
                           || trans_version_col_idx >= physical_column_count
                           || sql_sequence_col_idx < 0
                           || sql_sequence_col_idx >= physical_column_count)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("IVF physical row layout is invalid", K(ret), K(row_desc),
               K(physical_column_count), K(schema_rowkey_column_count),
               K(trans_version_col_idx), K(sql_sequence_col_idx));
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, 0, -1, dag_, write_param))) {
      LOG_WARN("failed to fill IVF writer param", K(ret));
    } else if (OB_FAIL(writer.init(write_param))) {
      LOG_WARN("failed to initialize IVF macro writer", K(ret));
    } else if (OB_FAIL(row.init(build_ctx_.allocator_, physical_column_count))) {
      LOG_WARN("failed to initialize IVF output row", K(ret));
    }
    int64_t center_self = 0, center_group = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(build_ctx_.sorter_.get_next_item(item))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("failed to get sorted IVF row", K(ret));
      } else if (OB_UNLIKELY(
          IVF_ROW_TYPE_CENTROID != item->row_type_
          && IVF_ROW_TYPE_LOW_PRECISION_VECTOR != item->row_type_
          && IVF_ROW_TYPE_RAW_VECTOR != item->row_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF producer emitted a reserved row type", K(ret), KPC(item));
      } else if (OB_UNLIKELY(
          (IVF_ROW_TYPE_CENTROID == item->row_type_
              && (OB_SUCCESS != build_ctx_.ivf_meta_.decode_centroid_key(
                  item->cid_, item->vid_, center_self, center_group)))
          || (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_
              && (item->cid_ < build_ctx_.ivf_meta_.leaf_cid_base()
                  || item->cid_ - build_ctx_.ivf_meta_.leaf_cid_base() >= build_ctx_.center_count_))
          || (IVF_ROW_TYPE_RAW_VECTOR == item->row_type_
              && IVF_RAW_VECTOR_CID != item->cid_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF row key violates disk contract", K(ret), KPC(item),
                 K(build_ctx_.center_count_));
      } else if (OB_UNLIKELY(
          (IVF_ROW_TYPE_CENTROID == item->row_type_
              && item->payload_.length() != ObIvfCentroidPayload::get_size(dim))
          || (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_
              && item->payload_.length() != expected_code_size)
          || (IVF_ROW_TYPE_RAW_VECTOR == item->row_type_
              && item->payload_.length() != vector_payload_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF payload length violates disk contract", K(ret),
                 KPC(item), K(expected_code_size),
                 K(dim), K(vector_payload_size));
      } else {
        row.row_flag_.reset();
        row.mvcc_row_flag_.reset();
        row.storage_datums_[row_desc.row_type_col_idx_].set_int(item->row_type_);
        row.storage_datums_[row_desc.centroid_id_col_idx_].set_int(item->cid_);
        row.storage_datums_[row_desc.vector_id_col_idx_].set_uint(item->vid_);
        // TODO @lyl512932: confirm whether incremental IVF trans_version should
        // use the source ORA_ROWSCN instead of the refresh snapshot version.
        row.storage_datums_[trans_version_col_idx].set_int(
            -dag_->get_ddl_task_param().snapshot_version_);
        row.storage_datums_[sql_sequence_col_idx].set_int(0);
        row.storage_datums_[row_desc.payload_col_idx_].set_string(item->payload_);
        row.count_ = physical_column_count;
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        row.mvcc_row_flag_.set_last_multi_version_row(true);
        if (dag_->get_ddl_task_param().is_async_index_refresh_
            && 1 == writer.get_row_count()
            && OB_FAIL(EVENT_CALL(EN_ASYNC_IVF_REFRESH_WRITE_FAIL, tablet_id_.id()))) {
          LOG_WARN("injected IVF refresh failure after partial output", K(ret), K(tablet_id_));
        } else if (OB_FAIL(writer.append_row(row))) {
          LOG_WARN("failed to append IVF output row", K(ret), KPC(item));
        } else if (IVF_ROW_TYPE_CENTROID == item->row_type_) {
          ++centroid_row_count;
        } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_) {
          ++low_precision_vector_row_count;
        } else {
          ++full_precision_vector_row_count;
          ++written_vectors;
          if (nullptr != progress && 0 == written_vectors % 1024) {
            progress->add_completed(ObIVFRefreshProgress::WRITE, 1024);
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(writer.close())) {
      LOG_WARN("failed to close IVF macro writer", K(ret));
    } else if (OB_SUCC(ret)
               && OB_UNLIKELY(centroid_row_count != build_ctx_.center_count_
                   || low_precision_vector_row_count != build_ctx_.posting_count_
                   || full_precision_vector_row_count != build_ctx_.vector_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF row type counts violate disk contract", K(ret),
               K(centroid_row_count), K(low_precision_vector_row_count),
               K(full_precision_vector_row_count), K(build_ctx_.center_count_),
               K(build_ctx_.vector_count_), K(build_ctx_.posting_count_));
    } else if (OB_SUCC(ret)
               && OB_UNLIKELY(writer.get_row_count() != expected_row_count
                   || build_ctx_.sorter_.get_row_count() != expected_row_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF writer row count violates disk contract", K(ret),
               "writer_row_count", writer.get_row_count(),
               "sorter_row_count", build_ctx_.sorter_.get_row_count(),
               K(expected_row_count));
    } else if (OB_SUCC(ret)) {
      tablet_context->ivf_ctx_->is_built_ = true;
      LOG_INFO("IVF write operator completed", K(tablet_id_),
               K(build_ctx_.center_count_), K(build_ctx_.vector_count_), K(build_ctx_.posting_count_),
               "row_count", writer.get_row_count());
    }
  }
  if (!input_chunk.is_end_chunk() && nullptr != progress) {
    progress->add_completed(ObIVFRefreshProgress::WRITE, written_vectors % 1024);
    if (OB_SUCC(ret)) { progress->begin_stage(ObIVFRefreshProgress::PUBLISH); }
  }
  return ret;
}

int ObIVFBuildAndWritePipeline::init(
    ObDDLIndependentDag *dag,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag) || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF build-and-write pipeline argument", K(ret), KP(dag), K(tablet_id));
  } else if (OB_FAIL(ObIDDLPipeline::init(tablet_id, 0))) {
    LOG_WARN("failed to initialize IVF pipeline base", K(ret), K(tablet_id));
  } else if (OB_FAIL(build_op_.init(dag, tablet_id))) {
    LOG_WARN("failed to initialize IVF build operator", K(ret), K(tablet_id));
  } else if (OB_FAIL(write_op_.init(dag, tablet_id))) {
    LOG_WARN("failed to initialize IVF write operator", K(ret), K(tablet_id));
  } else if (OB_FAIL(add_op(&build_op_))) {
    LOG_WARN("failed to add IVF build operator", K(ret));
  } else if (OB_FAIL(add_op(&write_op_))) {
    LOG_WARN("failed to add IVF write operator", K(ret));
  } else {
    dag_ = dag;
    tablet_id_ = tablet_id;
  }
  return ret;
}

ObITask::ObITaskPriority ObIVFBuildAndWritePipeline::get_priority()
{
  return nullptr != dag_ && dag_->is_scan_finished() && 0 == dag_->get_pipeline_count()
      ? TASK_PRIO_2 : TASK_PRIO_0;
}

int ObIVFBuildAndWritePipeline::get_next_chunk(ObChunk *&next_chunk)
{
  int ret = OB_SUCCESS;
  next_chunk = nullptr;
  if (OB_ISNULL(dag_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF build-and-write pipeline is not initialized", K(ret), K(tablet_id_));
  } else if (is_chunk_generated_) {
    chunk_.set_end_chunk();
    next_chunk = &chunk_;
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(dag_->get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("failed to get IVF pipeline tablet context", K(ret), K(tablet_id_));
    } else if (OB_ISNULL(tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF pipeline tablet context is null", K(ret), K(tablet_id_));
    } else {
      chunk_.type_ = ObChunk::DAG_TABLET_CONTEXT;
      chunk_.data_ptr_ = tablet_context;
      next_chunk = &chunk_;
      is_chunk_generated_ = true;
    }
  }
  return ret;
}

void ObIVFBuildAndWritePipeline::postprocess(int &ret_code)
{
  if (OB_ITER_END == ret_code) {
    ret_code = OB_SUCCESS;
  }
  if (OB_SUCCESS != ret_code && OB_NOT_NULL(dag_)) {
    dag_->set_ret_code(ret_code);
  }
}
