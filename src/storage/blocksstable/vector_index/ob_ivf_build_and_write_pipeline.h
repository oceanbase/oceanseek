/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_AND_WRITE_PIPELINE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_AND_WRITE_PIPELINE_H_

#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_schema_ctx.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

namespace oceanbase
{
namespace storage
{

class ObDDLIndependentDag;
class ObIVFRefreshMonitor;

// State shared by the build and write operators in one Tablet/window pipeline.
// Its allocator must outlive the external sorter because sorted payloads may
// spill and are consumed by the following write operator.
struct ObIVFBuildContext final
{
  ObIVFBuildContext()
    : allocator_(common::ObMemAttr(MTL_ID(), "IVFBuild")),
      schema_ctx_(allocator_), sample_refs_(), sorter_(), ivf_meta_(),
      center_count_(0), vector_count_(0), is_ready_(false), refresh_monitor_(nullptr)
  {}

  common::ObArenaAllocator allocator_;
  ObIVFBuildSchemaCtx schema_ctx_;
  common::ObSEArray<ObIVFSampleRef, 64> sample_refs_;
  ObIVFSorter sorter_;
  blocksstable::ObIvfSSTableMeta ivf_meta_;
  int64_t center_count_;
  int64_t vector_count_;
  bool is_ready_;
  ObIVFRefreshMonitor *refresh_monitor_; // borrowed from tablet context
};

// Stage 2a: after every capture pipeline is sealed, stream the slice replay
// stores as one logical input and train exactly one segment model. The sorted
// type-1/type-3/type-5 rows are retained in build_ctx_ for the following write
// operator.
class ObIVFBuildOperator final : public ObPipelineOperator
{
public:
  ObIVFBuildOperator(ObPipeline *pipeline, ObIVFBuildContext &build_ctx)
    : ObPipelineOperator(pipeline), is_inited_(false), dag_(nullptr),
      tablet_id_(), build_ctx_(build_ctx)
  {}
  int init(ObDDLIndependentDag *dag, const common::ObTabletID &tablet_id);
  virtual bool is_valid() const override
  { return is_inited_ && nullptr != dag_ && tablet_id_.is_valid(); }
  TO_STRING_KV(K_(is_inited), K_(tablet_id),
               "center_count", build_ctx_.center_count_,
               "vector_count", build_ctx_.vector_count_,
               "is_ready", build_ctx_.is_ready_);
protected:
  virtual int execute(const ObChunk &input_chunk, ResultState &result_state,
                      ObChunk &output_chunk) override;
private:
  int prepare_build_param_(ObIVFBuildParam &param);
  int prepare_slice_replay_reader_(ObDDLTabletContext &tablet_context,
                                   ObIVFMultiSliceReplayReader &replay_reader);
  int collect_sample_refs_(ObDDLTabletContext &tablet_context,
                           const ObIVFReplayReader &replay_reader);
private:
  bool is_inited_;
  ObDDLIndependentDag *dag_;
  common::ObTabletID tablet_id_;
  ObIVFBuildContext &build_ctx_;
};

// Stage 2b: consume the already sorted physical rows and write ordinary
// row-store IVF data micro-blocks through ObIvfMacroBlockWriter.
class ObIVFWriteMacroOperator final : public ObPipelineOperator
{
public:
  ObIVFWriteMacroOperator(ObPipeline *pipeline, ObIVFBuildContext &build_ctx)
    : ObPipelineOperator(pipeline), is_inited_(false), dag_(nullptr),
      tablet_id_(), build_ctx_(build_ctx)
  {}
  int init(ObDDLIndependentDag *dag, const common::ObTabletID &tablet_id);
  virtual bool is_valid() const override
  { return is_inited_ && nullptr != dag_ && tablet_id_.is_valid(); }
  TO_STRING_KV(K_(is_inited), K_(tablet_id),
               "center_count", build_ctx_.center_count_,
               "vector_count", build_ctx_.vector_count_,
               "is_ready", build_ctx_.is_ready_);
protected:
  virtual int execute(const ObChunk &input_chunk, ResultState &result_state,
                      ObChunk &output_chunk) override;
private:
  bool is_inited_;
  ObDDLIndependentDag *dag_;
  common::ObTabletID tablet_id_;
  ObIVFBuildContext &build_ctx_;
};

// The second IVF DDL pipeline. It mirrors the existing vector DDL
// build-and-write pipeline shape while preserving IVF's one-segment,
// replay, RABITQ, sorter and dedicated IVF writer contracts.
class ObIVFBuildAndWritePipeline final : public ObIDDLPipeline
{
public:
  ObIVFBuildAndWritePipeline()
    : ObIDDLPipeline(TASK_TYPE_DDL_VECTOR_INDEX_BUILD_AND_WRITE_PIPELINE),
      is_chunk_generated_(false), dag_(nullptr), tablet_id_(), build_ctx_(),
      build_op_(this, build_ctx_), write_op_(this, build_ctx_), chunk_()
  {}
  int init(ObDDLIndependentDag *dag, const common::ObTabletID &tablet_id);
  virtual ObITaskPriority get_priority() override;
protected:
  virtual int get_next_chunk(ObChunk *&next_chunk) override;
  virtual void postprocess(int &ret_code) override;
private:
  bool is_chunk_generated_;
  ObDDLIndependentDag *dag_;
  common::ObTabletID tablet_id_;
  ObIVFBuildContext build_ctx_;
  ObIVFBuildOperator build_op_;
  ObIVFWriteMacroOperator write_op_;
  ObChunk chunk_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_AND_WRITE_PIPELINE_H_
