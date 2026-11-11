/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CAPTURE_PIPELINE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CAPTURE_PIPELINE_H_

#include "storage/ddl/ob_ddl_pipeline.h"

namespace oceanbase
{
namespace quantization
{
class ObFhtKacRotator;
}
namespace storage
{

struct ObIVFSliceInput;
class ObIVFRefreshMonitor;

class ObIVFCaptureOperator final : public ObPipelineOperator
{
public:
  explicit ObIVFCaptureOperator(ObPipeline *pipeline)
    : ObPipelineOperator(pipeline), is_inited_(false), vid_col_idx_(-1),
      data_col_idx_(-1), vector_dim_(0), is_cosine_(false),
      rotator_(nullptr), slice_input_(nullptr), refresh_monitor_(nullptr),
      row_allocator_("IVFRow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      vector_allocator_("IVFVec", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()) {}
  int init(ObDDLTabletContext &tablet_context, const ObDDLTableSchema &ddl_schema,
           const int64_t slice_idx, const bool is_async_index_refresh);
  // Borrow the batch only for this call. Replay owns all captured payloads.
  int consume_batch(const blocksstable::ObBatchDatumRows &batch,
                    const int64_t begin, const int64_t end);
  int seal();
  virtual bool is_valid() const override
  { return is_inited_ && vector_dim_ > 0
      && nullptr != rotator_ && nullptr != slice_input_; }
  TO_STRING_KV(K_(is_inited), K_(vid_col_idx), K_(data_col_idx),
               K_(vector_dim), K_(is_cosine), KP_(rotator), KP_(slice_input));
protected:
  virtual int execute(const ObChunk &input_chunk, ResultState &result_state,
                      ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk, ResultState &result_state,
                                 ObChunk &output_chunk) override;
private:
  int consume_batch_(const blocksstable::ObBatchDatumRows &batch);
  bool is_inited_;
  int64_t vid_col_idx_;
  int64_t data_col_idx_;
  int64_t vector_dim_;
  bool is_cosine_;
  const quantization::ObFhtKacRotator *rotator_;
  ObIVFSliceInput *slice_input_;
  ObIVFRefreshMonitor *refresh_monitor_; // borrowed from the owning tablet context
  common::ObArenaAllocator row_allocator_;
  common::ObArenaAllocator vector_allocator_;
};

class ObIVFCapturePipeline final : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObIVFCapturePipeline()
    : ObDDLWriteMacroBlockBasePipeline(TASK_TYPE_DDL_WRITE_USING_TMP_FILE_PIPELINE),
      capture_op_(this) {}
  int init(ObDDLSlice *ddl_slice, ObDDLTabletContext *tablet_context,
           const ObDDLTableSchema &ddl_schema, const bool is_async_index_refresh);
  virtual int set_remain_block() override;
  virtual void postprocess(int &ret_code) override;
private:
  ObIVFCaptureOperator capture_op_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_CAPTURE_PIPELINE_H_
