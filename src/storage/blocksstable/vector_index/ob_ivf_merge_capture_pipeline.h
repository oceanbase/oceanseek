/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_CAPTURE_PIPELINE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_CAPTURE_PIPELINE_H_

#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/ddl/ob_pipeline.h"

namespace oceanbase
{
namespace compaction
{

struct ObIVFMergeCaptureRange
{
  ObIVFMergeCaptureRange() : table_idx_(-1), range_(), scanned_count_(0), raw_count_(0), live_count_(0) {}
  int64_t table_idx_;
  blocksstable::ObDatumRange range_;
  int64_t scanned_count_;
  int64_t raw_count_;
  int64_t live_count_;
  TO_STRING_KV(K_(table_idx), K_(range), K_(scanned_count), K_(raw_count), K_(live_count));
};

// Each slice owns mutable scan output and sampling memory. Input tables and
// the deletion bitmap belong to the owner merge context and remain read-only.
struct ObIVFMergeCaptureSlice
{
  ObIVFMergeCaptureSlice();
  common::ObArenaAllocator allocator_;
  common::ObArray<ObIVFMergeCaptureRange> ranges_;
  common::ObArray<ObAsyncIVFSourceLocator> locators_;
  storage::ObIVFSampleCandidateMaxCmp sample_cmp_;
  storage::ObIVFSampleHeap samples_;
  // Non-owning: the input adapter retains sealed stores after capture finishes.
  storage::ObIVFReplayInput *replay_;
  int64_t slice_idx_;
  bool finished_;
  TO_STRING_KV(K_(ranges), "vector_count", locators_.count(), K_(finished));
};

// The scan plan is separate from the merge output ranges. It borrows the
// common compaction parallelism policy without changing output macro sequences.
class ObIVFMergeCaptureCtx final
{
public:
  ObIVFMergeCaptureCtx();
  ~ObIVFMergeCaptureCtx();
  int init(ObAsyncIndexMergeCtx &merge_ctx, ObIVFMergeInputAdapter &input, const bool parallel = true);
  int64_t get_task_count() const { return slices_.count(); }
  int scan(const int64_t task_idx);
  // Called only after every scan task has finished successfully. The adapter
  // owns the sealed input; this context retains no role in subsequent building.
  int finish();
private:
  int plan_(const bool parallel);
  int scan_range_(ObIVFMergeCaptureSlice &slice, ObIVFMergeCaptureRange &range);
  void release_slices_();
  common::ObArenaAllocator allocator_;
  common::ObArray<ObIVFMergeCaptureSlice *> slices_;
  ObAsyncIndexMergeCtx *merge_ctx_;
  ObIVFMergeInputAdapter *input_;
  bool is_inited_;
  bool is_finished_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFMergeCaptureCtx);
};

class ObIVFMergeCapturePipeline final : public storage::ObPipeline
{
public:
  ObIVFMergeCapturePipeline()
    : ObPipeline(TASK_TYPE_MACROMERGE), capture_(nullptr), task_idx_(-1) {}
  int init(ObIVFMergeCaptureCtx *capture, const int64_t task_idx);
  virtual int64_t get_sub_task_id() const override { return task_idx_; }
protected:
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;
private:
  ObIVFMergeCaptureCtx *capture_;
  int64_t task_idx_;
};

} // namespace compaction
} // namespace oceanbase
#endif
