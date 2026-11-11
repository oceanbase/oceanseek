/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_PARENT_CENTROID_BUILD_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_PARENT_CENTROID_BUILD_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"

namespace oceanbase
{
namespace storage
{
struct ObIVFHcaAlgorithmParam;

// Reuses the full-build clustering state machine and processor over borrowed
// leaf-center memory. The owning DAG schedules every returned task count and
// joins each plan before finish(); this builder stops at the SOAR barrier.
class ObIVFParentCentroidBuilder final
{
public:
  explicit ObIVFParentCentroidBuilder(common::ObIAllocator &allocator);
  ~ObIVFParentCentroidBuilder();
  int init(const common::ObIArrayWrap<float> &leaves, int64_t dimension,
      const ObIVFHcaAlgorithmParam &param, int64_t cluster_size);
  int prepare(int64_t plan_id, int64_t &task_count);
  int get_next_operation(ObIVFMergeStep &step, int64_t &depth) const;
  ObIVFRefinePhase get_refine_phase() const;
  int execute(int64_t task_idx);
  int finish(int64_t plan_id, blocksstable::ObIvfCentroidLayout &layout, bool &finished);
  int get_work_perf(int64_t task_idx, ObIVFMergeWorkPerf &perf) const;
  ObIVFMergeStage get_stage() const;
  ObIVFMergeStep get_step() const;
  int64_t get_depth() const;
private:
  struct State;
  common::ObIAllocator &allocator_;
  State *state_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFParentCentroidBuilder);
};
} // namespace storage
} // namespace oceanbase
#endif
