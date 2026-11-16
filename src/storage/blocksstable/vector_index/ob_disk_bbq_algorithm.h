/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_ALGORITHM_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_ALGORITHM_H_

#include "lib/container/ob_iarray.h"
#include "share/vector_index/ob_ivf_refresh_progress.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_struct.h"

namespace oceanbase
{
namespace storage
{
class ObIVFBuildSchemaCtx;
class ObIVFReplayInput;
class ObIVFReplayReader;
struct ObIVFSampleRef;
}
namespace blocksstable
{
struct ObIvfSSTableMeta;
}
namespace quantization
{
class ObFhtKacRotator;
class ObQuantizationEncoder;
}
namespace storage
{

class ObDiskBBQAlgorithm final
{
public:
  static int build(const uint64_t tenant_id,
                   const ObIVFBuildSchemaCtx &schema_ctx,
                   ObIVFReplayReader &replay_input,
                   const quantization::ObFhtKacRotator &rotator,
                   const common::ObIArray<ObIVFSampleRef> &sample_refs,
                   ObIVFBuildRowSink &row_sink,
                   blocksstable::ObIvfSSTableMeta &ivf_meta,
                   int64_t &effective_center_count,
                   int64_t &valid_vector_count,
                   int64_t &posting_count,
                   share::ObIVFRefreshProgress *progress = nullptr);

  // Replay vectors and caller-provided centers are already in the index's
  // rotated coordinate space. Keeping this boundary independent from the
  // trainer supports both flat centers and ObSingleKmeansExecutor output.
  // Each vector emits one RAW and one primary posting, plus a SOAR secondary
  // when its primary residual and another center admit one. posting_count is
  // the actual number emitted, including secondary postings.
  // Refresh always writes a FLAT layout; later merge/rebuild decides whether
  // to construct parent centers.
  static int build_rows_with_centers(
      const uint64_t tenant_id,
      const int64_t dim,
      const share::ObVectorIndexDistAlgorithm distance,
      const common::ObIArray<float *> &centers,
      ObIVFReplayReader &replay_input,
      quantization::ObQuantizationEncoder &encoder,
      ObIVFBuildRowSink &row_sink,
      int64_t &materialized_vector_count,
      int64_t &posting_count,
      share::ObIVFRefreshProgress *progress = nullptr);

private:
  // Small refresh segments share one center to amortize per-cluster query
  // preparation. Keep the threshold internal until workload tuning is needed.
  static bool use_flat_centroid(const int64_t vector_count, const int64_t cluster_size)
  {
    return cluster_size >= share::ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE
        && cluster_size <= share::ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE
        && vector_count > 0 && vector_count <= 3 * cluster_size;
  }

  static int build_flat_center(ObIVFReplayReader &replay_input,
                              const share::ObVectorIndexDistAlgorithm distance,
                              common::ObIAllocator &allocator,
                              float *&center,
                              share::ObIVFRefreshProgress *progress);
  static int assign_one(const common::ObIArray<float *> &centers,
                        const int64_t dim,
                        const share::ObVectorIndexDistAlgorithm distance,
                        const float *vector,
                        common::ObIAllocator &scratch_allocator,
                        int64_t &cid,
                        int64_t &secondary_cid);
  static int validate_finite_vector_(const float *vector, const int64_t dim);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_ALGORITHM_H_
