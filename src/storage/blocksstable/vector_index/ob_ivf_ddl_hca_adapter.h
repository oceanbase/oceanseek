/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_HCA_ADAPTER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_HCA_ADAPTER_H_

#include "storage/blocksstable/vector_index/ob_ivf_vector_ids.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"

namespace oceanbase
{
namespace storage
{
struct ObIVFDDLRefineSession;

// Source-ordered builds share this I/O/cache policy across DDL and merge.
// Numerical kernels remain shared with the ordinary adapter.
class ObIVFDDLHcaAdapter : public ObIVFHcaAlgorithmAdapter
{
public:
  ObIVFDDLHcaAdapter();
  ~ObIVFDDLHcaAdapter() override;
  int neighborhood_add_centers(const share::ObVectorMatrix &centers,
      int64_t center_count, int64_t dimension, int64_t cid_begin, int64_t cid_end) override;
  int neighborhood_find_neighbors(ObIVFNeighborhoodMode mode,
      const share::ObVectorMatrix &centers, int64_t center_count, int64_t dimension,
      int64_t cid_begin, int64_t cid_end, common::ObIArray<int64_t> &offsets,
      common::ObIArray<int64_t> &cids, common::ObIArray<float> &distances) override;
  // A fully resident source supplies batches directly. Source and VID mapping
  // are borrowed until release_samples(), after all training workers drain.
  int begin_refine_samples(int64_t n, int64_t dim,
      const share::ObVectorMatrix &centers, const share::ObVectorArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t memory_limit = INT64_MAX, int64_t shards = 1,
      const ObIVFVectorStore *source = nullptr,
      ObIVFVectorIDView source_vids = ObIVFVectorIDView());
  bool uses_resident_refine_source() const;
  int64_t refine_sample_shards() const;
  int64_t refine_sample_memory_hold() const;
  int64_t refine_sample_file_size() const;
  int64_t refine_sample_read_rows() const;
  int begin_refine_sample_shard(int64_t shard);
  int finish_refine_sample_shard(int64_t shard);
  const share::ObVectorArray<int64_t> *refine_sample_indices() const;
  int load_refine_samples(const common::ObIArray<int64_t> &positions,
      const share::ObVectorMatrix &vectors, int64_t shard = 0);
  int finish_refine_samples();
  void release_samples(); // Workers must have drained before release.
  bool supports_batched_refine_train() const override { return true; }
  int refine_train_prepare(ObIVFVectorReader &reader,
      const ObIVFVectorIDView &vids, const share::ObVectorMatrix &centers,
      const share::ObVectorArray<int64_t> &assignments, const ObIVFNeighborGraphView *graph,
      int64_t k, int64_t &batch_size) override;
  int refine_train_distances(int64_t begin, int64_t end) override;
  int refine_train_finish_batch(share::ObVectorMatrix &centers, bool &finished) override;
  ObIVFRefinePhase refine_train_phase() const;
  int prepare_refine_phase(int64_t &count);
  int execute_refine_phase(int64_t begin, int64_t end);
  virtual int finish_refine_phase(share::ObVectorMatrix &centers, bool &finished);
  int refine_assign_loaded(const share::ObVectorMatrix &values,
      const share::ObVectorArray<int64_t> &indices, int64_t dim, const share::ObVectorMatrix &centers,
      const share::ObVectorArray<int64_t> &old_assignments, const ObIVFNeighborGraphView *graph,
      common::ObIArray<int64_t> &output);
  int soar_assign_loaded(const share::ObVectorMatrix &values, int64_t dim,
      const share::ObVectorMatrix &centers, const common::ObIArray<int64_t> &primary,
      const ObIVFNeighborGraphView *graph, common::ObIArray<int64_t> &secondary);
  void cancel(int ret_code) override;
private:
  common::ObArenaAllocator allocator_;
  ObIVFDDLRefineSession *session_;
  volatile int cancel_code_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLHcaAdapter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_HCA_ADAPTER_H_
