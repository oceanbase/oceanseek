/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_HCA_ALGORITHM_ADAPTER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_HCA_ALGORITHM_ADAPTER_H_

#include "lib/vector/ob_vsag_adaptor.h"
#include "lib/allocator/page_arena.h"
#include "lib/vector/ob_vsag_adaptor.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "vsag/allocator.h"

namespace oceanbase
{
namespace storage
{


// Numerical parameters shared by the three IVF build strategies.
struct ObIVFHcaAlgorithmParam
{
  ObIVFHcaAlgorithmParam();
  bool is_valid() const;

  share::ObHcaTrainParam train_param_;
  share::ObHcaNeighborhoodParam neighborhood_param_;
  share::ObHcaRefineParam refine_param_;
  share::ObHcaSoarParam soar_param_;
  // Optional and borrowed for the adapter lifetime. Cosine input adapters
  // normalize vectors before the DAG and provide this for center updates.
  const share::ObVectorNormalizeInfo *center_norm_info_;
  int64_t hgraph_m_;
  int64_t hgraph_ef_construction_;
};

// Adapts the shared IVF build operations to the upper-layer-owned stores used
// by the HCA implementation. The adapter owns only temporary numerical state
// (the HGraph between Neighborhood begin/finish); Planner and Processor retain
// ownership of every durable center, assignment, member row and graph row.
class ObIVFRefineSession;

class ObIVFHcaAlgorithmAdapter : public ObIVFMergeAlgorithm
{
public:
  ObIVFHcaAlgorithmAdapter();
  virtual ~ObIVFHcaAlgorithmAdapter();

  // hgraph_allocator may be null when the center count never selects HGraph.
  // Its lifetime must cover this adapter when HGraph is used.
  int init(
      const ObIVFHcaAlgorithmParam &param,
      vsag::Allocator *hgraph_allocator = nullptr);

  // Bind the dominant SSTable's immutable leaves. Its centroid iterator owns
  // all cache/storage decisions; parallel assignment tasks borrow this binding.
  void prepare_insertion_source(const ObIVFCentroidSource &source, const ObIVFMergePrior &prior,
      share::ObVectorIndexDistAlgorithm distance, int64_t leaf_cid_base);
  void finish_insertion_assign() override;
  int get_parent_cluster_params(ObIVFHcaAlgorithmParam &param) const override
  { param = param_; return OB_SUCCESS; }

  virtual int check_cluster_quality(const common::ObIArray<int64_t> &counts, bool &degenerate) override;
  virtual int insertion_train_centers(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const ObIVFFirstLevelSampleSet &first_level_samples,
      const common::ObIArray<float> &initial_centers, const int64_t center_count,
      common::ObIArray<float> &trained_centers) override;

  virtual int insertion_assign_vectors(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &offsets,
      const common::ObIArray<int64_t> &candidates,
      int64_t begin, int64_t end, common::ObIArray<int64_t> &assignments) override;

  virtual bool supports_prepared_cluster() const override { return true; }
  virtual int hca_collect_training_rows(const int64_t vector_count,
      const int64_t cluster_size, common::ObIArray<int64_t> &ordinals) override;
  virtual int hca_train_loaded_samples(const int64_t vector_count,
      const int64_t cluster_size, const int64_t dimension,
      const common::ObIArray<int64_t> &ordinals,
      const common::ObIArray<float> &vectors, int64_t &local_k,
      common::ObIArray<float> &centers) override;

  virtual int hca_train_local_centers(
      const int64_t depth,
      const int64_t node_idx,
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &node_vids,
      const ObIVFFirstLevelSampleSet *first_level_samples,
      const int64_t cluster_size,
      int64_t &local_k,
      common::ObIArray<float> &local_centers) override;
  virtual int hca_assign_vectors(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &node_vids,
      const common::ObIArray<float> &local_centers,
      const int64_t local_k,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &local_center_idxs) override;
  virtual bool supports_loaded_assign() const override { return true; }
  virtual int hca_assign_loaded_vectors(
      const common::ObIArray<float> &vectors,
      const common::ObIArray<int64_t> &positions,
      const int64_t begin, const int64_t end, const int64_t dimension,
      const common::ObIArray<float> &centers, const int64_t local_k,
      common::ObIArray<int64_t> &assignments) override;
  virtual bool supports_source_update() const override { return true; }
  virtual void hca_accumulate_center_stats(float *sum, const float *vector,
      const int64_t dimension) override;
  virtual int hca_finalize_center_stats(const float *sum, const int64_t count,
      const int64_t dimension, common::ObIArray<float> &center) override;
  virtual int hca_compute_local_center_stats_range(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &member_vids,
      const common::ObIArray<float> &local_centers,
      const int64_t local_center_idx,
      common::ObIArray<float> &updated_center) override;

  virtual int neighborhood_begin(
      const ObIVFNeighborhoodMode mode,
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension) override;
  virtual int neighborhood_add_centers(
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension,
      const int64_t cid_begin,
      const int64_t cid_end) override;
  virtual int neighborhood_finish_add() override;
  virtual int neighborhood_find_neighbors(
      const ObIVFNeighborhoodMode mode,
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension,
      const int64_t cid_begin,
      const int64_t cid_end,
      common::ObIArray<int64_t> &neighbor_offsets,
      common::ObIArray<int64_t> &neighbor_cids,
      common::ObIArray<float> &max_intra_distances) override;
  virtual int neighborhood_finish() override;

  virtual int refine_train(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      common::ObIArray<float> &working_centers) override;
  bool supports_parallel_refine() const override { return true; }
  int refine_train_begin(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t center_count,
      common::ObIArray<float> &working_centers) override;
  int64_t refine_train_task_count() const override;
  int refine_train_distance_task(int64_t task_idx) override;
  int refine_train_finish_batch(common::ObIArray<float> &centers, bool &finished) override;
  virtual int refine_assign_vectors(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &working_centers,
      const common::ObIArray<int64_t> &old_assignments,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &new_cids) override;
  virtual int refine_update_center(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &member_vids,
      const common::ObIArray<float> &working_centers,
      const int64_t center_count,
      const int64_t cid,
      common::ObIArray<float> &updated_center) override;
  virtual int soar_assign_secondary(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &final_centers,
      const common::ObIArray<int64_t> &primary_cids,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &secondary_cids) override;

  virtual void cancel(const int ret_code) override;

protected:
  bool is_initialized() const { return is_inited_; }
  const ObIVFHcaAlgorithmParam &parameters() const { return param_; }

  // DDL uses borrowed centers and per-task rows; the shared HGraph lifetime and
  // numerical kernels remain owned here. Ordinary merge keeps its existing path.
  int neighborhood_add_center_view(const share::ObHcaCenterStore &centers,
      int64_t cid_begin, int64_t cid_end);
  int neighborhood_build_row_view(ObIVFNeighborhoodMode mode,
      const share::ObHcaCenterStore &centers, int64_t cid_begin, int64_t cid_end,
      share::ObHcaNeighborhoodStore &rows);

private:
  void release_hgraph();
  void release_refine();

  common::ObArenaAllocator refine_allocator_;
  volatile int refine_cancel_code_;
  ObIVFRefineSession *refine_session_;
  bool is_inited_;
  ObIVFHcaAlgorithmParam param_;
  vsag::Allocator *hgraph_allocator_;
  common::obvsag::VectorIndexPtr hgraph_;
  ObIVFNeighborhoodMode neighborhood_mode_;
  int64_t hgraph_center_count_;
  int64_t hgraph_dimension_;
  struct InsertionCentroidSource
  {
    InsertionCentroidSource() : source_(nullptr), segment_(-1), count_(0), dim_(0) {}
    void reset() { source_ = nullptr; segment_ = -1; count_ = dim_ = 0; }
    bool matches(int64_t count, int64_t dim) const
    { return source_ != nullptr && count_ == count && dim_ == dim; }
    const ObIVFCentroidSource *source_; // Borrowed until the root assignment barrier.
    int64_t segment_;
    int64_t count_;
    int64_t dim_;
  } insertion_centroids_;
  int64_t insertion_source_queries_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFHcaAlgorithmAdapter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_HCA_ALGORITHM_ADAPTER_H_
