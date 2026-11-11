/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_CLUSTER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_CLUSTER_H_

#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "lib/random/ob_random.h"
#include "lib/container/ob_array.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace share {

/**
 * Train provisional centers for one hierarchy node with Balanced OT.
 * The method is serial; node-level parallelism is controlled by the upper
 * layer. allocator must be task-local. Optional training_samples supply the
 * captured root sample pool; K and the single-center mean still use vectors.
 */
int hca_train_local_centers(const ObHcaVectorBatch &vectors,
                            int64_t cluster_size,
                            const ObHcaTrainParam &param,
                            const ObVectorNormalizeInfo *center_norm_info,
                            ObHcaCenterStore &provisional_centers,
                            int64_t &provisional_center_count,
                            common::ObIAllocator &allocator,
                            const ObHcaVectorBatch *training_samples = nullptr);

// Plan the exact cold-start reservoirs without loading payloads. Center
// ordinals retain CID order; sample ordinals retain reservoir-slot order.
int hca_plan_local_training(int64_t vector_count, int64_t cluster_size,
    const ObHcaTrainParam &param, common::ObIArray<int64_t> &center_ordinals,
    common::ObIArray<int64_t> &sample_ordinals);

// The sparse batch still exposes the original node count/ordinals. Only the
// planned samples and final initial centers need payloads; no resampling of a
// reduced data set is permitted.
int hca_train_prepared_centers(const ObHcaVectorBatch &vectors,
    int64_t cluster_size, const ObHcaTrainParam &param,
    const ObVectorNormalizeInfo *center_norm_info, ObHcaCenterStore &centers,
    int64_t &center_count, common::ObIAllocator &allocator);

/** Select sorted, distinct ordinals without reading vector payloads. */
int hca_sample_ordinals(int64_t vector_count, int64_t sample_count,
    uint64_t seed, int64_t *ordinals);

/** Warm-start BalancedOT over the caller-provided training vector set.
 * Insertion's adapter supplies min(N, K*samples_per_cluster) sampled vectors.
 * The caller controls iterations (Insertion uses one) and performs final hard
 * assignment/recompute over the complete merge input separately.
 */
int hca_train_warm_centers(const ObHcaVectorBatch &vectors,
    const ObHcaCenterStore &initial_centers, const ObHcaTrainParam &param,
    const ObVectorNormalizeInfo *center_norm_info, ObHcaCenterStore &output_centers,
    common::ObIAllocator &allocator);

/**
 * Assign one externally partitioned vector batch to provisional local centers.
 * Writes both vector_idx-to-cid and cid-to-vector_idx mappings.
 */
int hca_assign_vectors(const ObHcaVectorBatch &vectors,
                       const ObHcaCenterStore &centers,
                       const int32_t *candidate_cids,
                       int64_t candidate_count,
                       ObHcaAssignmentStore &assignments,
                       ObHcaCidMemberStore &members);

static const int64_t OB_HCA_ASSIGN_BATCH_SIZE = 32;
// Bounded exact argmin. Null candidates means centers [0, candidate_count).
// Candidate order breaks ties; no vector_count x center_count matrix is kept.
int hca_find_nearest_centers(const float *const *vectors, int64_t vector_count,
    const ObHcaCenterStore &centers, const int32_t *candidate_cids,
    int64_t candidate_count, int64_t *nearest_cids);

/** Compute exact full-data sum/count statistics for a disjoint local-cid range.
 */
int hca_compute_local_center_stats_range(
    const ObHcaVectorAccessor &vectors,
    const ObHcaCidMemberStore &members,
    int64_t begin_cid,
    int64_t end_cid,
    int64_t dimension,
    ObHcaCenterStatsStore &global_stats,
    common::ObIAllocator &allocator);

/**
 * Convert full-data statistics to mean centers and compact empty local cids.
 * Returns NO_PROGRESS when no meaningful child split was produced.
 */
int hca_finalize_local_centers(
    const ObHcaCenterStatsStore &global_stats,
    int64_t old_center_count,
    int64_t dimension,
    const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &final_centers,
    ObHcaAssignmentStore &assignments,
    ObHcaCidMemberStore &member_lists,
    ObHcaCidRemapStore &old_to_new,
    int64_t &final_center_count,
    ObHcaSplitStatus &split_status);

/** Delegate atomic C0/A0/InitialMemberLists publication to the upper layer. */
int hca_publish_leaf_result(const float *leaf_center,
                            int64_t dimension,
                            const ObHcaCidMemberStore &leaf_members,
                            int64_t leaf_cid,
                            ObHcaLeafResultWriter &result_writer,
                            int32_t &global_cid);

// Reusable prior-centroid initialization and quality checks.
struct ObHcaClusterQuality
{
  ObHcaClusterQuality() : total_(0), maximum_(0), empty_(0), z_score_(0), degenerate_(false) {}
  int64_t total_;
  int64_t maximum_;
  int64_t empty_;
  double z_score_;
  bool degenerate_;
  TO_STRING_KV(K_(total), K_(maximum), K_(empty), K_(z_score), K_(degenerate));
};
int hca_check_cluster_quality(const int64_t *counts, int64_t count, ObHcaClusterQuality &quality);
int hca_check_prior_quality(const int64_t *mass, int64_t prior_count, int64_t k, bool &degenerate);
int hca_calculate_reuse_k(int64_t vector_count, int64_t cluster_size, bool insertion,
    const int64_t *mass, int64_t prior_count, int64_t covered_vector_count, int64_t &k);

// Pure numerical kernels. Input ranges and output ownership are supplied by
// the caller; these functions have no scheduler/task/phase dependency.
int64_t hca_choose_weighted(const double *weights, int64_t count, double total, common::ObRandom &random);
int hca_initialize_prior_centers(const float *prior, const int64_t *mass, int64_t prior_count,
    int64_t dimension, int64_t k, bool insertion, common::ObRandom &random,
    float *centers, int64_t &selected_count, common::ObIAllocator &allocator);
int hca_sample_prior_supplement(const ObHcaVectorBatch &vectors, int64_t sample_count,
    common::ObRandom &random, float *samples, common::ObIAllocator &allocator);
int hca_update_prior_weights(const float *prior, const int64_t *mass, int64_t count,
    int64_t dimension, const float *representative, float *minimum, double *weights, double &total);
int hca_compute_prior_distances(const float *prior, int64_t count, int64_t dimension,
    const float *centers, int64_t k, float *distances);
int hca_update_supplement_weights(const float *samples, int64_t count, int64_t dimension,
    const float *centers, int64_t center_count, const int64_t *mass,
    float *minimum, double *weights, double &total);
int hca_reduce_prior_centers(const float *prior, const int64_t *mass, int64_t count,
    int64_t dimension, int64_t k, const float *distances, float *centers, common::ObIAllocator &allocator);

int hca_insertion_assign_priors(const float *prior, int64_t count, int64_t dimension,
    const float *centers, int64_t k, float *sums, int64_t *counts);
int hca_insertion_update_centers(const float *sums, const int64_t *counts, int64_t k,
    int64_t dimension, const ObVectorNormalizeInfo *norm_info, float *centers);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_CLUSTER_H_
