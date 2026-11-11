/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/hca/ob_vector_hca_cluster.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_sort.h"
#include "share/vector_index/hca/ob_vector_hca_internal.h"
#include "share/vector_index/hca/ob_vector_hca_simd.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include <cmath>
#include <cfloat>

namespace oceanbase {
namespace share {

using namespace hca_internal;

namespace {

// Convert the user target cluster size to the bounded local fan-out used by
// one hierarchy node. A node at or below the target becomes one leaf center.
int calculate_local_k(const int64_t vector_count, const int64_t cluster_size,
                      const int64_t max_local_k, int64_t &local_k) {
  int ret = OB_SUCCESS;
  if (vector_count <= 0 || cluster_size <= 0 || max_local_k <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid local hca k argument", K(ret), K(vector_count), K(cluster_size), K(max_local_k));
  } else if (vector_count <= cluster_size) {
    local_k = 1;
  } else {
    const int64_t quotient = vector_count / cluster_size;
    const int64_t remainder = vector_count % cluster_size;
    const int64_t round_up_threshold = cluster_size / 2 + cluster_size % 2;
    local_k = quotient + (remainder >= round_up_threshold ? 1 : 0);
    local_k = std::max<int64_t>(2, std::min<int64_t>(max_local_k, local_k));
  }
  return ret;
}

// Keep one owner for RNG consumption. The legacy path reads each reservoir
// replacement as before; preparation retains only the final center ordinals.
template <typename SelectCenter>
int select_training_ordinals(const int64_t vector_count, const int64_t local_k,
    const int64_t sample_count, const uint64_t seed, int64_t *sample_indices,
    SelectCenter select_center)
{
  int ret = OB_SUCCESS;
  common::ObRandom center_random;
  common::ObRandom sample_random;
  center_random.seed(seed);
  sample_random.seed(seed + 1);
  if (local_k <= 0 || sample_count < local_k || sample_count > vector_count
      || OB_ISNULL(sample_indices)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca sampling argument", K(ret), K(vector_count),
             K(local_k), K(sample_count), KP(sample_indices));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vector_count; ++i) {
    if (i < sample_count) {
      sample_indices[i] = i;
    } else {
      const int64_t selected_sample = sample_random.get(0, i);
      if (selected_sample < 0 || selected_sample > i) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hca reservoir sample index", K(ret), K(i),
                 K(selected_sample), K(sample_count), K(vector_count));
      } else if (selected_sample < sample_count) {
        sample_indices[selected_sample] = i;
      }
    }

    int64_t selected_cid = -1;
    if (OB_SUCC(ret)) {
      if (i < local_k) {
        selected_cid = i;
      } else if (random_unit_double(center_random) <
                 static_cast<double>(local_k) / static_cast<double>(i)) {
        selected_cid = center_random.get(0, local_k - 1);
        if (selected_cid < 0 || selected_cid >= local_k) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid hca reservoir center index", K(ret), K(i), K(selected_cid), K(local_k));
        }
      }
    }
    if (OB_SUCC(ret) && selected_cid >= 0) {
      ret = select_center(i, selected_cid);
    }
  }
  return ret;
}

int initialize_centers_and_sample_pool(
    const ObHcaVectorBatch &vectors, const int64_t local_k,
    const int64_t sample_count, const uint64_t seed,
    const ObVectorNormalizeInfo *center_norm_info, int64_t *sample_indices,
    float *centers, const bool prepared = false)
{
  int ret = OB_SUCCESS;
  const int64_t dimension = vectors.dimension();
  common::ObSEArray<int64_t, OB_HCA_MAX_LOCAL_K> ordinals;
  auto load_center = [&](int64_t ordinal, int64_t cid) -> int {
    int ret = OB_SUCCESS;
    int64_t vector_idx = -1;
    const float *vector = nullptr;
    if (OB_FAIL(vectors.get_vector(ordinal, vector_idx, vector))) {
      LOG_WARN("failed to read initial HCA center", K(ret), K(ordinal), K(cid));
    } else if (vector_idx < 0 || OB_ISNULL(vector)) {
      ret = OB_INVALID_DATA;
    } else {
      copy_vector(centers + cid * dimension, vector, dimension);
    }
    return ret;
  };
  if (OB_ISNULL(centers)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (prepared && OB_FAIL(ordinals.prepare_allocate(local_k))) {
    LOG_WARN("failed to allocate initial HCA ordinals", K(ret));
  } else if (OB_FAIL(select_training_ordinals(vectors.count(), local_k,
      sample_count, seed, sample_indices, [&](int64_t ordinal, int64_t cid) -> int {
        if (prepared) { ordinals.at(cid) = ordinal; return OB_SUCCESS; }
        return load_center(ordinal, cid);
      }))) {
    LOG_WARN("failed to initialize HCA reservoirs", K(ret));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < local_k; ++cid) {
    if (prepared && OB_FAIL(load_center(ordinals.at(cid), cid))) {
      LOG_WARN("failed to load prepared initial center", K(ret), K(cid));
    } else if (OB_NOT_NULL(center_norm_info) && OB_FAIL(hca_normalize_center(centers + cid * dimension,
        dimension, *center_norm_info))) {
      LOG_WARN("failed to finalize initial HCA center", K(ret), K(cid));
    }
  }
  return ret;
}

/**
 * Compute the relative movement of all centers for the ES-compatible
 * early-stop condition. Treat centers and old_centers as center_count by
 * dimension matrices C and C_old:
 *
 *   movement = ||C - C_old||_F / ||C_old||_F
 *            = sqrt(sum(cid, d, (C[cid][d] - C_old[cid][d])^2)
 *                   / sum(cid, d, C_old[cid][d]^2))
 *
 * Training stops when movement is below the dimension-dependent tolerance.
 * If ||C_old||_F is zero, return 0 when C is also unchanged; otherwise return
 * the maximum float value so a real center update cannot trigger early stop.
 */
float normalized_frobenius_norm(const float *centers, const float *old_centers,
                                const int64_t center_count, const int64_t dimension) {
  const int64_t center_value_count = center_count * dimension;
  const float difference = hca_square_distance(centers, old_centers, center_value_count);
  const float old_norm = hca_simd::squared_norm(old_centers, center_value_count);
  return old_norm > 0.0F
             ? std::sqrt(difference / old_norm)
             : (difference > 0.0F ? std::numeric_limits<float>::max() : 0.0F);
}

/**
 * Build the only provisional center for a node whose local_k is one.
 *
 * This path is selected when the node does not need another hierarchy split,
 * normally because vectors.count() <= cluster_size. It bypasses random center
 * initialization and Balanced OT training, reads every vector in the node and
 * calculates the exact arithmetic mean:
 *
 *   center = sum(node vectors) / vectors.count()
 *
 * L2 writes the mean directly. COS/IP call hca_normalize_center() to normalize
 * it before writing local provisional center 0. This method does not assign
 * members or publish a leaf; the caller continues through the common
 * Assignment/Stats/Finalize flow.
 */
int compute_single_center(const ObHcaVectorBatch &vectors,
                          const ObVectorNormalizeInfo *center_norm_info,
                          ObHcaCenterStore &output,
                          common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  const int64_t dimension = vectors.dimension();
  ScopedArray<float> sum(allocator);
  if (OB_FAIL(sum.allocate(dimension, true))) {
    LOG_WARN("failed to allocate hca single center", K(ret), K(dimension));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      int64_t vector_idx = -1;
      const float *vector = nullptr;
      if (OB_FAIL(vectors.get_vector(i, vector_idx, vector))) {
        LOG_WARN("failed to read vector", K(ret), K(i));
      } else if (vector_idx < 0 || OB_ISNULL(vector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid vector while computing single hca center", K(ret), K(i), K(vector_idx), KP(vector));
      } else {
        add_scaled_vector(sum.get(), vector, 1.0F, dimension);
      }
    }
    if (OB_SUCC(ret)) {
      const float inverse_count = 1.0F / static_cast<float>(vectors.count());
      for (int64_t d = 0; d < dimension; ++d) {
        sum[d] *= inverse_count;
      }
      if (OB_NOT_NULL(center_norm_info) &&
          OB_FAIL(hca_normalize_center(sum.get(), dimension, *center_norm_info))) {
        LOG_WARN("failed to normalize single hca center", K(ret));
      } else if (OB_FAIL(output.set_center(0, sum.get()))) {
        LOG_WARN("failed to write single hca center", K(ret));
      }
    }
  }
  return ret;
}

int initialize_warm_centers_and_sample_pool(const ObHcaCenterStore &initial,
    const ObVectorNormalizeInfo *norm_info, const int64_t sample_count,
    int64_t *sample_indices, float *centers)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < sample_count; ++i) { sample_indices[i] = i; }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < initial.count(); ++cid) {
    const float *center = nullptr;
    if (OB_FAIL(initial.get_center(cid, center))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "initial.get_center");
    }
    else if (center == nullptr) {
      LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "initialize_warm_centers_and_sample_pool",
          KP(center), K(sample_count));
      ret = OB_INVALID_DATA;
    }
    else {
      copy_vector(centers + cid * initial.dimension(), center, initial.dimension());
      if (OB_NOT_NULL(norm_info) && OB_FAIL(hca_normalize_center(
          centers + cid * initial.dimension(), initial.dimension(), *norm_info))) {
        LOG_WARN("HCA operation failed", K(ret), "operation", "hca_normalize_center", K(sample_count));
      }
    }
  }
  return ret;
}

} // namespace

static int train_local_centers_impl(const ObHcaVectorBatch &vectors,
                            const int64_t cluster_size,
                            const ObHcaTrainParam &param,
                            const ObVectorNormalizeInfo *center_norm_info,
                            ObHcaCenterStore &provisional_centers,
                            int64_t &provisional_center_count,
                            common::ObIAllocator &allocator,
                            const ObHcaCenterStore *initial_centers,
                            const ObHcaVectorBatch *training_samples, const bool prepared = false) {
  int ret = OB_SUCCESS;
  provisional_center_count = 0;
  const int64_t vector_count = vectors.count();
  const int64_t dimension = vectors.dimension();
  const ObHcaVectorBatch &training_vectors = training_samples == nullptr ? vectors : *training_samples;
  const int64_t training_count = training_vectors.count();
  int64_t local_k = initial_centers == nullptr ? 0 : initial_centers->count();
  if (initial_centers != nullptr && (local_k <= 0 || local_k > INT32_MAX
      || initial_centers->dimension() != dimension)) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "train_local_centers_impl",
        KP(initial_centers), K(local_k), "initial_centers_dimension",
        (nullptr == initial_centers) ? -1 : static_cast<int64_t>(initial_centers->dimension()), K(dimension),
        K(cluster_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (training_count <= 0 || training_count > vector_count
      || training_vectors.dimension() != dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "train_local_centers_impl",
        K(training_count), K(vector_count), "training_vectors_dimension", training_vectors.dimension(), K(dimension),
        K(cluster_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (vector_count <= 0 || dimension <= 0 || cluster_size <= 0 ||
      !param.is_valid() || provisional_centers.dimension() != dimension) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca train argument", K(ret), K(vector_count),
             K(dimension), K(cluster_size), K(param.is_valid()),
             K(provisional_centers.dimension()));
  } else if (initial_centers == nullptr && OB_FAIL(calculate_local_k(vector_count, cluster_size,
                                       param.max_local_k_, local_k))) {
    LOG_WARN("failed to calculate local hca k", K(ret));
  } else if (provisional_centers.capacity() < local_k) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("hca center writer capacity is insufficient", K(ret), K(local_k), K(provisional_centers.capacity()));
  } else if (local_k == 1) {
    if (OB_FAIL(compute_single_center(vectors, center_norm_info, provisional_centers, allocator))) {
      LOG_WARN("failed to compute single hca center", K(ret));
    } else {
      provisional_center_count = 1;
    }
  } else if (training_count < local_k) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "train_local_centers_impl",
        K(training_count), K(local_k), K(cluster_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (dimension > INT64_MAX / local_k) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("hca local center element count overflows int64", K(ret), K(local_k), K(dimension));
  } else if (local_k > INT64_MAX / param.samples_per_cluster_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("hca sample pool element count overflows int64", K(ret), K(local_k), K(param.samples_per_cluster_));
  } else {
    const int64_t center_values = local_k * dimension;
    const int64_t sample_size = initial_centers != nullptr ? training_count
        : std::min<int64_t>(training_count, local_k * param.samples_per_cluster_);
    const int64_t batch_size = std::min<int64_t>(training_count, local_k * 2);
    if (batch_size > INT64_MAX / local_k) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("hca distance matrix element count overflows int64", K(ret), K(batch_size), K(local_k));
    } else {
      // All scratch buffers are local to one node. Shared hierarchy state is
      // written only after training has completed successfully.
      ScopedArray<float> centers(allocator);
      ScopedArray<float> old_centers(allocator);
      ScopedArray<float> distances(allocator);
      ScopedArray<float> soft_assignments(allocator);
      ScopedArray<int64_t> sample_indices(allocator);
      ScopedArray<int64_t> shuffled_indices(allocator);
      ScopedArray<int64_t> batch_indices(allocator);
      ScopedArray<float> cumulative_weights(allocator);
      ScopedArray<float> batch_sums(allocator);
      ScopedArray<float> batch_weights(allocator);
      if (OB_FAIL(centers.allocate(center_values))) {
        LOG_WARN("failed to allocate hca centers", K(ret), K(center_values));
      } else if (OB_FAIL(old_centers.allocate(center_values))) {
        LOG_WARN("failed to allocate old hca centers", K(ret), K(center_values));
      } else if (OB_FAIL(distances.allocate(batch_size * local_k))) {
        LOG_WARN("failed to allocate hca distances", K(ret), K(batch_size), K(local_k));
      } else if (OB_FAIL(soft_assignments.allocate(batch_size * local_k))) {
        LOG_WARN("failed to allocate hca soft assignments", K(ret), K(batch_size), K(local_k));
      } else if (OB_FAIL(sample_indices.allocate(sample_size))) {
        LOG_WARN("failed to allocate hca sample indices", K(ret), K(sample_size));
      } else if (OB_FAIL(shuffled_indices.allocate(sample_size))) {
        LOG_WARN("failed to allocate shuffled hca indices", K(ret), K(sample_size));
      } else if (OB_FAIL(batch_indices.allocate(batch_size))) {
        LOG_WARN("failed to allocate hca batch indices", K(ret), K(batch_size));
      } else if (OB_FAIL(cumulative_weights.allocate(local_k, true))) {
        LOG_WARN("failed to allocate cumulative hca weights", K(ret), K(local_k));
      } else if (OB_FAIL(batch_sums.allocate(center_values, true))) {
        LOG_WARN("failed to allocate hca batch sums", K(ret), K(center_values));
      } else if (OB_FAIL(batch_weights.allocate(local_k, true))) {
        LOG_WARN("failed to allocate hca batch weights", K(ret), K(local_k));
      } else if (initial_centers != nullptr && OB_FAIL(initialize_warm_centers_and_sample_pool(
                     *initial_centers, center_norm_info, sample_size, sample_indices.get(), centers.get()))) {
        LOG_WARN("failed to initialize warm-start HCA centers", K(ret));
      } else if (initial_centers == nullptr && OB_FAIL(initialize_centers_and_sample_pool(
                     training_vectors, local_k, sample_size, param.random_seed_,
                     center_norm_info, sample_indices.get(), centers.get(), prepared))) {
        LOG_WARN("failed to initialize hca centers and sample pool", K(ret),
                 K(sample_size));
      } else {
        copy_vector(old_centers.get(), centers.get(), center_values);
        SinkhornWorkspace sinkhorn(allocator);
        // One streaming median is shared by every Balanced OT mini-batch. It
        // controls the Sinkhorn temperature without sorting all distances.
        OnlineQuantileEstimator median;
        bool median_inited = false;
        float eta = param.eta_init_;
        if (OB_FAIL(sinkhorn.init(batch_size, local_k))) {
          LOG_WARN("failed to initialize hca sinkhorn workspace", K(ret));
        }
        for (int64_t epoch = 0; OB_SUCC(ret) && epoch < param.max_iterations_;
             ++epoch) {
          if (OB_FAIL(shuffle_samples(sample_indices.get(), sample_size,
                                      param.random_seed_ + 2 + static_cast<uint64_t>(epoch),
                                      shuffled_indices.get()))) {
            LOG_WARN("failed to shuffle hca epoch sample pool", K(ret), K(epoch), K(sample_size));
          }
          for (int64_t offset = 0; OB_SUCC(ret) && offset < sample_size;
               offset += batch_size) {
            // The final short slice wraps to the start of the shuffled pool so
            // every Sinkhorn call retains the fixed batch_size-by-K shape.
            for (int64_t row = 0; row < batch_size; ++row) {
              batch_indices[row] = shuffled_indices[(offset + row) % sample_size];
            }
            lib::ob_sort(batch_indices.get(), batch_indices.get() + batch_size);
            // Phase 1: build the batch-by-center squared-L2 cost matrix.
            float max_distance = 0.0F;
            for (int64_t row = 0; OB_SUCC(ret) && row < batch_size; ++row) {
              int64_t vector_idx = -1;
              const float *vector = nullptr;
              if (OB_FAIL(training_vectors.get_vector(batch_indices[row], vector_idx, vector))) {
                LOG_WARN("failed to read sampled hca vector", K(ret), K(row),
                         K(batch_indices[row]));
              } else if (vector_idx < 0 || OB_ISNULL(vector)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid sampled hca vector", K(ret), K(row), K(batch_indices[row]), K(vector_idx),
                         KP(vector));
              } else {
                for (int64_t cid = 0; cid < local_k; ++cid) {
                  const float distance = hca_square_distance(vector, centers.get() + cid * dimension, dimension);
                  distances[row * local_k + cid] = distance;
                  max_distance = std::max(max_distance, distance);
                }
              }
            }
            if (OB_SUCC(ret) && !median_inited) {
              if (OB_FAIL(median.init(0.5F, 0.0F, max_distance, 1e-4F, 42))) {
                LOG_WARN("failed to initialize hca median estimator", K(ret), K(max_distance));
              } else {
                median_inited = true;
              }
            }
            if (OB_SUCC(ret)) {
              for (int64_t i = 0; i < batch_size * local_k; ++i) {
                median.update(distances[i]);
              }
              const float eps = std::max(eta * median.estimate(), param.eta_min_);
              // Phase 2: turn costs into balanced soft assignments.
              if (OB_FAIL(sinkhorn.compute(distances.get(), param.sinkhorn_iterations_, eps, soft_assignments.get()))) {
                LOG_WARN("failed to compute hca sinkhorn plan", K(ret), K(eps));
              }
            }
            if (OB_SUCC(ret)) {
              // Phase 3: aggregate weighted batch sums for every center.
              MEMSET(batch_sums.get(), 0, sizeof(float) * center_values);
              MEMSET(batch_weights.get(), 0, sizeof(float) * local_k);
              for (int64_t row = 0; OB_SUCC(ret) && row < batch_size; ++row) {
                int64_t vector_idx = -1;
                const float *vector = nullptr;
                if (OB_FAIL(training_vectors.get_vector(batch_indices[row], vector_idx, vector))) {
                  LOG_WARN("failed to reread sampled hca vector", K(ret),
                           K(row));
                } else if (vector_idx < 0 || OB_ISNULL(vector)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid reread sampled hca vector", K(ret),
                           K(row), K(batch_indices[row]), K(vector_idx), KP(vector));
                } else {
                  for (int64_t cid = 0; cid < local_k; ++cid) {
                    const float weight = soft_assignments[row * local_k + cid];
                    if (weight > 1e-7F) {
                      batch_weights[cid] += weight;
                      add_scaled_vector(batch_sums.get() + cid * dimension, vector, weight, dimension);
                    }
                  }
                }
              }
            }
            // Phase 4: apply the cumulative-weight learning rate. Center
            // projection must finish before the next distance matrix reads
            // it.
            for (int64_t cid = 0; OB_SUCC(ret) && cid < local_k; ++cid) {
              if (batch_weights[cid] > 0.0F) {
                const float scaled_batch_weight = batch_weights[cid] * static_cast<float>(local_k);
                cumulative_weights[cid] += scaled_batch_weight;
                const float learning_rate = scaled_batch_weight / cumulative_weights[cid];
                const float sum_scale = learning_rate / batch_weights[cid];
                blend_vector(centers.get() + cid * dimension,
                             batch_sums.get() + cid * dimension, sum_scale, 1.0F - learning_rate, dimension);
                if (OB_NOT_NULL(center_norm_info) &&
                    OB_FAIL(hca_normalize_center(centers.get() + cid * dimension, dimension, *center_norm_info))) {
                  LOG_WARN("failed to normalize updated hca center", K(ret), K(cid));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            // Forget stale soft mass and stop when relative center movement is
            // below the dimension-dependent tolerance.
            eta *= param.eta_multiplier_;
            for (int64_t cid = 0; cid < local_k; ++cid) {
              cumulative_weights[cid] *= param.forgetting_factor_;
            }
            const float tolerance = dimension < 100 ? 1e-3F : 1e-2F;
            if (normalized_frobenius_norm(centers.get(), old_centers.get(), local_k, dimension) < tolerance) {
              break;
            } else {
              copy_vector(old_centers.get(), centers.get(), center_values);
            }
          }
        }
        for (int64_t cid = 0; OB_SUCC(ret) && cid < local_k; ++cid) {
          if (OB_FAIL(provisional_centers.set_center(cid, centers.get() + cid * dimension))) {
            LOG_WARN("failed to write provisional hca center", K(ret), K(cid));
          }
        }
        if (OB_SUCC(ret)) {
          provisional_center_count = local_k;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to train local hca centers", K(ret), K(vector_count), K(local_k), K(dimension));
      }
    }
  }
  return ret;
}

int hca_train_local_centers(const ObHcaVectorBatch &vectors, const int64_t cluster_size,
    const ObHcaTrainParam &param, const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &provisional_centers, int64_t &provisional_center_count,
    common::ObIAllocator &allocator, const ObHcaVectorBatch *training_samples)
{
  return train_local_centers_impl(vectors, cluster_size, param, center_norm_info,
      provisional_centers, provisional_center_count, allocator, nullptr, training_samples);
}

int hca_plan_local_training(const int64_t vector_count, const int64_t cluster_size,
    const ObHcaTrainParam &param, common::ObIArray<int64_t> &center_ordinals,
    common::ObIArray<int64_t> &sample_ordinals)
{
  int ret = OB_SUCCESS;
  int64_t local_k = 0;
  center_ordinals.reuse();
  sample_ordinals.reuse();
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca local training plan parameters", K(ret), K(vector_count), K(cluster_size));
  } else if (OB_FAIL(calculate_local_k(vector_count, cluster_size, param.max_local_k_, local_k))) {
    LOG_WARN("failed to calculate sample plan K", K(ret));
  } else if (local_k > INT64_MAX / param.samples_per_cluster_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("hca local training sample count overflow", K(ret), K(local_k), K(param.samples_per_cluster_));
  } else {
    const int64_t count = local_k == 1 ? vector_count
        : MIN(vector_count, local_k * param.samples_per_cluster_);
    if (OB_FAIL(sample_ordinals.prepare_allocate(count))) {
      LOG_WARN("failed to allocate hca training sample ordinals", K(ret), K(count));
    } else if (local_k == 1) {
      for (int64_t i = 0; i < count; ++i) { sample_ordinals.at(i) = i; }
    } else if (OB_FAIL(center_ordinals.prepare_allocate(local_k))) {
      LOG_WARN("failed to allocate hca training center ordinals", K(ret), K(local_k));
    } else {
      ret = select_training_ordinals(vector_count, local_k, count, param.random_seed_,
          &sample_ordinals.at(0), [&](int64_t ordinal, int64_t cid) -> int {
            center_ordinals.at(cid) = ordinal;
            return OB_SUCCESS;
          });
    }
  }
  return ret;
}

int hca_train_prepared_centers(const ObHcaVectorBatch &vectors,
    const int64_t cluster_size, const ObHcaTrainParam &param,
    const ObVectorNormalizeInfo *center_norm_info, ObHcaCenterStore &centers,
    int64_t &center_count, common::ObIAllocator &allocator)
{
  return train_local_centers_impl(vectors, cluster_size, param, center_norm_info,
      centers, center_count, allocator, nullptr, nullptr, true);
}

int hca_sample_ordinals(const int64_t vector_count, const int64_t sample_count,
    const uint64_t seed, int64_t *ordinals)
{
  return hca_internal::reservoir_sample(vector_count, sample_count, seed, ordinals);
}

int hca_train_warm_centers(const ObHcaVectorBatch &vectors, const ObHcaCenterStore &initial_centers,
    const ObHcaTrainParam &param, const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &output_centers, common::ObIAllocator &allocator)
{
  int64_t output_count = 0;
  // Use exactly the caller's centers and full sample work. Only initialization
  // differs from ordinary HCA; Sinkhorn and center updates share one owner.
  return train_local_centers_impl(vectors, 1 /* unused with initial centers */, param,
      center_norm_info, output_centers, output_count, allocator, &initial_centers, nullptr);
}

int hca_find_nearest_centers(const float *const *vectors, const int64_t vector_count,
    const ObHcaCenterStore &centers, const int32_t *candidate_cids,
    const int64_t candidate_count, int64_t *nearest_cids)
{
  int ret = OB_SUCCESS;
  static const int64_t CENTER_TILE = 128;
  float best[OB_HCA_ASSIGN_BATCH_SIZE];
  const float *center_tile[CENTER_TILE];
  const auto distance4 = hca_simd::select_square_distance4();
  if (vectors == nullptr || nearest_cids == nullptr || vector_count <= 0
      || vector_count > OB_HCA_ASSIGN_BATCH_SIZE || candidate_count <= 0
      || centers.dimension() <= 0 || (candidate_cids == nullptr && candidate_count > centers.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid HCA nearest-center batch", K(ret), K(vector_count), K(candidate_count),
        K(centers.dimension()), K(centers.count()));
  }
  for (int64_t q = 0; OB_SUCC(ret) && q < vector_count; ++q) {
    if (vectors[q] == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null HCA query vector", K(ret), K(q), K(vector_count));
    } else {
      nearest_cids[q] = -1;
      best[q] = 0.0F;
    }
  }
  for (int64_t cb = 0; OB_SUCC(ret) && cb < candidate_count; cb += CENTER_TILE) {
    const int64_t count = MIN(CENTER_TILE, candidate_count - cb);
    for (int64_t c = 0; OB_SUCC(ret) && c < count; ++c) {
      const int64_t cid = candidate_cids == nullptr ? cb + c : candidate_cids[cb + c];
      if (cid < 0 || cid >= centers.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid HCA candidate center", K(ret), K(cid), K(cb), K(c), K(centers.count()));
      } else if (OB_FAIL(centers.get_center(cid, center_tile[c]))) {
        LOG_WARN("failed to read HCA candidate center", K(ret), K(cid));
      } else if (center_tile[c] == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null HCA candidate center", K(ret), K(cid), K(cb), K(c));
      }
    }
    for (int64_t qb = 0; OB_SUCC(ret) && qb < vector_count; qb += 4) {
      const int64_t queries = MIN(4, vector_count - qb);
      for (int64_t c = 0; OB_SUCC(ret) && c < count; ++c) {
        const int64_t cid = candidate_cids == nullptr ? cb + c : candidate_cids[cb + c];
        float distances[4];
        if (queries == 4) {
          distance4(vectors + qb, center_tile[c], centers.dimension(), distances);
        } else {
          for (int64_t q = 0; q < queries; ++q) {
            distances[q] = hca_square_distance(vectors[qb + q], center_tile[c], centers.dimension());
          }
        }
        for (int64_t q = 0; OB_SUCC(ret) && q < queries; ++q) {
          if (!std::isfinite(distances[q])) {
            ret = OB_INVALID_DATA;
            LOG_WARN("non-finite HCA distance", K(ret), K(qb), K(q), K(cid), K(distances[q]));
          } else if (nearest_cids[qb + q] < 0 || distances[q] < best[qb + q]) {
            nearest_cids[qb + q] = cid;
            best[qb + q] = distances[q];
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(dag_yield())) {
      LOG_WARN("HCA nearest-center batch yield failed", K(ret), K(cb), K(candidate_count));
    }
  }
  return ret;
}

int hca_assign_vectors(const ObHcaVectorBatch &vectors,
                       const ObHcaCenterStore &centers,
                       const int32_t *candidate_cids,
                       const int64_t candidate_count,
                       ObHcaAssignmentStore &assignments,
                       ObHcaCidMemberStore &members)
{
  int ret = OB_SUCCESS;
  if (vectors.count() < 0 || vectors.dimension() <= 0
      || vectors.dimension() != centers.dimension() || candidate_count <= 0
      || candidate_cids == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid HCA vector assignment arguments", K(ret), K(vectors.count()), K(vectors.dimension()),
        K(candidate_count));
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < candidate_count; ++c) {
    if (candidate_cids[c] < 0 || candidate_cids[c] >= centers.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid HCA assignment candidate", K(ret), K(c), K(candidate_cids[c]), K(centers.count()));
    }
  }
  for (int64_t begin = 0; OB_SUCC(ret) && begin < vectors.count(); begin += OB_HCA_ASSIGN_BATCH_SIZE) {
    const int64_t count = MIN(OB_HCA_ASSIGN_BATCH_SIZE, vectors.count() - begin);
    const float *batch[OB_HCA_ASSIGN_BATCH_SIZE];
    int64_t vector_idxs[OB_HCA_ASSIGN_BATCH_SIZE];
    int64_t nearest[OB_HCA_ASSIGN_BATCH_SIZE];
    for (int64_t q = 0; OB_SUCC(ret) && q < count; ++q) {
      if (OB_FAIL(vectors.get_vector(begin + q, vector_idxs[q], batch[q]))) {
        LOG_WARN("failed to read HCA assignment batch vector", K(ret), K(begin), K(q));
      } else if (vector_idxs[q] < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid HCA batch vector index", K(ret), K(begin), K(q), K(vector_idxs[q]));
      }
    }
    if (OB_SUCC(ret)) {
      ret = hca_find_nearest_centers(batch, count, centers, candidate_cids, candidate_count, nearest);
    }
    for (int64_t q = 0; OB_SUCC(ret) && q < count; ++q) {
      if (OB_FAIL(members.append_member(nearest[q], vector_idxs[q]))) {
        LOG_WARN("failed to append HCA batch member", K(ret), K(nearest[q]), K(vector_idxs[q]));
      } else if (OB_FAIL(assignments.set_assignment(vector_idxs[q], nearest[q]))) {
        LOG_WARN("failed to set HCA batch assignment", K(ret), K(vector_idxs[q]), K(nearest[q]));
      }
    }
  }
  if (OB_FAIL(ret)) { LOG_WARN("failed to assign HCA vector batch", K(ret), K(candidate_count)); }
  return ret;
}

int hca_compute_local_center_stats_range(
    const ObHcaVectorAccessor &vectors,
    const ObHcaCidMemberStore &members,
    const int64_t begin_cid,
    const int64_t end_cid,
    const int64_t dimension,
    ObHcaCenterStatsStore &global_stats,
    common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  ScopedArray<float> sum(allocator);
  if (dimension <= 0 || dimension != vectors.dimension() ||
      OB_FAIL(validate_range(begin_cid, end_cid, members.cid_count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca center stats argument", K(ret), K(dimension),
             K(vectors.dimension()), K(begin_cid), K(end_cid), K(members.cid_count()));
  } else if (OB_FAIL(sum.allocate(dimension, true))) {
    LOG_WARN("failed to allocate hca center stats sum", K(ret), K(dimension));
  }
  for (int64_t cid = begin_cid; OB_SUCC(ret) && cid < end_cid; ++cid) {
    // Cid-range partitioning gives one writer exclusive ownership of each sum,
    // avoiding task-local K*dimension partial sums and a later reduce barrier.
    zero_vector(sum.get(), dimension);
    const int64_t count = members.member_count(cid);
    if (count < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hca member count", K(ret), K(cid), K(count));
    }
    for (int64_t offset = 0; OB_SUCC(ret) && offset < count; ++offset) {
      int64_t vector_idx = -1;
      const float *vector = nullptr;
      if (OB_FAIL(members.get_member(cid, offset, vector_idx))) {
        LOG_WARN("failed to read hca member", K(ret), K(cid), K(offset));
      } else if (OB_FAIL(vectors.get_vector(vector_idx, vector))) {
        LOG_WARN("failed to read hca member vector", K(ret), K(cid), K(vector_idx));
      } else if (OB_ISNULL(vector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hca member vector is null", K(ret), K(cid), K(offset), K(vector_idx));
      } else {
        add_scaled_vector(sum.get(), vector, 1.0F, dimension);
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(global_stats.set_stats(cid, sum.get(), count))) {
      LOG_WARN("failed to write hca center stats", K(ret), K(cid), K(count));
    }
  }
  return ret;
}

int hca_finalize_local_centers(
    const ObHcaCenterStatsStore &global_stats,
    const int64_t old_center_count,
    const int64_t dimension,
    const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &final_centers,
    ObHcaAssignmentStore &assignments,
    ObHcaCidMemberStore &member_lists,
    ObHcaCidRemapStore &old_to_new,
    int64_t &final_center_count,
    ObHcaSplitStatus &split_status) {
  int ret = OB_SUCCESS;
  final_center_count = 0;
  split_status = HCA_SPLIT_NO_PROGRESS;
  if (old_center_count <= 0 || dimension <= 0 ||
      global_stats.count() < old_center_count ||
      global_stats.dimension() != dimension ||
      final_centers.capacity() < old_center_count ||
      final_centers.dimension() != dimension ||
      member_lists.cid_count() < old_center_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca center finalize argument", K(ret),
             K(old_center_count), K(dimension), K(global_stats.count()),
             K(global_stats.dimension()), K(final_centers.capacity()),
             K(final_centers.dimension()), K(member_lists.cid_count()));
  }
  for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_center_count;
       ++old_cid) {
    // First pass computes exact means and establishes a dense non-empty cid
    // map.
    const float *sum = nullptr;
    int64_t member_count = 0;
    if (OB_FAIL(global_stats.get_stats(old_cid, sum, member_count))) {
      LOG_WARN("failed to read hca center stats", K(ret), K(old_cid));
    } else if (member_count < 0 || (member_count > 0 && OB_ISNULL(sum))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hca center stats", K(ret), K(old_cid), K(member_count), KP(sum));
    } else if (member_lists.member_count(old_cid) != member_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hca stats and member row count mismatch", K(ret), K(old_cid),
               K(member_count), K(member_lists.member_count(old_cid)));
    } else if (member_count == 0) {
      if (OB_FAIL(old_to_new.set_new_cid(old_cid, -1))) {
        LOG_WARN("failed to write empty hca cid remap", K(ret), K(old_cid));
      }
    } else {
      const int32_t new_cid = static_cast<int32_t>(final_center_count++);
      float *center = nullptr;
      const float inverse_count = 1.0F / static_cast<float>(member_count);
      if (OB_FAIL(final_centers.get_mutable_center(new_cid, center))) {
        LOG_WARN("failed to get mutable final hca center", K(ret), K(new_cid));
      } else if (OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mutable final hca center is null", K(ret), K(old_cid), K(new_cid));
      } else {
        for (int64_t d = 0; d < dimension; ++d) {
          center[d] = sum[d] * inverse_count;
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(center_norm_info) &&
          OB_FAIL(hca_normalize_center(center, dimension, *center_norm_info))) {
        LOG_WARN("failed to normalize full-data hca center", K(ret), K(old_cid));
      } else if (OB_SUCC(ret) &&
                 OB_FAIL(final_centers.set_center(new_cid, center))) {
        LOG_WARN("failed to commit final hca center", K(ret), K(old_cid), K(new_cid));
      } else if (OB_SUCC(ret) &&
                 OB_FAIL(old_to_new.set_new_cid(old_cid, new_cid))) {
        LOG_WARN("failed to write hca cid remap", K(ret), K(old_cid), K(new_cid));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // Second pass applies the prepared remap to assignments and moves whole
    // member rows instead of appending every vector again.
    for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_center_count;
         ++old_cid) {
      int32_t new_cid = -1;
      if (OB_FAIL(old_to_new.get_new_cid(old_cid, new_cid))) {
        LOG_WARN("failed to read hca cid remap", K(ret), K(old_cid));
      } else if (new_cid < -1 || new_cid >= final_center_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid hca cid remap", K(ret), K(old_cid), K(new_cid), K(final_center_count));
      } else if (new_cid >= 0 && new_cid != old_cid) {
        const int64_t member_count = member_lists.member_count(old_cid);
        if (member_count <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid hca member count for remap", K(ret), K(old_cid), K(new_cid), K(member_count));
        }
        for (int64_t offset = 0; OB_SUCC(ret) && offset < member_count;
             ++offset) {
          int64_t vector_idx = -1;
          if (OB_FAIL(member_lists.get_member(old_cid, offset, vector_idx))) {
            LOG_WARN("failed to read hca member for remap", K(ret), K(old_cid), K(offset));
          } else if (OB_FAIL(assignments.set_assignment(vector_idx, new_cid))) {
            LOG_WARN("failed to remap hca assignment", K(ret), K(vector_idx), K(new_cid));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(member_lists.move_row(old_cid, new_cid))) {
          LOG_WARN("failed to move hca member row", K(ret), K(old_cid), K(new_cid));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    split_status = final_center_count <= 1 ? HCA_SPLIT_NO_PROGRESS : HCA_SPLIT_SUCCEEDED;
  }
  return ret;
}

int hca_publish_leaf_result(const float *leaf_center,
                            const int64_t dimension,
                            const ObHcaCidMemberStore &leaf_members,
                            const int64_t leaf_cid,
                            ObHcaLeafResultWriter &result_writer,
                            int32_t &global_cid) {
  int ret = OB_SUCCESS;
  global_cid = -1;
  if (OB_ISNULL(leaf_center) || dimension <= 0 || leaf_cid < 0 ||
      leaf_cid >= leaf_members.cid_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca leaf publish argument", K(ret), KP(leaf_center),
             K(dimension), K(leaf_cid), K(leaf_members.cid_count()));
  } else if (OB_FAIL(result_writer.publish(leaf_center, dimension, leaf_members, leaf_cid, global_cid))) {
    LOG_WARN("failed to publish hca leaf result", K(ret), K(leaf_cid));
  }
  return ret;
}


using namespace common;
using namespace hca_internal;

template <typename T>
static int fill_array(ObIArray<T> &array, const int64_t count, const T value)
{
  int ret = OB_SUCCESS;
  array.reuse();
  if (count < 0 || count > INT64_MAX / static_cast<int64_t>(sizeof(T))) {
    LOG_WARN("HCA size overflow", "ret", OB_SIZE_OVERFLOW, "operation", "fill_array", K(count));
    ret = OB_SIZE_OVERFLOW;
  }
  else if (OB_FAIL(array.reserve(count))) {
    LOG_WARN("IVF merge operation failed", K(ret), "operation", "array.reserve");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ret = array.push_back(value);
    if (OB_FAIL(ret)) {
      LOG_WARN("HCA operation failed", K(ret), "operation", "array.push_back", K(i), K(count));
    }
  }
  return ret;
}

int hca_check_cluster_quality(const int64_t *counts, const int64_t count, ObHcaClusterQuality &quality)
{
  int ret = OB_SUCCESS;
  quality = ObHcaClusterQuality();
  double squares = 0;
  if (count < 0 || (count > 0 && nullptr == counts)) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_check_cluster_quality", K(count),
        KP(counts));
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!hca_simd::count_statistics(counts, count, quality.total_, quality.maximum_, quality.empty_, squares)) {
    LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_check_cluster_quality", KP(counts), K(count),
        K(quality.total_), K(quality.maximum_), K(quality.empty_), K(squares));
    ret = OB_INVALID_DATA;
  } else if (count == 0 || quality.total_ == 0) {
    quality.degenerate_ = true;
  } else {
    const double mean = static_cast<double>(quality.total_) / count;
    if (count > 1 && squares > 0) {
      quality.z_score_ = (quality.maximum_ - mean) / std::sqrt(squares / (count - 1));
      if (std::abs(quality.z_score_ - 3.0) < 1e-10) {
        // Match the scalar reference when a SIMD reduction could flip a boundary decision.
        squares = 0;
        for (int64_t i = 0; i < count; ++i) {
          const double delta = static_cast<double>(counts[i]) - mean;
          squares += delta * delta;
        }
        quality.z_score_ = squares > 0 ? (quality.maximum_ - mean) / std::sqrt(squares / (count - 1)) : 0;
      }
    }
    quality.degenerate_ = static_cast<double>(quality.empty_) / count > 0.1 || quality.z_score_ > 3.0;
  }
  return ret;
}

int hca_check_prior_quality(const int64_t *mass, const int64_t prior_count, const int64_t k, bool &degenerate)
{
  int ret = OB_SUCCESS;
  int64_t total = 0, maximum = 0, zeros = 0;
  double ignored = 0;
  degenerate = false;
  if (k < 2 || prior_count < 0 || (prior_count > 0 && mass == nullptr)) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_check_prior_quality", K(k),
        K(prior_count), KP(mass));
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!hca_simd::count_statistics(mass, prior_count, total, maximum, zeros, ignored)) {
    LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_check_prior_quality", KP(mass),
        K(prior_count), K(total), K(maximum), K(zeros), K(ignored), K(k));
    ret = OB_INVALID_DATA;
  }
  else { degenerate = prior_count < std::max<int64_t>(2, k / 4) || total == 0 || static_cast<double>(zeros) / prior_count > 0.5; }
  return ret;
}

int hca_calculate_reuse_k(const int64_t vector_count, const int64_t cluster_size, const bool insertion,
    const int64_t *mass, const int64_t prior_count, const int64_t covered_vector_count, int64_t &k)
{
  int ret = OB_SUCCESS;
  k = 0;
  if (vector_count <= 0 || cluster_size <= 0 || prior_count <= 0
      || covered_vector_count <= 0 || covered_vector_count > vector_count || mass == nullptr) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_calculate_reuse_k", K(vector_count),
        K(cluster_size), K(prior_count), K(covered_vector_count), KP(mass));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t rounded = vector_count / cluster_size
        + (vector_count % cluster_size >= cluster_size / 2 + cluster_size % 2);
    k = std::max<int64_t>(2, std::min<int64_t>(OB_HCA_MAX_LOCAL_K, rounded));
    if (!insertion) {
      int64_t total = 0, maximum = 0, zeros = 0;
      double ignored = 0;
      if (!hca_simd::count_statistics(mass, prior_count, total, maximum, zeros, ignored)) {
        LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_calculate_reuse_k", KP(mass),
            K(prior_count), K(total), K(maximum), K(zeros), K(ignored), K(vector_count), K(cluster_size),
            K(covered_vector_count));
        ret = OB_INVALID_DATA;
      }
      else if (static_cast<double>(total) / prior_count < cluster_size) {
        const double density_k = static_cast<double>(prior_count) * vector_count / covered_vector_count;
        k = std::max(k, static_cast<int64_t>(std::min<double>(OB_HCA_MAX_LOCAL_K, std::floor(density_k + 0.5))));
      }
    }
  }
  return ret;
}

int64_t hca_choose_weighted(const double *weights, const int64_t count,
    const double total, ObRandom &random)
{
  if (weights == nullptr || count <= 0 || !std::isfinite(total)) { return -1; }
  if (total <= 0) { return random.get(0, count - 1); }
  const double target = random_unit_double(random) * total;
  double cumulative = 0;
  for (int64_t i = 0; i < count; ++i) { cumulative += weights[i]; if (cumulative > target) { return i; } }
  return count - 1;
}

int hca_initialize_prior_centers(const float *prior, const int64_t *mass, const int64_t prior_count,
    const int64_t dimension, const int64_t k, const bool insertion, ObRandom &random,
    float *centers, int64_t &selected_count, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  selected_count = 0;
  if (prior == nullptr || mass == nullptr || centers == nullptr || prior_count <= 0
      || dimension <= 0 || k < 2 || k > OB_HCA_MAX_LOCAL_K || prior_count > INT64_MAX / dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_initialize_prior_centers", KP(prior),
        KP(mass), KP(centers), K(prior_count), K(dimension), K(k));
    ret = OB_INVALID_ARGUMENT;
  } else if (prior_count <= k) {
    MEMCPY(centers, prior, sizeof(float) * prior_count * dimension);
    selected_count = prior_count;
  } else if (insertion) {
    ScopedArray<int64_t> selected(allocator);
    if (OB_FAIL(selected.allocate(k))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "selected.allocate");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < prior_count; ++i) {
      if (i < k) { selected[i] = i; }
      else { const int64_t slot = random.get(0, i); if (slot < k) { selected[slot] = i; } }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < k; ++i) {
      MEMCPY(centers + i * dimension, prior + selected[i] * dimension, sizeof(float) * dimension);
    }
    if (OB_SUCC(ret)) { selected_count = k; }
  } else {
    int64_t total = 0, maximum = 0, zeros = 0;
    double ignored = 0;
    if (!hca_simd::count_statistics(mass, prior_count, total, maximum, zeros, ignored)) {
      LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_initialize_prior_centers", KP(mass),
          K(prior_count), K(total), K(maximum), K(zeros), K(ignored), K(dimension), K(k));
      ret = OB_INVALID_DATA;
    } else {
      int64_t selected = prior_count - 1;
      if (total <= 0) { selected = random.get(0, prior_count - 1); }
      else {
        const double target = random_unit_double(random) * total;
        double cumulative = 0;
        for (int64_t i = 0; i < prior_count; ++i) {
          cumulative += mass[i];
          if (cumulative > target) { selected = i; break; }
        }
      }
      MEMCPY(centers, prior + selected * dimension, sizeof(float) * dimension);
      selected_count = 1;
    }
  }
  return ret;
}

int hca_sample_prior_supplement(const ObHcaVectorBatch &vectors, const int64_t sample_count,
    ObRandom &random, float *samples, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ScopedArray<int64_t> indices(allocator);
  if (sample_count <= 0 || sample_count > vectors.count() || vectors.dimension() <= 0
      || samples == nullptr || sample_count > INT64_MAX / vectors.dimension()) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_sample_prior_supplement",
        K(sample_count), "vectors_count", vectors.count(), "vectors_dimension", vectors.dimension(), KP(samples));
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_FAIL(indices.allocate(sample_count))) {
    LOG_WARN("IVF merge operation failed", K(ret), "operation", "indices.allocate");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    if (i < sample_count) { indices[i] = i; }
    else { const int64_t slot = random.get(0, i); if (slot < sample_count) { indices[slot] = i; } }
  }
  if (OB_SUCC(ret)) { lib::ob_sort(indices.get(), indices.get() + sample_count); }
  for (int64_t i = 0; OB_SUCC(ret) && i < sample_count; ++i) {
    const float *vector = nullptr;
    int64_t vector_idx = -1;
    if (OB_FAIL(vectors.get_vector(indices[i], vector_idx, vector))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "vectors.get_vector");
    }
    else if (vector == nullptr) {
      LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_sample_prior_supplement", KP(vector),
          K(sample_count));
      ret = OB_INVALID_DATA;
    }
    else { MEMCPY(samples + i * vectors.dimension(), vector, sizeof(float) * vectors.dimension()); }
  }
  return ret;
}

int hca_update_prior_weights(const float *prior, const int64_t *mass, const int64_t count,
    const int64_t dimension, const float *representative, float *minimum, double *weights, double &total)
{
  int ret = OB_SUCCESS;
  total = 0;
  constexpr int64_t DISTANCE_BATCH_SIZE = 256;
  float distances[DISTANCE_BATCH_SIZE];
  if (prior == nullptr || mass == nullptr || representative == nullptr || minimum == nullptr
      || weights == nullptr || count < 0 || dimension <= 0 || count > INT64_MAX / dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_update_prior_weights", KP(prior),
        KP(mass), KP(representative), KP(minimum), KP(weights), K(count), K(dimension));
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t begin = 0; OB_SUCC(ret) && begin < count; begin += DISTANCE_BATCH_SIZE) {
    const int64_t size = std::min(DISTANCE_BATCH_SIZE, count - begin);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      distances[i] = hca_square_distance(prior + (begin + i) * dimension, representative, dimension);
      if (!std::isfinite(distances[i]) || mass[begin + i] < 0) {
        LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_update_prior_weights", KP(distances),
            K(i), KP(mass), K(begin), K(count), K(dimension), "distance", distances[i], "posting_mass",
            mass[begin + i]);
        ret = OB_INVALID_DATA;
      }
    }
    if (OB_SUCC(ret)) {
      hca_simd::update_min_distances(minimum + begin, distances, size);
      total += hca_simd::weighted_distances(minimum + begin, mass + begin, size, weights + begin);
    }
  }
  return ret;
}

int hca_compute_prior_distances(const float *prior, const int64_t count, const int64_t dimension,
    const float *centers, const int64_t k, float *distances)
{
  int ret = OB_SUCCESS;
  if (prior == nullptr || centers == nullptr || distances == nullptr || count < 0
      || dimension <= 0 || k <= 0 || count > INT64_MAX / dimension || count > INT64_MAX / k) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_compute_prior_distances", KP(prior),
        KP(centers), KP(distances), K(count), K(dimension), K(k));
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    for (int64_t c = 0; OB_SUCC(ret) && c < k; ++c) {
      const float distance = hca_square_distance(prior + i * dimension, centers + c * dimension, dimension);
      if (!std::isfinite(distance)) {
        LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_compute_prior_distances", K(distance),
            K(count), K(dimension), K(k));
        ret = OB_INVALID_DATA;
      }
      else { distances[i * k + c] = distance; }
    }
  }
  return ret;
}

int hca_update_supplement_weights(const float *samples, const int64_t count, const int64_t dimension,
    const float *centers, const int64_t center_count, const int64_t *mass,
    float *minimum, double *weights, double &total)
{
  int ret = OB_SUCCESS;
  total = 0;
  constexpr int64_t DISTANCE_BATCH_SIZE = 256;
  float distances[DISTANCE_BATCH_SIZE];
  if (samples == nullptr || centers == nullptr || minimum == nullptr || weights == nullptr
      || count < 0 || dimension <= 0 || center_count <= 0 || count > INT64_MAX / dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_update_supplement_weights",
        KP(samples), KP(centers), KP(minimum), KP(weights), K(count), K(dimension), K(center_count));
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < center_count; ++c) {
    const double scale = mass == nullptr ? 1.0 : static_cast<double>(mass[c]);
    if (scale < 0) {
      LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_update_supplement_weights", K(scale),
          K(count), K(dimension), K(center_count));
      ret = OB_INVALID_DATA;
    }
    for (int64_t begin = 0; OB_SUCC(ret) && begin < count; begin += DISTANCE_BATCH_SIZE) {
      const int64_t size = std::min(DISTANCE_BATCH_SIZE, count - begin);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        distances[i] = hca_square_distance(samples + (begin + i) * dimension, centers + c * dimension, dimension);
        if (!std::isfinite(distances[i])) {
          LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_update_supplement_weights",
              KP(distances), K(i), K(count), K(dimension), K(center_count), "distance", distances[i]);
          ret = OB_INVALID_DATA;
        }
      }
      if (OB_SUCC(ret)) {
        hca_simd::update_min_distance_weights(minimum + begin, weights + begin, distances, scale, size);
      }
    }
  }
  if (OB_SUCC(ret)) { total = hca_simd::sum_weights(weights, count); }
  return ret;
}

int hca_reduce_prior_centers(const float *prior, const int64_t *mass, const int64_t count,
    const int64_t dimension, const int64_t k, const float *distances, float *centers, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (prior == nullptr || mass == nullptr || distances == nullptr || centers == nullptr
      || count <= 0 || dimension <= 0 || k < 2 || k > OB_HCA_MAX_LOCAL_K
      || count > INT64_MAX / dimension || count > INT64_MAX / k) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_reduce_prior_centers", KP(prior),
        KP(mass), KP(distances), KP(centers), K(count), K(dimension), K(k));
    return OB_INVALID_ARGUMENT;
  }
  struct PriorOrder { int64_t idx_; float regret_; int64_t nearest_;
    TO_STRING_KV(K_(idx), K_(regret), K_(nearest)); };
  ObArray<PriorOrder, ObWrapperAllocator> order;
  order.set_block_allocator(ObWrapperAllocator(allocator));
  ObArray<int64_t, ObWrapperAllocator> remaining;
  remaining.set_block_allocator(ObWrapperAllocator(allocator));
  ObArray<int64_t, ObWrapperAllocator> total_mass;
  total_mass.set_block_allocator(ObWrapperAllocator(allocator));
  ObArray<float, ObWrapperAllocator> result;
  result.set_block_allocator(ObWrapperAllocator(allocator));
  int64_t sum = 0, maximum = 0, zeros = 0;
  double ignored = 0;
  if (!hca_simd::count_statistics(mass, count, sum, maximum, zeros, ignored)) {
    LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_reduce_prior_centers", KP(mass), K(count),
        K(sum), K(maximum), K(zeros), K(ignored), K(dimension), K(k));
    ret = OB_INVALID_DATA;
  }
  const int64_t capacity = sum / k + (sum % k != 0);
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  }
  else if (OB_FAIL(fill_array(remaining, k, capacity + capacity / 4))) {
    LOG_WARN("IVF merge operation failed", K(ret), "operation", "fill_array");
  }
  else if (OB_FAIL(fill_array(total_mass, k, int64_t(0)))) {
    LOG_WARN("IVF merge operation failed", K(ret), "operation", "fill_array");
  }
  else if (OB_FAIL(fill_array(result, k * dimension, 0.0F))) {
    LOG_WARN("IVF merge operation failed", K(ret), "operation", "fill_array");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    float best = FLT_MAX, second = FLT_MAX;
    int64_t nearest = 0;
    for (int64_t c = 0; c < k; ++c) {
      const float d = distances[i * k + c];
      if (d < best) { second = best; best = d; nearest = c; }
      else if (d < second) { second = d; }
    }
    ret = order.push_back({i, second - best, nearest});
    if (OB_FAIL(ret)) {
      LOG_WARN("HCA operation failed", K(ret), "operation", "order.push_back", K(i), K(count), K(dimension), K(k));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(order.begin(), order.end(), [](const PriorOrder &a, const PriorOrder &b) {
      return a.regret_ != b.regret_ ? a.regret_ > b.regret_ : a.idx_ < b.idx_;
    });
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order.count(); ++i) {
    const PriorOrder &entry = order.at(i);
    int64_t chosen = -1;
    float best = FLT_MAX;
    for (int64_t c = 0; c < k; ++c) {
      const float d = distances[entry.idx_ * k + c];
      if (remaining.at(c) > 0 && d < best) { best = d; chosen = c; }
    }
    if (chosen < 0) { chosen = entry.nearest_; }
    remaining.at(chosen) = std::max<int64_t>(0, remaining.at(chosen) - mass[entry.idx_]);
    total_mass.at(chosen) += mass[entry.idx_];
    hca_simd::axpy(&result.at(chosen * dimension), prior + entry.idx_ * dimension,
        static_cast<float>(mass[entry.idx_]), dimension);
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < k; ++c) {
    if (total_mass.at(c) > 0) {
      hca_simd::axpby(&centers[c * dimension], &result.at(c * dimension),
          1.0F / static_cast<float>(total_mass.at(c)), 0.0F, dimension);
    }
  }
  return ret;
}





int hca_insertion_assign_priors(const float *prior, const int64_t count, const int64_t dimension,
    const float *centers, const int64_t k, float *sums, int64_t *counts)
{
  int ret = OB_SUCCESS;
  if (prior == nullptr || centers == nullptr || sums == nullptr || counts == nullptr
      || count < 0 || dimension <= 0 || k <= 0 || k > INT64_MAX / dimension
      || count > INT64_MAX / dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_insertion_assign_priors", KP(prior),
        KP(centers), KP(sums), KP(counts), K(count), K(dimension), K(k));
    return OB_INVALID_ARGUMENT;
  }
  MEMSET(sums, 0, sizeof(float) * k * dimension);
  MEMSET(counts, 0, sizeof(int64_t) * k);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    int64_t chosen = 0;
    float best = FLT_MAX;
    for (int64_t c = 0; OB_SUCC(ret) && c < k; ++c) {
      const float distance = hca_square_distance(prior + i * dimension, centers + c * dimension, dimension);
      if (!std::isfinite(distance)) {
        LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_insertion_assign_priors", K(distance),
            K(count), K(dimension), K(k));
        ret = OB_INVALID_DATA;
      }
      else if (distance < best) { best = distance; chosen = c; }
    }
    if (OB_SUCC(ret)) {
      ++counts[chosen];
      hca_simd::axpy(sums + chosen * dimension, prior + i * dimension, 1.0F, dimension);
    }
  }
  return ret;
}

int hca_insertion_update_centers(const float *sums, const int64_t *counts, const int64_t k,
    const int64_t dimension, const ObVectorNormalizeInfo *norm_info, float *centers)
{
  int ret = OB_SUCCESS;
  if (sums == nullptr || counts == nullptr || centers == nullptr || k <= 0 || dimension <= 0
      || k > INT64_MAX / dimension) {
    LOG_WARN("invalid HCA argument", "ret", OB_INVALID_ARGUMENT, "operation", "hca_insertion_update_centers", KP(sums),
        KP(counts), KP(centers), K(k), K(dimension));
    return OB_INVALID_ARGUMENT;
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < k; ++c) {
    if (counts[c] < 0) {
      LOG_WARN("invalid HCA data", "ret", OB_INVALID_DATA, "operation", "hca_insertion_update_centers", KP(counts),
          K(c), K(k), K(dimension), "member_count", counts[c]);
      ret = OB_INVALID_DATA;
    }
    else if (counts[c] > 0) {
      hca_simd::axpby(centers + c * dimension, sums + c * dimension,
          1.0F / static_cast<float>(counts[c]), 0.0F, dimension);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(norm_info) &&
        OB_FAIL(hca_normalize_center(centers + c * dimension, dimension, *norm_info))) {
      LOG_WARN("HCA operation failed", K(ret), "operation", "hca_normalize_center", K(k), K(dimension));
    }
  }
  return ret;
}


} // namespace share
} // namespace oceanbase
