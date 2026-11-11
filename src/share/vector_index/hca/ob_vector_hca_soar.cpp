/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/hca/ob_vector_hca_soar.h"

#include "lib/oblog/ob_log.h"
#include "share/vector_index/hca/ob_vector_hca_internal.h"
#include "share/vector_type/ob_vector_soar_distance.h"
#include <climits>

namespace oceanbase {
namespace share {

using namespace hca_internal;

namespace {

// Evaluate candidates four at a time through the existing SIMD-aware kernel,
// then process the scalar tail without materializing per-candidate residuals.
int update_best_soar_candidate(
    const float *vector, const float *primary_residual,
    const float primary_distance, const int64_t dimension, const float lambda,
    const ObHcaCenterStore &centers, const int32_t *candidate_cids,
    const int64_t candidate_count, int32_t &best_cid, float &best_distance) {
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  float distances[4] = {0.0F, 0.0F, 0.0F, 0.0F};
  for (; OB_SUCC(ret) && offset + 4 <= candidate_count; offset += 4) {
    const float *candidate_centers[4] = {nullptr, nullptr, nullptr, nullptr};
    for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
      if (OB_FAIL(centers.get_center(candidate_cids[offset + i], candidate_centers[i]))) {
        LOG_WARN("failed to read bulk hca soar candidate center", K(ret),
                 K(offset), K(i), K(candidate_cids[offset + i]));
      } else if (OB_ISNULL(candidate_centers[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bulk hca soar candidate center is null", K(ret), K(offset), K(i), K(candidate_cids[offset + i]));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObVectorSoarDistance<float>::soar_distance_bulk4(
          vector, candidate_centers[0], candidate_centers[1],
          candidate_centers[2], candidate_centers[3], primary_residual, dimension, lambda, primary_distance, distances);
      for (int64_t i = 0; i < 4; ++i) {
        if (best_cid < 0 || distances[i] < best_distance) {
          best_cid = candidate_cids[offset + i];
          best_distance = distances[i];
        }
      }
    }
  }
  for (; OB_SUCC(ret) && offset < candidate_count; ++offset) {
    const float *candidate_center = nullptr;
    if (OB_FAIL(centers.get_center(candidate_cids[offset], candidate_center))) {
      LOG_WARN("failed to read scalar hca soar candidate center", K(ret), K(offset), K(candidate_cids[offset]));
    } else if (OB_ISNULL(candidate_center)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar hca soar candidate center is null", K(ret), K(offset), K(candidate_cids[offset]));
    } else {
      const float distance = common::ObVectorSoarDistance<float>::soar_distance(
          vector, candidate_center, primary_residual, dimension, lambda, primary_distance);
      if (best_cid < 0 || distance < best_distance) {
        best_cid = candidate_cids[offset];
        best_distance = distance;
      }
    }
  }
  return ret;
}

// A2 and its reverse member row are one stage output. A failure leaves the
// caller-owned stage output invalid, matching the other Assignment methods.
int write_secondary_result(const int64_t vector_idx,
                           const int32_t secondary_cid,
                           ObHcaAssignmentStore &secondary_assignments,
                           ObHcaCidMemberStore &secondary_members) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(secondary_assignments.set_assignment(vector_idx, secondary_cid))) {
    LOG_WARN("failed to write secondary hca soar assignment", K(ret), K(vector_idx), K(secondary_cid));
  } else if (secondary_cid >= 0 && OB_FAIL(secondary_members.append_member(secondary_cid, vector_idx))) {
    LOG_WARN("failed to append secondary hca soar member", K(ret), K(vector_idx), K(secondary_cid));
  }
  return ret;
}

} // namespace

int hca_assign_soar_range(const ObHcaVectorBatch &vectors,
                          const ObHcaCenterStore &centers,
                          const ObHcaAssignmentStore &refined_assignments,
                          const ObHcaCidRemapStore &old_to_new,
                          const ObHcaNeighborhoodStore *neighborhood,
                          const ObHcaSoarParam &param,
                          ObHcaAssignmentStore &primary_assignments,
                          ObHcaAssignmentStore *secondary_assignments,
                          ObHcaCidMemberStore *secondary_members,
                          common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  const int64_t dimension = vectors.dimension();
  const bool soar_enabled = OB_NOT_NULL(secondary_assignments);
  ScopedArray<float> primary_residual(allocator);
  ScopedArray<int32_t> candidate_cids(allocator);
  const int64_t candidate_capacity = OB_ISNULL(neighborhood) ? centers.count() : OB_HCA_NEIGHBOR_COUNT;
  if (dimension <= 0 || centers.dimension() != dimension ||
      centers.count() <= 0 || centers.count() > INT32_MAX ||
      !param.is_valid() ||
      (OB_ISNULL(neighborhood) && centers.count() > OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(neighborhood) && centers.count() <= OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(neighborhood) &&
       neighborhood->center_count() != centers.count()) ||
      (soar_enabled != OB_NOT_NULL(secondary_members))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca soar assignment argument", K(ret),
             K(vectors.count()), K(dimension), K(centers.count()),
             K(centers.dimension()), K(param.is_valid()), K(soar_enabled),
             KP(neighborhood), KP(secondary_members));
  } else if (soar_enabled && OB_FAIL(primary_residual.allocate(dimension))) {
    LOG_WARN("failed to allocate hca soar primary residual", K(ret), K(dimension));
  } else if (soar_enabled &&
             OB_FAIL(candidate_cids.allocate(candidate_capacity))) {
    LOG_WARN("failed to allocate hca soar candidate cids", K(ret), K(candidate_capacity));
  }
  // Every successful vector first writes its compacted A1 cid. A2 evaluation
  // is optional and never changes the primary assignment.
  for (int64_t pos = 0; OB_SUCC(ret) && pos < vectors.count(); ++pos) {
    int64_t vector_idx = -1;
    const float *vector = nullptr;
    int32_t old_primary_cid = -1;
    int32_t primary_cid = -1;
    if (OB_FAIL(vectors.get_vector(pos, vector_idx, vector))) {
      LOG_WARN("failed to read hca soar vector", K(ret), K(pos));
    } else if (OB_FAIL(refined_assignments.get_assignment(vector_idx, old_primary_cid))) {
      LOG_WARN("failed to read refined hca soar assignment", K(ret), K(pos), K(vector_idx));
    } else if (OB_FAIL(old_to_new.get_new_cid(old_primary_cid, primary_cid))) {
      LOG_WARN("failed to remap primary hca soar cid", K(ret), K(pos), K(vector_idx), K(old_primary_cid));
    } else if (primary_cid < 0 || primary_cid >= centers.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid primary hca soar cid", K(ret), K(pos), K(vector_idx),
               K(old_primary_cid), K(primary_cid), K(centers.count()));
    } else if (OB_FAIL(primary_assignments.set_assignment(vector_idx, primary_cid))) {
      LOG_WARN("failed to write primary hca soar assignment", K(ret), K(pos), K(vector_idx), K(primary_cid));
    } else if (!soar_enabled) {
    } else if (centers.count() <= 1) {
      if (OB_FAIL(write_secondary_result(vector_idx, -1, *secondary_assignments, *secondary_members))) {
        LOG_WARN("failed to write single-center hca soar result", K(ret), K(vector_idx));
      }
    } else if (OB_ISNULL(vector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hca soar vector is null", K(ret), K(pos), K(vector_idx));
    } else {
      const float *primary_center = nullptr;
      if (OB_FAIL(centers.get_center(primary_cid, primary_center))) {
        LOG_WARN("failed to read primary hca soar center", K(ret), K(pos), K(vector_idx), K(primary_cid));
      } else if (OB_ISNULL(primary_center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary hca soar center is null", K(ret), K(pos), K(vector_idx), K(primary_cid));
      } else {
        for (int64_t d = 0; d < dimension; ++d) {
          primary_residual[d] = vector[d] - primary_center[d];
        }
        const float primary_distance = hca_square_distance(vector, primary_center, dimension);
        if (primary_distance <= param.min_primary_distance_) {
          // The SOAR projection term divides by primary_distance; an exact or
          // near-exact primary hit therefore has no secondary assignment.
          if (OB_FAIL(write_secondary_result(vector_idx, -1, *secondary_assignments, *secondary_members))) {
            LOG_WARN("failed to write exact-hit hca soar result", K(ret),
                     K(vector_idx), K(primary_cid), K(primary_distance));
          }
        } else {
          int64_t candidate_count = 0;
          if (OB_ISNULL(neighborhood)) {
            // Small-K mode has no materialized G1 and scans every other center.
            for (int64_t cid = 0; cid < centers.count(); ++cid) {
              if (cid != primary_cid) {
                candidate_cids[candidate_count++] = static_cast<int32_t>(cid);
              }
            }
          } else {
            // Large-K mode restricts SOAR to the compacted primary
            // neighborhood.
            const int64_t neighbor_count = neighborhood->neighbor_count(primary_cid);
            if (neighbor_count < 0 || neighbor_count > candidate_capacity) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid hca soar neighbor count", K(ret),
                       K(primary_cid), K(neighbor_count), K(candidate_capacity));
            }
            for (int64_t offset = 0; OB_SUCC(ret) && offset < neighbor_count;
                 ++offset) {
              int32_t candidate_cid = -1;
              if (OB_FAIL(neighborhood->get_neighbor(primary_cid, offset, candidate_cid))) {
                LOG_WARN("failed to read hca soar neighbor", K(ret), K(primary_cid), K(offset));
              } else if (candidate_cid < 0 ||
                         candidate_cid >= centers.count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid hca soar neighbor cid", K(ret),
                         K(primary_cid), K(offset), K(candidate_cid), K(centers.count()));
              } else if (candidate_cid != primary_cid) {
                candidate_cids[candidate_count++] = candidate_cid;
              }
            }
          }
          int32_t secondary_cid = -1;
          float secondary_distance = 0.0F;
          if (OB_SUCC(ret) && candidate_count > 0 &&
              OB_FAIL(update_best_soar_candidate(
                  vector, primary_residual.get(), primary_distance, dimension,
                  param.lambda_, centers, candidate_cids.get(), candidate_count,
                  secondary_cid, secondary_distance))) {
            LOG_WARN("failed to select secondary hca soar center", K(ret),
                     K(vector_idx), K(primary_cid), K(candidate_count));
          }
          if (OB_SUCC(ret) &&
              OB_FAIL(write_secondary_result(vector_idx, secondary_cid, *secondary_assignments, *secondary_members))) {
            LOG_WARN("failed to write hca soar result", K(ret), K(vector_idx), K(primary_cid), K(secondary_cid));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
