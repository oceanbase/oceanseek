/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/vector_index/hca/ob_vector_hca_refine.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_sort.h"
#include "share/vector_index/hca/ob_vector_hca_internal.h"
#include <algorithm>
#include <climits>

namespace oceanbase {
namespace share {

using namespace hca_internal;

namespace {

// Candidate offset zero is always the current A0 cid. Remaining offsets are
// either every center or the current cid's materialized neighborhood.
int get_refine_candidate_count(const ObHcaNeighborhoodStore *neighborhood,
                               const int64_t center_count,
                               const int32_t owner_cid,
                               int64_t &candidate_count) {
  int ret = OB_SUCCESS;
  if (owner_cid < 0 || owner_cid >= center_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca refine candidate owner", K(ret), K(owner_cid), K(center_count));
  } else if (OB_ISNULL(neighborhood)) {
    candidate_count = center_count;
  } else if (neighborhood->center_count() != center_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hca refine neighborhood center count mismatch", K(ret), K(neighborhood->center_count()), K(center_count));
  } else {
    const int64_t neighbors = neighborhood->neighbor_count(owner_cid);
    if (neighbors < 0 || neighbors > OB_HCA_NEIGHBOR_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hca refine neighbor count", K(ret), K(owner_cid), K(neighbors));
    } else {
      candidate_count = neighbors + 1;
    }
  }
  return ret;
}

int get_refine_candidate_cid(const ObHcaNeighborhoodStore *neighborhood,
                             const int64_t center_count,
                             const int32_t owner_cid,
                             const int64_t candidate_offset,
                             int32_t &candidate_cid) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(neighborhood)) {
    if (candidate_offset < 0 || candidate_offset >= center_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid all-center hca refine candidate offset", K(ret), K(candidate_offset), K(center_count));
    } else {
      candidate_cid = static_cast<int32_t>(candidate_offset);
    }
  } else if (candidate_offset == 0) {
    candidate_cid = owner_cid;
  } else if (OB_FAIL(neighborhood->get_neighbor(owner_cid, candidate_offset - 1, candidate_cid))) {
    LOG_WARN("failed to read hca refine neighbor", K(ret), K(owner_cid), K(candidate_offset));
  }
  if (OB_SUCC(ret) && (candidate_cid < 0 || candidate_cid >= center_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hca refine candidate cid", K(ret), K(owner_cid),
             K(candidate_offset), K(candidate_cid), K(center_count));
  }
  return ret;
}

int find_nearest_refine_center(const float *vector, const int32_t old_cid,
                               const ObHcaCenterStore &centers,
                               const ObHcaNeighborhoodStore *neighborhood,
                               int32_t &new_cid) {
  int ret = OB_SUCCESS;
  const float *center = nullptr;
  if (OB_ISNULL(vector) || old_cid < 0 || old_cid >= centers.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nearest hca refine center argument", K(ret), KP(vector), K(old_cid), K(centers.count()));
  } else if (OB_FAIL(centers.get_center(old_cid, center))) {
    LOG_WARN("failed to read current hca refine center", K(ret), K(old_cid));
  } else if (OB_ISNULL(center)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current hca refine center is null", K(ret), K(old_cid));
  } else {
    new_cid = old_cid;
    float min_distance = hca_square_distance(vector, center, centers.dimension());
    bool scan_candidates = true;
    if (OB_NOT_NULL(neighborhood)) {
      float max_intra_distance = 0.0F;
      if (OB_FAIL(neighborhood->get_max_intra_distance(old_cid, max_intra_distance))) {
        LOG_WARN("failed to read hca refine max intra distance", K(ret), K(old_cid));
      } else {
        // Preserve the reference implementation's fast path: a vector already
        // sufficiently close to its current center skips neighbor evaluation.
        scan_candidates = min_distance >= max_intra_distance;
      }
    }
    int64_t candidate_count = 0;
    if (OB_SUCC(ret) && scan_candidates &&
        OB_FAIL(get_refine_candidate_count(neighborhood, centers.count(), old_cid, candidate_count))) {
      LOG_WARN("failed to get hca refine candidate count", K(ret), K(old_cid));
    }
    for (int64_t offset = 0;
         OB_SUCC(ret) && scan_candidates && offset < candidate_count;
         ++offset) {
      int32_t candidate_cid = -1;
      if (OB_FAIL(get_refine_candidate_cid(neighborhood, centers.count(), old_cid, offset, candidate_cid))) {
        LOG_WARN("failed to get hca refine candidate cid", K(ret), K(old_cid), K(offset));
      } else if (candidate_cid == old_cid) {
      } else if (OB_FAIL(centers.get_center(candidate_cid, center))) {
        LOG_WARN("failed to read hca refine candidate center", K(ret), K(old_cid), K(candidate_cid));
      } else if (OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hca refine candidate center is null", K(ret), K(old_cid), K(candidate_cid));
      } else {
        const float distance = hca_square_distance(vector, center, centers.dimension());
        if (distance < min_distance) {
          min_distance = distance;
          new_cid = candidate_cid;
        }
      }
    }
  }
  return ret;
}

} // namespace

class ObHcaRefineTrainerImpl {
public:
  ObHcaRefineTrainerImpl(const ObHcaVectorAccessor &v,
                         const ObHcaCenterStore &c,
                         const ObHcaAssignmentStore &a,
                         const ObHcaNeighborhoodStore *n,
                         const ObHcaRefineParam &p,
                         const ObVectorNormalizeInfo *norm, ObHcaCenterStore &w,
                         common::ObIAllocator &allocator)
      : all_vectors_(v), input_centers_(c), input_assignments_(a),
        neighborhood_(n), param_(p), center_norm_info_(norm),
        working_centers_(w), vector_count_(v.count()), center_count_(c.count()),
        dimension_(v.dimension()), sample_size_(0), batch_size_(0),
        candidate_stride_(0), epoch_(0), offset_(0), learning_rate_shift_(0),
        median_(), median_inited_(false), batch_ready_(false),
        distances_(vector_array_allocator(allocator)), sample_indices_(vector_array_allocator(allocator)),
        shuffled_indices_(vector_array_allocator(allocator)), batch_indices_(vector_array_allocator(allocator)),
        local_assignments_(allocator), cumulative_weights_(allocator),
        updated_centers_(allocator), batch_vectors_(allocator),
        row_max_(allocator), row_done_(allocator), finish_phase_(0), alpha_(0.0F),
        update_heads_(allocator), update_tails_(allocator), update_next_(allocator) {}
  int init() {
    int ret = OB_SUCCESS;
    if (vector_count_ <= 0 || center_count_ <= 1 ||
        center_count_ >= vector_count_ || center_count_ > INT32_MAX ||
        dimension_ <= 0 || input_centers_.dimension() != dimension_ ||
        input_assignments_.count() != vector_count_ ||
        working_centers_.capacity() < center_count_ ||
        working_centers_.dimension() != dimension_ || !param_.is_valid() ||
        (OB_ISNULL(neighborhood_) && center_count_ > OB_HCA_NEIGHBOR_COUNT) ||
        (OB_NOT_NULL(neighborhood_) &&
         center_count_ <= OB_HCA_NEIGHBOR_COUNT) ||
        (OB_NOT_NULL(neighborhood_) &&
         neighborhood_->center_count() != center_count_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid hca refine train argument", K(ret), K(vector_count_),
               K(center_count_), K(dimension_), K(param_.is_valid()));
    }
    // WorkingCenters are a private mutable snapshot. C0 remains immutable so
    // the upper layer can retry Refine from the same hierarchy result.
    for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count_; ++cid) {
      const float *center = nullptr;
      if (OB_FAIL(input_centers_.get_center(cid, center))) {
        LOG_WARN("failed to read input hca center", K(ret), K(cid));
      } else if (OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input hca center is null", K(ret), K(cid));
      } else if (OB_FAIL(working_centers_.set_center(cid, center))) {
        LOG_WARN("failed to initialize hca working center", K(ret), K(cid));
      }
    }
    if (OB_SUCC(ret)) {
      sample_size_ = std::min<int64_t>(
          vector_count_, center_count_ > INT64_MAX / param_.samples_per_cluster_
                             ? INT64_MAX
                             : center_count_ * param_.samples_per_cluster_);
      batch_size_ = std::min<int64_t>(
          vector_count_,
          center_count_ > INT64_MAX / 2 ? INT64_MAX : center_count_ * 2);
      candidate_stride_ = center_count_;
      if (OB_NOT_NULL(neighborhood_)) {
        // Neighborhood rows can be shorter after compact/remap, so allocate for
        // the largest row while tracking each sampled row's actual candidate
        // count.
        candidate_stride_ = 1;
        for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count_; ++cid) {
          const int64_t neighbor_count = neighborhood_->neighbor_count(cid);
          if (neighbor_count < 0 || neighbor_count > OB_HCA_NEIGHBOR_COUNT) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid hca refine training neighbor count", K(ret),
                     K(cid), K(neighbor_count));
          } else {
            candidate_stride_ =
                std::max<int64_t>(candidate_stride_, neighbor_count + 1);
          }
        }
      }
      if (OB_SUCCESS != ret) {
      } else if (candidate_stride_ <= 0 || candidate_stride_ > center_count_ ||
                 batch_size_ > INT64_MAX / candidate_stride_) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("hca refine distance matrix element count overflows int64",
                 K(ret), K(batch_size_), K(candidate_stride_));
      } else {
        if (OB_FAIL(allocate_build_array(distances_, batch_size_ * candidate_stride_))) {
          LOG_WARN("failed to allocate hca refine distances_", K(ret),
                   K(batch_size_), K(candidate_stride_));
        } else if (OB_FAIL(allocate_build_array(sample_indices_, sample_size_))) {
          LOG_WARN("failed to allocate hca refine sample indices", K(ret),
                   K(sample_size_));
        } else if (OB_FAIL(allocate_build_array(shuffled_indices_, sample_size_))) {
          LOG_WARN("failed to allocate shuffled hca refine indices", K(ret),
                   K(sample_size_));
        } else if (OB_FAIL(allocate_build_array(batch_indices_, batch_size_))) {
          LOG_WARN("failed to allocate hca refine batch indices", K(ret),
                   K(batch_size_));
        } else if (OB_FAIL(local_assignments_.allocate(batch_size_))) {
          LOG_WARN("failed to allocate local hca refine assignments", K(ret),
                   K(batch_size_));
        } else if (OB_FAIL(cumulative_weights_.allocate(center_count_, true))) {
          LOG_WARN("failed to allocate hca refine weights", K(ret),
                   K(center_count_));
        } else if (OB_FAIL(updated_centers_.allocate(center_count_, true))) {
          LOG_WARN("failed to allocate updated hca refine centers", K(ret),
                   K(center_count_));
        }
        common::ObRandom sample_random;
        sample_random.seed(param_.random_seed_ + 1);
        for (int64_t vector_idx = 0; OB_SUCC(ret) && vector_idx < vector_count_;
             ++vector_idx) {
          // A0 counts seed the lagging cluster-size estimator used by the
          // penalty.
          int32_t cid = -1;
          if (OB_FAIL(input_assignments_.get_assignment(vector_idx, cid))) {
            LOG_WARN("failed to read initial hca refine assignment", K(ret),
                     K(vector_idx));
          } else if (cid < 0 || cid >= center_count_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid initial hca refine assignment", K(ret),
                     K(vector_idx), K(cid), K(center_count_));
          } else {
            cumulative_weights_[cid] += 1.0F;
          }
          if (OB_SUCC(ret)) {
            if (vector_idx < sample_size_) {
              sample_indices_.at(vector_idx) = vector_idx;
            } else {
              const int64_t selected = sample_random.get(0, vector_idx);
              if (selected < 0 || selected > vector_idx) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid hca refine reservoir sample index", K(ret),
                         K(vector_idx), K(selected), K(sample_size_));
              } else if (selected < sample_size_) {
                sample_indices_.at(selected) = vector_idx;
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(batch_vectors_.allocate(batch_size_))) {
      LOG_WARN("failed to allocate HCA refine batch_vectors", K(ret), K(batch_size_));
    }
    if (OB_SUCC(ret) && OB_FAIL(row_max_.allocate(batch_size_))) {
      LOG_WARN("failed to allocate HCA refine row_max", K(ret), K(batch_size_));
    }
    if (OB_SUCC(ret) && OB_FAIL(row_done_.allocate(batch_size_, true))) {
      LOG_WARN("failed to allocate HCA refine row_done", K(ret), K(batch_size_));
    }
    if (OB_SUCC(ret)) {
      learning_rate_shift_ = 3.0F * static_cast<float>(sample_size_) /
                             static_cast<float>(center_count_);
    }
    return ret;
  }
  int get_sample_indices(const share::ObVectorArray<int64_t> *&indices, int64_t &count) const {
    indices = &sample_indices_;
    count = sample_size_;
    return indices != nullptr && count > 0 ? OB_SUCCESS : OB_NOT_INIT;
  }
  int get_epoch_sample_indices(int64_t epoch, share::ObVectorArray<int64_t> &indices, int64_t count,
      int64_t &batch_size) const {
    int ret = OB_SUCCESS;
    batch_size = 0;
    if (epoch < 0 || epoch >= param_.max_iterations_ || count != sample_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid HCA refinement epoch schedule", K(ret), K(epoch), K(count), K(sample_size_));
    } else if (OB_FAIL(shuffle_build_samples(
                   sample_indices_, sample_size_,
        param_.random_seed_ + 2 + static_cast<uint64_t>(epoch), indices))) {
      LOG_WARN("failed to copy HCA refinement epoch schedule", K(ret), K(epoch), K(count));
    } else {
      batch_size = batch_size_;
    }
    return ret;
  }
  int prepare_batch(int64_t &row_count) {
    int ret = OB_SUCCESS;
    row_count = 0;
    if (batch_ready_) {
      LOG_WARN("HCA refine batch already prepared", "ret", OB_STATE_NOT_MATCH, K(epoch_), K(offset_));
      return OB_STATE_NOT_MATCH;
    }
    if (epoch_ >= param_.max_iterations_)
      return OB_ITER_END;
    if (offset_ == 0) {
      int64_t planned_batch_size = 0;
      ret = get_epoch_sample_indices(epoch_, shuffled_indices_, sample_size_, planned_batch_size);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to shuffle HCA refine samples", K(ret), K(epoch_), K(sample_size_));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t row = 0; row < batch_size_; ++row) {
        batch_indices_.at(row) = shuffled_indices_.at((offset_ + row) % sample_size_);
      }
      lib::ob_sort(vector_array_begin(batch_indices_), vector_array_end(batch_indices_));
      ret = all_vectors_.prefetch_vectors(batch_indices_, batch_size_);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to prefetch HCA refine batch", K(ret), K(epoch_), K(offset_), K(batch_size_));
      }
    }
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size_; ++row) {
      ret = all_vectors_.get_vector(batch_indices_.at(row), batch_vectors_[row]);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to read HCA refine batch vector", K(ret), K(row), K(batch_indices_.at(row)));
      } else if (OB_ISNULL(batch_vectors_[row])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null HCA refine batch vector", K(ret), K(row), K(batch_indices_.at(row)));
      }
    }
    if (OB_SUCC(ret)) {
      MEMSET(row_done_.get(), 0, batch_size_ * sizeof(uint8_t));
      row_count = batch_size_;
      batch_ready_ = true;
    }
    return ret;
  }
  int distance_range(int64_t begin, int64_t end) {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 0 || begin < 0 || end <= begin || end > batch_size_) {
      LOG_WARN("invalid HCA refine distance range", "ret", OB_INVALID_ARGUMENT, K(batch_ready_), K(begin),
          K(end), K(batch_size_));
      return OB_INVALID_ARGUMENT;
    }
    for (int64_t row = begin; OB_SUCC(ret) && row < end; ++row) {
      const int64_t vector_idx = batch_indices_.at(row);
      const float *vector = batch_vectors_[row];
      float max_distance = 0.0F;
      int32_t owner_cid = -1;
      int64_t candidate_count = 0;
      if (OB_FAIL(input_assignments_.get_assignment(vector_idx, owner_cid))) {
        LOG_WARN("failed to read sampled hca refine assignment", K(ret), K(row),
                 K(vector_idx));
      } else if (OB_FAIL(get_refine_candidate_count(neighborhood_,
                                                    center_count_, owner_cid,
                                                    candidate_count))) {
        LOG_WARN("failed to get sampled hca refine candidate count", K(ret),
                 K(row), K(vector_idx), K(owner_cid));
      } else if (OB_ISNULL(vector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sampled hca refine vector is null", K(ret), K(row),
                 K(vector_idx));
      } else {
        for (int64_t candidate_offset = 0;
             OB_SUCC(ret) && candidate_offset < candidate_count;
             ++candidate_offset) {
          int32_t candidate_cid = -1;
          const float *center = nullptr;
          if (OB_FAIL(get_refine_candidate_cid(neighborhood_, center_count_,
                                               owner_cid, candidate_offset,
                                               candidate_cid))) {
            LOG_WARN("failed to get sampled hca refine candidate", K(ret),
                     K(row), K(owner_cid), K(candidate_offset));
          } else if (OB_FAIL(
                         working_centers_.get_center(candidate_cid, center))) {
            LOG_WARN("failed to read sampled hca refine center", K(ret), K(row),
                     K(candidate_cid));
          } else if (OB_ISNULL(center)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sampled hca refine center is null", K(ret), K(row),
                     K(candidate_cid));
          } else {
            const float distance =
                hca_square_distance(vector, center, dimension_);
            distances_.at(row * candidate_stride_ + candidate_offset) = distance;
            max_distance = std::max(max_distance, distance);
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_max_[row] = max_distance;
        row_done_[row] = 1;
      }
    }

    return ret;
  }
  int finish_batch() {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 0) {
      LOG_WARN("HCA refine batch is not ready", "ret", OB_STATE_NOT_MATCH, K(epoch_), K(offset_));
      return OB_STATE_NOT_MATCH;
    }
    float max_distance = 0.0F;
    for (int64_t row = 0; row < batch_size_; ++row) {
      if (!row_done_[row]) {
        LOG_WARN("HCA refine distance row is unfinished", "ret", OB_STATE_NOT_MATCH, K(row), K(batch_size_));
        return OB_STATE_NOT_MATCH;
      }
      max_distance = std::max(max_distance, row_max_[row]);
    }
    if (OB_SUCC(ret) && !median_inited_) {
      if (OB_FAIL(median_.init(0.5F, 0.0F, max_distance, 1e-4F, 42))) {
        LOG_WARN("failed to initialize hca refine median estimator", K(ret),
                 K(max_distance));
      } else {
        median_inited_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t row = 0; row < batch_size_; ++row) {
        int32_t owner_cid = -1;
        int64_t candidate_count = 0;
        if (OB_FAIL(input_assignments_.get_assignment(batch_indices_.at(row),
                                                      owner_cid))) {
          LOG_WARN("failed to reread sampled hca refine assignment", K(ret),
                   K(row), K(batch_indices_.at(row)));
          break;
        } else if (OB_FAIL(get_refine_candidate_count(neighborhood_,
                                                      center_count_, owner_cid,
                                                      candidate_count))) {
          LOG_WARN("failed to reread hca refine candidate count", K(ret),
                   K(row), K(owner_cid));
          break;
        }
        for (int64_t candidate_offset = 0; candidate_offset < candidate_count;
             ++candidate_offset) {
          median_.update(
              distances_.at(row * candidate_stride_ + candidate_offset));
        }
      }
    }
    const float alpha = OB_SUCC(ret) ? param_.beta_ * median_.estimate() *
                                           static_cast<float>(center_count_) /
                                           static_cast<float>(vector_count_)
                                     : 0.0F;
    // Phase 2: add the cluster-size penalty and choose one local cid
    // for each sampled vector without changing the persistent A0
    // mapping.
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size_; ++row) {
      int32_t owner_cid = -1;
      int64_t candidate_count = 0;
      if (OB_FAIL(input_assignments_.get_assignment(batch_indices_.at(row),
                                                    owner_cid))) {
        LOG_WARN("failed to read hca refine assignment for penalty", K(ret),
                 K(row), K(batch_indices_.at(row)));
      } else if (OB_FAIL(get_refine_candidate_count(neighborhood_,
                                                    center_count_, owner_cid,
                                                    candidate_count))) {
        LOG_WARN("failed to get hca refine candidates for penalty", K(ret),
                 K(row), K(owner_cid));
      } else {
        int32_t best_cid = -1;
        float best_adjusted = 0.0F;
        for (int64_t candidate_offset = 0;
             OB_SUCC(ret) && candidate_offset < candidate_count;
             ++candidate_offset) {
          int32_t candidate_cid = -1;
          if (OB_FAIL(get_refine_candidate_cid(neighborhood_, center_count_,
                                               owner_cid, candidate_offset,
                                               candidate_cid))) {
            LOG_WARN("failed to get penalized hca refine candidate", K(ret),
                     K(row), K(owner_cid), K(candidate_offset));
          } else {
            const float adjusted = distances_.at(row * candidate_stride_ + candidate_offset)
                + alpha * cumulative_weights_[candidate_cid];
            if (candidate_offset == 0 || adjusted < best_adjusted) {
              best_adjusted = adjusted;
              best_cid = candidate_cid;
            }
          }
        }
        local_assignments_[row] = best_cid;
      }
    }
    // Phase 3: apply SGD updates. Projection is delayed until the
    // entire mini-batch is consumed so vector order within the batch is
    // stable.
    MEMSET(updated_centers_.get(), 0, sizeof(uint8_t) * center_count_);
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size_; ++row) {
      const int32_t cid = local_assignments_[row];
      const float *vector = batch_vectors_[row];
      float *center = nullptr;
      if (cid < 0 || cid >= center_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid local hca refine assignment", K(ret), K(row), K(cid),
                 K(center_count_));
      } else if (OB_FAIL(working_centers_.get_mutable_center(cid, center))) {
        LOG_WARN("failed to get mutable hca refine center", K(ret), K(row),
                 K(cid));
      } else if (OB_ISNULL(vector) || OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hca refine update vector or center is null", K(ret), K(row),
                 K(cid), KP(vector), KP(center));
      } else {
        cumulative_weights_[cid] += 1.0F;
        const float learning_rate =
            1.0F / (cumulative_weights_[cid] + learning_rate_shift_);
        blend_vector(center, vector, learning_rate, 1.0F - learning_rate,
                     dimension_);
        updated_centers_[cid] = 1;
      }
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count_; ++cid) {
      if (updated_centers_[cid] != 0) {
        float *center = nullptr;
        if (OB_FAIL(working_centers_.get_mutable_center(cid, center))) {
          LOG_WARN("failed to get updated mutable hca refine center", K(ret),
                   K(cid));
        } else if (OB_ISNULL(center)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("updated mutable hca refine center is null", K(ret), K(cid));
        } else if (OB_NOT_NULL(center_norm_info_) &&
                   OB_FAIL(hca_normalize_center(center, dimension_, *center_norm_info_))) {
          LOG_WARN("failed to finalize hca refine working center", K(ret),
                   K(cid));
        } else if (OB_FAIL(working_centers_.set_center(cid, center))) {
          LOG_WARN("failed to commit hca refine working center", K(ret),
                   K(cid));
        }
      }
    }
    if (OB_SUCC(ret)) {
      offset_ += batch_size_;
      if (offset_ >= sample_size_) {
        for (int64_t cid = 0; cid < center_count_; ++cid) {
          cumulative_weights_[cid] *= param_.forgetting_factor_;
        }
        offset_ = 0;
        ++epoch_;
      }
      batch_ready_ = false;
    }
    return ret;
  }

  int prepare_assign() {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 0) {
      LOG_WARN("HCA refine batch is not ready", "ret", OB_STATE_NOT_MATCH, K(epoch_), K(offset_));
      return OB_STATE_NOT_MATCH;
    }
    float max_distance = 0.0F;
    for (int64_t row = 0; row < batch_size_; ++row) {
      if (!row_done_[row]) {
        LOG_WARN("HCA refine distance row is unfinished", "ret", OB_STATE_NOT_MATCH, K(row), K(batch_size_));
        return OB_STATE_NOT_MATCH;
      }
      max_distance = std::max(max_distance, row_max_[row]);
    }
    finish_phase_ = -1; // A partially consumed median stream cannot be retried.
    if (OB_SUCC(ret) && !median_inited_) {
      if (OB_FAIL(median_.init(0.5F, 0.0F, max_distance, 1e-4F, 42))) {
        LOG_WARN("failed to initialize hca refine median estimator", K(ret),
                 K(max_distance));
      } else {
        median_inited_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t row = 0; row < batch_size_; ++row) {
        int32_t owner_cid = -1;
        int64_t candidate_count = 0;
        if (OB_FAIL(input_assignments_.get_assignment(batch_indices_.at(row),
                                                      owner_cid))) {
          LOG_WARN("failed to reread sampled hca refine assignment", K(ret),
                   K(row), K(batch_indices_.at(row)));
          break;
        } else if (OB_FAIL(get_refine_candidate_count(neighborhood_,
                                                      center_count_, owner_cid,
                                                      candidate_count))) {
          LOG_WARN("failed to reread hca refine candidate count", K(ret),
                   K(row), K(owner_cid));
          break;
        }
        for (int64_t candidate_offset = 0; candidate_offset < candidate_count;
             ++candidate_offset) {
          median_.update(
              distances_.at(row * candidate_stride_ + candidate_offset));
        }
      }
    }
    alpha_ = OB_SUCC(ret) ? param_.beta_ * median_.estimate() *
                                           static_cast<float>(center_count_) /
                                           static_cast<float>(vector_count_)
                                     : 0.0F;
    if (OB_SUCC(ret)) {
      finish_phase_ = 1;
      MEMSET(row_done_.get(), 0, batch_size_ * sizeof(uint8_t));
    }
    return ret;
  }
  int assign_range(int64_t begin, int64_t end) {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 1 || begin < 0 || end <= begin || end > batch_size_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid HCA penalty range", K(ret), K(begin), K(end), K_(finish_phase), K_(batch_size));
      return ret;
    }
    for (int64_t row = begin; OB_SUCC(ret) && row < end; ++row) {
      int32_t owner_cid = -1;
      int64_t candidate_count = 0;
      if (OB_FAIL(input_assignments_.get_assignment(batch_indices_.at(row),
                                                    owner_cid))) {
        LOG_WARN("failed to read hca refine assignment for penalty", K(ret),
                 K(row), K(batch_indices_.at(row)));
      } else if (OB_FAIL(get_refine_candidate_count(neighborhood_,
                                                    center_count_, owner_cid,
                                                    candidate_count))) {
        LOG_WARN("failed to get hca refine candidates for penalty", K(ret),
                 K(row), K(owner_cid));
      } else {
        int32_t best_cid = -1;
        float best_adjusted = 0.0F;
        for (int64_t candidate_offset = 0;
             OB_SUCC(ret) && candidate_offset < candidate_count;
             ++candidate_offset) {
          int32_t candidate_cid = -1;
          if (OB_FAIL(get_refine_candidate_cid(neighborhood_, center_count_,
                                               owner_cid, candidate_offset,
                                               candidate_cid))) {
            LOG_WARN("failed to get penalized hca refine candidate", K(ret),
                     K(row), K(owner_cid), K(candidate_offset));
          } else {
            const float adjusted = distances_.at(row * candidate_stride_ + candidate_offset)
                + alpha_ * cumulative_weights_[candidate_cid];
            if (candidate_offset == 0 || adjusted < best_adjusted) {
              best_adjusted = adjusted;
              best_cid = candidate_cid;
            }
          }
        }
        local_assignments_[row] = best_cid;
        if (OB_SUCC(ret)) { row_done_[row] = 1; }
      }
    }
    return ret;
  }
  int prepare_update() {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 1) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("HCA penalty phase is not ready", K(ret), K_(finish_phase));
    }
    for (int64_t row = 0; OB_SUCC(ret) && row < batch_size_; ++row) {
      if (!row_done_[row] || local_assignments_[row] < 0 || local_assignments_[row] >= center_count_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("incomplete HCA penalty assignments", K(ret), K(row), K_(batch_size));
      }
    }
    if (OB_SUCC(ret) && update_heads_.count() == 0 && OB_FAIL(update_heads_.allocate(center_count_))) {
      LOG_WARN("failed to allocate HCA update heads", K(ret), K_(center_count));
    }
    if (OB_SUCC(ret) && update_tails_.count() == 0 && OB_FAIL(update_tails_.allocate(center_count_))) {
      LOG_WARN("failed to allocate HCA update tails", K(ret), K_(center_count));
    }
    if (OB_SUCC(ret) && update_next_.count() == 0 && OB_FAIL(update_next_.allocate(batch_size_))) {
      LOG_WARN("failed to allocate HCA update links", K(ret), K_(batch_size));
    }
    if (OB_SUCC(ret)) {
      MEMSET(update_heads_.get(), -1, center_count_ * sizeof(int64_t));
      MEMSET(updated_centers_.get(), 0, center_count_ * sizeof(uint8_t));
      for (int64_t row = 0; row < batch_size_; ++row) {
        const int32_t cid = local_assignments_[row];
        update_next_[row] = -1;
        if (update_heads_[cid] < 0) { update_heads_[cid] = row; }
        else { update_next_[update_tails_[cid]] = row; }
        update_tails_[cid] = row;
      }
      finish_phase_ = 2;
    }
    return ret;
  }
  int update_range(int64_t begin, int64_t end) {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 2 || begin < 0 || end <= begin || end > center_count_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid HCA SGD range", K(ret), K(begin), K(end), K_(finish_phase), K_(center_count));
    }
    for (int64_t cid = begin; OB_SUCC(ret) && cid < end; ++cid) {
      float *center = nullptr;
      if (updated_centers_[cid] != 0) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("HCA SGD range cannot be repeated", K(ret), K(cid), K(begin), K(end));
      } else if (update_heads_[cid] < 0) {
        updated_centers_[cid] = 1;
      } else {
        updated_centers_[cid] = 2; // A partial update is not retryable.
        if (OB_FAIL(working_centers_.get_mutable_center(cid, center))) {
          LOG_WARN("failed to access parallel HCA center", K(ret), K(cid));
        } else if (OB_ISNULL(center)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null parallel HCA center", K(ret), K(cid));
        }
        for (int64_t row = update_heads_[cid]; OB_SUCC(ret) && row >= 0; row = update_next_[row]) {
          const float *vector = batch_vectors_[row];
          if (OB_ISNULL(vector)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null parallel HCA vector", K(ret), K(cid), K(row));
          } else {
            cumulative_weights_[cid] += 1.0F;
            const float learning_rate = 1.0F / (cumulative_weights_[cid] + learning_rate_shift_);
            blend_vector(center, vector, learning_rate, 1.0F - learning_rate, dimension_);
          }
        }
        if (OB_SUCC(ret) && center_norm_info_ != nullptr
            && OB_FAIL(hca_normalize_center(center, dimension_, *center_norm_info_))) {
          LOG_WARN("failed to normalize parallel HCA center", K(ret), K(cid));
        }
        if (OB_SUCC(ret) && OB_FAIL(working_centers_.set_center(cid, center))) {
          LOG_WARN("failed to commit parallel HCA center", K(ret), K(cid));
        }
        if (OB_SUCC(ret)) { updated_centers_[cid] = 1; }
      }
    }
    return ret;
  }
  int finish_update() {
    int ret = OB_SUCCESS;
    if (!batch_ready_ || finish_phase_ != 2) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("HCA SGD phase is not ready", K(ret), K_(finish_phase));
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count_; ++cid) {
      if (updated_centers_[cid] != 1) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("incomplete HCA SGD range", K(ret), K(cid));
      }
    }
    if (OB_SUCC(ret)) {
      offset_ += batch_size_;
      if (offset_ >= sample_size_) {
        for (int64_t cid = 0; cid < center_count_; ++cid) {
          cumulative_weights_[cid] *= param_.forgetting_factor_;
        }
        offset_ = 0;
        ++epoch_;
      }
      batch_ready_ = false;
      finish_phase_ = 0;
    }
    return ret;
  }
  int64_t finish_workspace_bytes() const {
    return (update_heads_.count() + update_tails_.count() + update_next_.count()) * sizeof(int64_t);
  }

private:
  const ObHcaVectorAccessor &all_vectors_;
  const ObHcaCenterStore &input_centers_;
  const ObHcaAssignmentStore &input_assignments_;
  const ObHcaNeighborhoodStore *neighborhood_;
  const ObHcaRefineParam param_;
  const ObVectorNormalizeInfo *center_norm_info_;
  ObHcaCenterStore &working_centers_;
  const int64_t vector_count_;
  const int64_t center_count_;
  const int64_t dimension_;
  int64_t sample_size_;
  int64_t batch_size_;
  int64_t candidate_stride_;
  int64_t epoch_;
  int64_t offset_;
  float learning_rate_shift_;
  OnlineQuantileEstimator median_;
  bool median_inited_;
  bool batch_ready_;
  ObVectorArray<float> distances_;
  ObVectorArray<int64_t> sample_indices_;
  ObVectorArray<int64_t> shuffled_indices_;
  ObVectorArray<int64_t> batch_indices_;
  ScopedArray<int32_t> local_assignments_;
  ScopedArray<float> cumulative_weights_;
  ScopedArray<uint8_t> updated_centers_;
  ScopedArray<const float *> batch_vectors_;
  ScopedArray<float> row_max_;
  ScopedArray<uint8_t> row_done_;
  int finish_phase_;
  float alpha_;
  ScopedArray<int64_t> update_heads_, update_tails_, update_next_;
};

ObHcaRefineTrainer::ObHcaRefineTrainer(common::ObIAllocator &allocator)
    : allocator_(allocator), impl_(nullptr) {}
ObHcaRefineTrainer::~ObHcaRefineTrainer() { reset(); }
void ObHcaRefineTrainer::reset() {
  if (impl_ != nullptr) {
    impl_->~ObHcaRefineTrainerImpl();
    allocator_.free(impl_);
    impl_ = nullptr;
  }
}
int ObHcaRefineTrainer::init(const ObHcaVectorAccessor &vectors,
                             const ObHcaCenterStore &centers,
                             const ObHcaAssignmentStore &assignments,
                             const ObHcaNeighborhoodStore *neighbors,
                             const ObHcaRefineParam &param,
                             const ObVectorNormalizeInfo *norm,
                             ObHcaCenterStore &working_centers) {
  int ret = OB_SUCCESS;
  if (impl_ != nullptr) {
    LOG_WARN("HCA refine trainer already initialized", "ret", OB_INIT_TWICE);
    return OB_INIT_TWICE;
  }
  void *buf = allocator_.alloc(sizeof(ObHcaRefineTrainerImpl));
  if (buf == nullptr) {
    LOG_WARN("failed to allocate HCA refine trainer", "ret", OB_ALLOCATE_MEMORY_FAILED);
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  impl_ = new (buf)
      ObHcaRefineTrainerImpl(vectors, centers, assignments, neighbors, param,
                             norm, working_centers, allocator_);
  if (OB_FAIL(impl_->init())) {
    LOG_WARN("failed to initialize HCA refine trainer implementation", K(ret));
    reset();
  }
  return ret;
}
int ObHcaRefineTrainer::get_sample_indices(const share::ObVectorArray<int64_t> *&indices, int64_t &count) const
{
  indices = nullptr;
  count = 0;
  return impl_ == nullptr ? OB_NOT_INIT : impl_->get_sample_indices(indices, count);
}
int ObHcaRefineTrainer::get_epoch_sample_indices(int64_t epoch, share::ObVectorArray<int64_t> &indices,
    int64_t count, int64_t &batch_size) const {
  int ret = OB_SUCCESS;
  batch_size = 0;
  if (impl_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("HCA refine trainer is not initialized for epoch planning", K(ret), K(epoch));
  } else {
    ret = impl_->get_epoch_sample_indices(epoch, indices, count, batch_size);
  }
  return ret;
}
int ObHcaRefineTrainer::prepare_batch(int64_t &row_count) {
  if (impl_ == nullptr) {
    int ret = OB_NOT_INIT;
    LOG_WARN("HCA refine trainer is not initialized for prepare_batch", K(ret));
  }
  return impl_ == nullptr ? OB_NOT_INIT : impl_->prepare_batch(row_count);
}
int ObHcaRefineTrainer::distance_range(int64_t begin, int64_t end) {
  if (impl_ == nullptr) {
    int ret = OB_NOT_INIT;
    LOG_WARN("HCA refine trainer is not initialized for distance_range", K(ret));
  }
  return impl_ == nullptr ? OB_NOT_INIT : impl_->distance_range(begin, end);
}
int ObHcaRefineTrainer::finish_batch() {
  if (impl_ == nullptr) {
    int ret = OB_NOT_INIT;
    LOG_WARN("HCA refine trainer is not initialized for finish_batch", K(ret));
  }
  return impl_ == nullptr ? OB_NOT_INIT : impl_->finish_batch();
}

int ObHcaRefineTrainer::prepare_assign() {
  int ret = impl_ == nullptr ? OB_NOT_INIT : impl_->prepare_assign();
  if (OB_FAIL(ret)) { LOG_WARN("HCA staged refinement failed", K(ret), "operation", "prepare_assign"); }
  return ret;
}

int ObHcaRefineTrainer::assign_range(int64_t begin, int64_t end) {
  int ret = impl_ == nullptr ? OB_NOT_INIT : impl_->assign_range(begin, end);
  if (OB_FAIL(ret)) { LOG_WARN("HCA staged refinement failed", K(ret), "operation", "assign_range"); }
  return ret;
}

int ObHcaRefineTrainer::prepare_update() {
  int ret = impl_ == nullptr ? OB_NOT_INIT : impl_->prepare_update();
  if (OB_FAIL(ret)) { LOG_WARN("HCA staged refinement failed", K(ret), "operation", "prepare_update"); }
  return ret;
}

int ObHcaRefineTrainer::update_range(int64_t begin, int64_t end) {
  int ret = impl_ == nullptr ? OB_NOT_INIT : impl_->update_range(begin, end);
  if (OB_FAIL(ret)) { LOG_WARN("HCA staged refinement failed", K(ret), "operation", "update_range"); }
  return ret;
}

int ObHcaRefineTrainer::finish_update() {
  int ret = impl_ == nullptr ? OB_NOT_INIT : impl_->finish_update();
  if (OB_FAIL(ret)) { LOG_WARN("HCA staged refinement failed", K(ret), "operation", "finish_update"); }
  return ret;
}

int64_t ObHcaRefineTrainer::finish_workspace_bytes() const {
  return impl_ == nullptr ? 0 : impl_->finish_workspace_bytes();
}

int hca_refine_train_centers(const ObHcaVectorAccessor &all_vectors,
                             const ObHcaCenterStore &input_centers,
                             const ObHcaAssignmentStore &input_assignments,
                             const ObHcaNeighborhoodStore *neighborhood,
                             const ObHcaRefineParam &param,
                             const ObVectorNormalizeInfo *center_norm_info,
                             ObHcaCenterStore &working_centers,
                             common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  ObHcaRefineTrainer trainer(allocator);
  if (OB_FAIL(trainer.init(all_vectors, input_centers, input_assignments,
                           neighborhood, param, center_norm_info,
                           working_centers))) {
    LOG_WARN("failed to initialize hca refine trainer", K(ret));
  }
  while (OB_SUCC(ret)) {
    int64_t rows = 0;
    if (OB_FAIL(trainer.prepare_batch(rows))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to prepare HCA refine batch", K(ret), K(rows));
      }
      break;
    } else if (OB_FAIL(trainer.distance_range(0, rows))) {
      LOG_WARN("failed to calculate HCA refine batch distances", K(ret), K(rows));
    } else if (OB_FAIL(trainer.finish_batch())) {
      LOG_WARN("failed to finish HCA refine batch", K(ret), K(rows));
    }
  }
  return ret;
}

int hca_refine_assign_range(const ObHcaVectorBatch &vectors,
                            const ObHcaCenterStore &working_centers,
                            const ObHcaAssignmentStore &input_assignments,
                            const ObHcaNeighborhoodStore *neighborhood,
                            ObHcaAssignmentStore &refined_assignments,
                            ObHcaCidMemberStore &refined_members,
                            ObHcaChangedCidSet &local_changed_cids) {
  int ret = OB_SUCCESS;
  if (vectors.dimension() <= 0 ||
      vectors.dimension() != working_centers.dimension() ||
      working_centers.count() <= 0 ||
      (OB_ISNULL(neighborhood) &&
       working_centers.count() > OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(neighborhood) &&
       working_centers.count() <= OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(neighborhood) &&
       neighborhood->center_count() != working_centers.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca refine assignment argument", K(ret),
             K(vectors.count()), K(vectors.dimension()),
             K(working_centers.count()), K(working_centers.dimension()),
             KP(neighborhood));
  }
  for (int64_t pos = 0; OB_SUCC(ret) && pos < vectors.count(); ++pos) {
    int64_t vector_idx = -1;
    const float *vector = nullptr;
    int32_t old_cid = -1;
    int32_t new_cid = -1;
    if (OB_FAIL(vectors.get_vector(pos, vector_idx, vector))) {
      LOG_WARN("failed to read final hca refine vector", K(ret), K(pos));
    } else if (OB_FAIL(input_assignments.get_assignment(vector_idx, old_cid))) {
      LOG_WARN("failed to read final hca refine input assignment", K(ret), K(pos), K(vector_idx));
    } else if (OB_FAIL(find_nearest_refine_center(vector, old_cid, working_centers, neighborhood, new_cid))) {
      LOG_WARN("failed to find final hca refine center", K(ret), K(pos), K(vector_idx), K(old_cid));
    } else if (OB_FAIL(refined_members.append_member(new_cid, vector_idx))) {
      LOG_WARN("failed to append final hca refine member", K(ret), K(pos), K(vector_idx), K(new_cid));
    } else if (OB_FAIL(refined_assignments.set_assignment(vector_idx, new_cid))) {
      LOG_WARN("failed to write final hca refine assignment", K(ret), K(pos), K(vector_idx), K(new_cid));
    } else if (new_cid != old_cid) {
      // Both the source and destination must be recomputed after a migration.
      if (OB_FAIL(local_changed_cids.set_changed(old_cid))) {
        LOG_WARN("failed to mark source hca refine cid changed", K(ret), K(old_cid), K(new_cid));
      } else if (OB_FAIL(local_changed_cids.set_changed(new_cid))) {
        LOG_WARN("failed to mark destination hca refine cid changed", K(ret), K(old_cid), K(new_cid));
      }
    }
  }
  return ret;
}

int hca_refine_update_centers_range(
    const ObHcaVectorAccessor &all_vectors,
    const ObHcaCidMemberStore &refined_members,
    const ObHcaChangedCidSet &changed_cids,
    const int64_t begin_cid,
    const int64_t end_cid,
    const ObVectorNormalizeInfo *center_norm_info,
    ObHcaCenterStore &working_centers,
    common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  const int64_t dimension = all_vectors.dimension();
  ScopedArray<float> sum(allocator);
  if (dimension <= 0 || working_centers.dimension() != dimension ||
      refined_members.cid_count() < working_centers.count() ||
      OB_FAIL(validate_range(begin_cid, end_cid, working_centers.count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca refine center update argument", K(ret), K(dimension),
             K(working_centers.count()), K(working_centers.dimension()),
             K(refined_members.cid_count()), K(begin_cid), K(end_cid));
  } else if (OB_FAIL(sum.allocate(dimension, true))) {
    LOG_WARN("failed to allocate hca refine center sum", K(ret), K(dimension));
  }
  for (int64_t cid = begin_cid; OB_SUCC(ret) && cid < end_cid; ++cid) {
    const int64_t count = refined_members.member_count(cid);
    if (count < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid refined hca member count", K(ret), K(cid), K(count));
    } else if (changed_cids.has_changed(cid) && count > 0) {
      // Only changed centers need an exact full-data mean. Unchanged centers
      // retain the Balanced AS result, matching the reference behavior.
      zero_vector(sum.get(), dimension);
      for (int64_t offset = 0; OB_SUCC(ret) && offset < count; ++offset) {
        int64_t vector_idx = -1;
        const float *vector = nullptr;
        if (OB_FAIL(refined_members.get_member(cid, offset, vector_idx))) {
          LOG_WARN("failed to read refined hca member", K(ret), K(cid), K(offset));
        } else if (OB_FAIL(all_vectors.get_vector(vector_idx, vector))) {
          LOG_WARN("failed to read refined hca member vector", K(ret), K(cid), K(offset), K(vector_idx));
        } else if (OB_ISNULL(vector)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("refined hca member vector is null", K(ret), K(cid), K(offset), K(vector_idx));
        } else {
          add_scaled_vector(sum.get(), vector, 1.0F, dimension);
        }
      }
      float *center = nullptr;
      if (OB_SUCC(ret) &&
          OB_FAIL(working_centers.get_mutable_center(cid, center))) {
        LOG_WARN("failed to get mutable refined hca center", K(ret), K(cid));
      } else if (OB_SUCC(ret) && OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mutable refined hca center is null", K(ret), K(cid));
      } else if (OB_SUCC(ret)) {
        const float inverse_count = 1.0F / static_cast<float>(count);
        for (int64_t d = 0; d < dimension; ++d) {
          center[d] = sum[d] * inverse_count;
        }
        if (OB_NOT_NULL(center_norm_info) &&
            OB_FAIL(hca_normalize_center(center, dimension, *center_norm_info))) {
          LOG_WARN("failed to normalize refined hca center", K(ret), K(cid));
        } else if (OB_FAIL(working_centers.set_center(cid, center))) {
          LOG_WARN("failed to commit refined hca center", K(ret), K(cid));
        }
      }
    }
  }
  return ret;
}

int hca_refine_compact_clusters(
    const ObHcaCenterStore &working_centers,
    const ObHcaCidMemberStore &refined_members,
    const ObHcaNeighborhoodStore *input_neighborhood,
    ObHcaCenterStore &output_centers,
    ObHcaCidMemberStore &output_members,
    ObHcaNeighborhoodStore *output_neighborhood,
    ObHcaCidRemapStore &old_to_new,
    int64_t &output_center_count,
    common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  output_center_count = 0;
  const int64_t old_center_count = working_centers.count();
  if (old_center_count <= 0 || old_center_count > INT32_MAX ||
      working_centers.dimension() <= 0 ||
      refined_members.cid_count() < old_center_count ||
      output_centers.capacity() < old_center_count ||
      output_centers.dimension() != working_centers.dimension() ||
      old_to_new.count() < old_center_count ||
      (OB_ISNULL(input_neighborhood) &&
       old_center_count > OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(input_neighborhood) &&
       old_center_count <= OB_HCA_NEIGHBOR_COUNT) ||
      (OB_NOT_NULL(input_neighborhood) &&
       input_neighborhood->center_count() != old_center_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hca refine compact argument", K(ret),
             K(old_center_count), K(working_centers.dimension()),
             K(refined_members.cid_count()), K(output_centers.capacity()),
             K(output_centers.dimension()), K(old_to_new.count()), KP(input_neighborhood), KP(output_neighborhood));
  }
  for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_center_count;
       ++old_cid) {
    // Pass 1 is O(K): identify empty centers and generate a dense remap.
    const int64_t count = refined_members.member_count(old_cid);
    if (count < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid refined hca center count", K(ret), K(old_cid), K(count));
    } else if (count == 0) {
      if (OB_FAIL(old_to_new.set_new_cid(old_cid, -1))) {
        LOG_WARN("failed to write empty refined hca cid remap", K(ret), K(old_cid));
      }
    } else {
      if (OB_FAIL(old_to_new.set_new_cid(old_cid, static_cast<int32_t>(output_center_count++)))) {
        LOG_WARN("failed to write refined hca cid remap", K(ret), K(old_cid), K(output_center_count));
      }
    }
  }
  for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_center_count;
       ++old_cid) {
    // Pass 2 copies centers and rebinds whole member rows. Assignment remapping
    // is intentionally deferred to the parallel A1/SOAR stage.
    int32_t new_cid = -1;
    if (OB_FAIL(old_to_new.get_new_cid(old_cid, new_cid))) {
      LOG_WARN("failed to read refined hca cid remap", K(ret), K(old_cid));
    } else if (new_cid >= 0) {
      const float *center = nullptr;
      if (OB_FAIL(working_centers.get_center(old_cid, center))) {
        LOG_WARN("failed to read compacted hca center", K(ret), K(old_cid), K(new_cid));
      } else if (OB_ISNULL(center)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compacted hca center is null", K(ret), K(old_cid), K(new_cid));
      } else if (OB_FAIL(output_centers.set_center(new_cid, center))) {
        LOG_WARN("failed to write compacted hca center", K(ret), K(old_cid), K(new_cid));
      } else if (OB_FAIL(output_members.bind_row(new_cid, refined_members, old_cid))) {
        LOG_WARN("failed to bind compacted hca member row", K(ret), K(old_cid), K(new_cid));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(output_neighborhood) &&
      OB_NOT_NULL(input_neighborhood) &&
      output_center_count > OB_HCA_NEIGHBOR_COUNT) {
    ScopedArray<int32_t> remapped_neighbors(allocator);
    if (OB_FAIL(remapped_neighbors.allocate(OB_HCA_NEIGHBOR_COUNT))) {
      LOG_WARN("failed to allocate remapped hca neighbors", K(ret), K(OB_HCA_NEIGHBOR_COUNT));
    }
    for (int64_t old_cid = 0; OB_SUCC(ret) && old_cid < old_center_count;
         ++old_cid) {
      // Preserve input neighbor order while filtering removed and duplicate
      // neighbor cids. G1 is a remap of G0, not a rebuilt neighborhood graph.
      int32_t new_cid = -1;
      if (OB_FAIL(old_to_new.get_new_cid(old_cid, new_cid))) {
        LOG_WARN("failed to read hca cid remap for neighborhood", K(ret), K(old_cid));
      } else if (new_cid >= 0) {
        int64_t kept = 0;
        const int64_t neighbor_count = input_neighborhood->neighbor_count(old_cid);
        for (int64_t offset = 0; OB_SUCC(ret) && offset < neighbor_count;
             ++offset) {
          int32_t old_neighbor = -1;
          int32_t new_neighbor = -1;
          if (OB_FAIL(input_neighborhood->get_neighbor(old_cid, offset, old_neighbor))) {
            LOG_WARN("failed to read hca neighbor for remap", K(ret), K(old_cid), K(offset));
          } else if (OB_FAIL(old_to_new.get_new_cid(old_neighbor, new_neighbor))) {
            LOG_WARN("failed to remap hca neighbor cid", K(ret), K(old_cid), K(old_neighbor));
          } else if (new_neighbor >= 0 && new_neighbor != new_cid) {
            bool duplicate = false;
            for (int64_t i = 0; !duplicate && i < kept; ++i) {
              duplicate = remapped_neighbors[i] == new_neighbor;
            }
            if (!duplicate && kept < OB_HCA_NEIGHBOR_COUNT) {
              remapped_neighbors[kept++] = new_neighbor;
            }
          }
        }
        float max_intra_distance = 0.0F;
        if (OB_SUCC(ret) && OB_FAIL(input_neighborhood->get_max_intra_distance(old_cid, max_intra_distance))) {
          LOG_WARN("failed to read remapped hca max intra distance", K(ret), K(old_cid), K(new_cid));
        } else if (OB_SUCC(ret) && OB_FAIL(output_neighborhood->set_row(
                                       new_cid, remapped_neighbors.get(), kept, max_intra_distance))) {
          LOG_WARN("failed to write remapped hca neighborhood row", K(ret), K(old_cid), K(new_cid), K(kept));
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
