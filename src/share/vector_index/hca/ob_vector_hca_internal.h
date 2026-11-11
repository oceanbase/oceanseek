/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_INTERNAL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_INTERNAL_H_

#include "lib/random/ob_random.h"
#include "lib/utility/ob_sort.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "share/vector_index/hca/ob_vector_hca_simd.h"
#include <algorithm>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>

namespace oceanbase {
namespace share {
namespace hca_internal {

/**
 * Accumulate one scaled vector into a center sum:
 * destination[d] = scale * source[d] + destination[d].
 */
inline void add_scaled_vector(float *destination, const float *source, const float scale, const int64_t dimension) {
  hca_simd::axpy(destination, source, scale, dimension);
}

/**
 * Move a center toward a source vector:
 * destination[d] = source_scale * source[d]
 *                + destination_scale * destination[d].
 */
inline void blend_vector(float *destination, const float *source,
                         const float source_scale, const float destination_scale, const int64_t dimension) {
  hca_simd::axpby(destination, source, source_scale, destination_scale, dimension);
}

/**
 * RAII wrapper for trivially destructible task-local arrays.
 *
 * It uses the caller's allocator, checks integer byte-size overflow and frees
 * successfully allocated memory on every early-return path. Allocation policy
 * and memory limits remain the responsibility of the supplied allocator.
 */
template <typename T> class ScopedArray {
public:
  explicit ScopedArray(common::ObIAllocator &allocator)
      : allocator_(allocator), ptr_(nullptr), count_(0) {}
  ~ScopedArray() { reset(); }

  int allocate(const int64_t count, const bool zero = false) {
    int ret = OB_SUCCESS;
    reset();
    if (count < 0 ||
        (count > 0 && count > INT64_MAX / static_cast<int64_t>(sizeof(T)))) {
      ret = OB_SIZE_OVERFLOW;
    } else if (count > 0) {
      const int64_t bytes = count * static_cast<int64_t>(sizeof(T));
      if (OB_ISNULL(ptr_ = static_cast<T *>(allocator_.alloc(bytes)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        count_ = count;
        if (zero) {
          MEMSET(ptr_, 0, bytes);
        }
      }
    }
    return ret;
  }

  void reset() {
    if (OB_NOT_NULL(ptr_)) {
      allocator_.free(ptr_);
      ptr_ = nullptr;
      count_ = 0;
    }
  }

  T *get() { return ptr_; }
  const T *get() const { return ptr_; }
  int64_t count() const { return count_; }
  T &operator[](const int64_t idx) { return ptr_[idx]; }
  const T &operator[](const int64_t idx) const { return ptr_[idx]; }

private:
  common::ObIAllocator &allocator_;
  T *ptr_;
  int64_t count_;
  DISALLOW_COPY_AND_ASSIGN(ScopedArray);
};

// Convert a local ObRandom stream to [0, 1) values. Exact Java RNG parity is
// intentionally not required; only fixed-seed repeatability is required.
inline float random_unit_float(common::ObRandom &random) {
  const uint64_t bits = static_cast<uint64_t>(random.get());
  return static_cast<float>(bits >> 40) / static_cast<float>(1U << 24);
}

inline double random_unit_double(common::ObRandom &random) {
  const uint64_t bits = static_cast<uint64_t>(random.get());
  return static_cast<double>(bits >> 11) / static_cast<double>(1ULL << 53);
}

/**
 * Streaming q-quantile estimator used to approximate the distance median.
 *
 * HCA does not need the exact median and should not retain/sort every distance
 * produced by all mini-batches. This estimator keeps constant-size state and is
 * updated once for every observed distance.
 *
 * The current HCA callers use q=0.5:
 *   - Balanced OT: eps = max(eta * median_distance, eta_min)
 *   - Balanced AS: alpha = beta * median_distance * K / N
 *
 * Values are first quantized into integer buckets in [min_, max_]. The estimate
 * then moves stochastically toward values above/below it. step_ and sign_
 * retain the direction and momentum of recent movements. The random sequence
 * only affects approximation error; it does not sample vectors or assignments.
 *
 * The range is initialized from the first mini-batch. Later out-of-range values
 * are clamped to that range, matching the intended streaming estimator
 * behavior.
 */
class OnlineQuantileEstimator {
public:
  OnlineQuantileEstimator()
      : min_(0.0F), max_(0.0F), q_(0.5F), precision_(1e-4F), random_(),
        estimate_(-1), step_(0), sign_(1), is_inited_(false) {}

  /**
   * Initialize a new stream.
   *
   * @param q target quantile in (0, 1], 0.5 means median
   * @param min_value lower bound of the quantization range
   * @param max_value upper bound of the quantization range
   * @param precision bucket width relative to (max_value - min_value)
   * @param seed fixed seed used by stochastic estimate updates
   */
  int init(float q, float min_value, float max_value, float precision, int64_t seed) {
    int ret = OB_SUCCESS;
    if (!(q > 0.0F && q <= 1.0F) || !(precision > 0.0F && precision <= 1.0F) ||
        min_value > max_value) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid hca online quantile parameter", K(ret), K(q),
                K(min_value), K(max_value), K(precision), K(seed));
    } else {
      q_ = q;
      min_ = min_value;
      max_ = max_value;
      precision_ = precision;
      random_.seed(static_cast<uint64_t>(seed));
      estimate_ = -1;
      step_ = 0;
      sign_ = 1;
      is_inited_ = true;
    }
    return ret;
  }

  /** Feed one observed distance into the estimator. */
  void update(float value) {
    if (!is_inited_) {
      // Callers must initialize the estimator from the first mini-batch.
    } else {
      // Snap later observations to the range learned from the first batch.
      value = std::max(min_, std::min(max_, value));
      const float quantum = (max_ - min_) * precision_;
      const float bucket = quantum > 0.0F
                               ? std::floor((value - min_) / quantum + 0.5F)
                               : 0.0F;
      // Inputs are guaranteed finite by the upper layer. The only narrowing
      // conversion is intentionally kept in the original hot path.
      const int32_t int_value = static_cast<int32_t>(bucket);
      if (estimate_ == -1) {
        // The first observation initializes the estimate directly.
        estimate_ = int_value;
      } else {
        const float random = random_unit_float(random_);
        if (int_value > estimate_ && random > 1.0F - q_) {
          // Move upward. Repeated upward moves increase step_ so the estimate
          // can follow a drifting stream without storing historical values.
          step_ += sign_ > 0 ? 1 : -1;
          estimate_ += step_ > 0 ? step_ : 1;
          if (estimate_ > int_value) {
            step_ += int_value - estimate_;
            estimate_ = int_value;
          }
          if (sign_ < 0 && step_ > 1) {
            step_ = 1;
          }
          sign_ = 1;
        } else if (int_value < estimate_ && random > q_) {
          // Symmetric downward update.
          step_ += sign_ < 0 ? 1 : -1;
          estimate_ -= step_ > 0 ? step_ : 1;
          if (estimate_ < int_value) {
            step_ += estimate_ - int_value;
            estimate_ = int_value;
          }
          if (sign_ > 0 && step_ > 1) {
            step_ = 1;
          }
          sign_ = -1;
        }
      }
    }
  }

  /** Return the current estimate converted back to the distance scale. */
  float estimate() const {
    return estimate_ < 0 ? min_
                         : estimate_ * ((max_ - min_) * precision_) + min_;
  }

private:
  float min_;               // Lower bound of the quantized distance range.
  float max_;               // Upper bound of the quantized distance range.
  float q_;                 // Requested quantile; HCA currently uses 0.5.
  float precision_;         // Relative width of one quantization bucket.
  common::ObRandom random_; // Local fixed-seed RNG for stochastic updates.
  int32_t estimate_;        // Current estimate in integer-bucket space.
  int32_t step_;            // Movement magnitude accumulated in one direction.
  int32_t sign_;            // Last movement direction: +1 upward, -1 downward.
  bool is_inited_;          // Whether init() has established the stream range.
};

// NQT approximations keep Sinkhorn's log-domain operations inexpensive while
// preserving the base-2 numerical scale used by the reference algorithm.
inline float log2_nqt(const float value) {
  int32_t bits = 0;
  MEMCPY(&bits, &value, sizeof(bits));
  const int32_t exponent = ((bits >> 23) & 0xFF) - 126;
  const int32_t mantissa_bits = (bits & 0x007FFFFF) | (126 << 23);
  float mantissa = 0.0F;
  MEMCPY(&mantissa, &mantissa_bits, sizeof(mantissa));
  return 2.0F * (mantissa - 1.0F) + static_cast<float>(exponent);
}

inline float pow2_nqt(const float exponent) {
  int32_t p = 0;
  if (OB_LIKELY(exponent >= -30.0F && exponent <= 30.0F)) {
    p = static_cast<int32_t>(std::floor(exponent + 1.0F));
  } else if (OB_UNLIKELY(std::isnan(exponent))) {
    // NaN would make the following floating-to-integer conversion undefined.
    return 0.0F;
  } else {
    // Match the NQT reference: clamp p, but keep the original exponent in the
    // mantissa calculation so large negative values underflow to zero.
    p = exponent < 0.0F ? -30 : 30;
  }
  const float mantissa = (exponent - static_cast<float>(p)) * 0.5F + 1.0F;
  const int32_t bits = (p + 127) << 23;
  float power_of_two = 0.0F;
  MEMCPY(&power_of_two, &bits, sizeof(power_of_two));
  return std::max(mantissa * power_of_two, 0.0F);
}

inline float log_sum_exp_nqt_diff(const float *left, const float *right, const int64_t count, const float eps) {
  float max_value = (left[0] - right[0]) / eps;
  float sum = 1.0F;
  for (int64_t i = 1; i < count; ++i) {
    const float value = (left[i] - right[i]) / eps;
    const float new_max = std::max(max_value, value);
    sum *= pow2_nqt(max_value - new_max);
    sum += pow2_nqt(value - new_max);
    max_value = new_max;
  }
  return max_value + log2_nqt(sum);
}

/**
 * Reusable log-domain Sinkhorn workspace for one fixed rows-by-cols shape.
 *
 * log_rows_ and log_cols_ are deliberately retained across mini-batches, while
 * transpose_ is refreshed for each cost matrix. compute() produces a balanced
 * soft transport plan whose row and column marginals target 1/rows and 1/cols.
 */
class SinkhornWorkspace {
public:
  explicit SinkhornWorkspace(common::ObIAllocator &allocator)
      : rows_(0), cols_(0), log_rows_(allocator), log_cols_(allocator),
        transpose_(allocator) {}

  int init(const int64_t rows, const int64_t cols) {
    int ret = OB_SUCCESS;
    rows_ = rows;
    cols_ = cols;
    if (rows <= 0 || cols <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(log_rows_.allocate(rows, true))) {
    } else if (OB_FAIL(log_cols_.allocate(cols, true))) {
    } else if (cols > INT64_MAX / rows) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(transpose_.allocate(rows * cols))) {
    }
    return ret;
  }

  int compute(const float *cost, const int64_t iterations, const float eps, float *result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(cost) || OB_ISNULL(result) || iterations <= 0 ||
        eps <= 0.0F) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      for (int64_t row = 0; row < rows_; ++row) {
        for (int64_t col = 0; col < cols_; ++col) {
          transpose_[col * rows_ + row] = cost[row * cols_ + col];
        }
      }
      const float row_target = -eps * log2_nqt(static_cast<float>(rows_));
      const float col_target = -eps * log2_nqt(static_cast<float>(cols_));
      for (int64_t iteration = 0; iteration < iterations; ++iteration) {
        for (int64_t row = 0; row < rows_; ++row) {
          log_rows_[row] =
              -eps * log_sum_exp_nqt_diff(log_cols_.get(), cost + row * cols_,
                                          cols_, eps) +
              row_target;
        }
        for (int64_t col = 0; col < cols_; ++col) {
          log_cols_[col] =
              -eps * log_sum_exp_nqt_diff(log_rows_.get(),
                                          transpose_.get() + col * rows_, rows_,
                                          eps) +
              col_target;
        }
      }
      for (int64_t row = 0; row < rows_; ++row) {
        for (int64_t col = 0; col < cols_; ++col) {
          result[row * cols_ + col] = pow2_nqt(
              (log_rows_[row] + log_cols_[col] - cost[row * cols_ + col]) /
              eps);
        }
      }
    }
    return ret;
  }

private:
  int64_t rows_;
  int64_t cols_;
  ScopedArray<float> log_rows_;
  ScopedArray<float> log_cols_;
  ScopedArray<float> transpose_;
  DISALLOW_COPY_AND_ASSIGN(SinkhornWorkspace);
};

/**
 * Uniformly select k distinct ordinals from [0, n) using reservoir sampling.
 * Sorting the result preserves sequential-access behavior for disk-backed
 * VectorAccessor implementations.
 */
inline int reservoir_sample(const int64_t n, const int64_t k, const int64_t seed, int64_t *samples) {
  int ret = OB_SUCCESS;
  if (n <= 0 || k <= 0 || k > n || OB_ISNULL(samples)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < k; ++i) {
      samples[i] = i;
    }
    common::ObRandom random;
    random.seed(static_cast<uint64_t>(seed));
    for (int64_t i = k; i < n; ++i) {
      const int64_t selected = random.get(0, i);
      if (selected >= 0 && selected < k) {
        samples[selected] = i;
      }
    }
    lib::ob_sort(samples, samples + k);
  }
  return ret;
}

/** Copy and deterministically shuffle one fixed sample-position pool. */
inline int shuffle_samples(const int64_t *samples, const int64_t count, const uint64_t seed, int64_t *shuffled) {
  int ret = OB_SUCCESS;
  if (count <= 0 || OB_ISNULL(samples) || OB_ISNULL(shuffled)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (count > INT64_MAX / static_cast<int64_t>(sizeof(int64_t))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    MEMCPY(shuffled, samples, sizeof(int64_t) * count);
    common::ObRandom random;
    random.seed(seed);
    for (int64_t i = count - 1; OB_SUCC(ret) && i > 0; --i) {
      const int64_t selected = random.get(0, i);
      if (selected < 0 || selected > i) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        std::swap(shuffled[i], shuffled[selected]);
      }
    }
  }
  return ret;
}

// Small scalar vector kernels shared by training and exact center
// recomputation.
inline void zero_vector(float *vector, const int64_t dimension) {
  MEMSET(vector, 0, sizeof(float) * dimension);
}

inline void copy_vector(float *destination, const float *source, const int64_t dimension) {
  MEMCPY(destination, source, sizeof(float) * dimension);
}

/** Validate an externally partitioned half-open range. */
inline int validate_range(const int64_t begin, const int64_t end, const int64_t total) {
  return begin >= 0 && begin <= end && end <= total ? OB_SUCCESS
                                                    : OB_INVALID_ARGUMENT;
}

} // namespace hca_internal
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_HCA_OB_VECTOR_HCA_INTERNAL_H_
