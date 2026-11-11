/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_SOAR_DISTANCE_H_
#define OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_SOAR_DISTANCE_H_

/**
* Spilling with Orthogonality-Amplified Residuals (SOAR).
*
* Reference:
* [1] Philip Sun, David Simcha, Dave Dopson, Ruiqi Guo, and Sanjiv Kumar. 2023.
*     SOAR: Improved Indexing for Approximate Nearest Neighbor Search.
*     Advances in Neural Information Processing Systems 36, 3189–3204.
*/

#include "ob_vector_op_common.h"

namespace oceanbase
{
namespace common
{

template <typename T>
struct ObVectorSoarDistance
{
  static float soar_distance(const T *vector,
                             const T *candidate_centroid,
                             const float *primary_residual,
                             const int64_t dimension,
                             const float lambda,
                             const float primary_square_distance);

  static void soar_distance_bulk4(const T *vector,
                                  const T *candidate_centroid0,
                                  const T *candidate_centroid1,
                                  const T *candidate_centroid2,
                                  const T *candidate_centroid3,
                                  const float *primary_residual,
                                  const int64_t dimension,
                                  const float lambda,
                                  const float primary_square_distance,
                                  float distances[4]);
};

template <>
float ObVectorSoarDistance<float>::soar_distance(const float *vector,
                                                 const float *candidate_centroid,
                                                 const float *primary_residual,
                                                 const int64_t dimension,
                                                 const float lambda,
                                                 const float primary_square_distance);

template <>
void ObVectorSoarDistance<float>::soar_distance_bulk4(const float *vector,
                                                      const float *candidate_centroid0,
                                                      const float *candidate_centroid1,
                                                      const float *candidate_centroid2,
                                                      const float *candidate_centroid3,
                                                      const float *primary_residual,
                                                      const int64_t dimension,
                                                      const float lambda,
                                                      const float primary_square_distance,
                                                      float distances[4]);

OB_INLINE static float finish_soar_distance(const float candidate_square_distance,
                                            const float projection,
                                            const float lambda,
                                            const float primary_square_distance)
{
  // NOTE: no need to handle primary_square_distance == 0 here.
  // Upper layer will not search the second centroid when primary_square_distance is 0,
  // so soar distance will not be invoked.
  return candidate_square_distance + lambda * projection * projection / primary_square_distance;
}

OB_INLINE static void accumulate_soar_tail(const float *vector,
                                           const float *candidate_centroid,
                                           const float *primary_residual,
                                           const int64_t begin,
                                           const int64_t dimension,
                                           float &candidate_square_distance,
                                           float &projection)
{
  // Scalar remainder after SIMD (SSE 0..3, AVX/AVX2 0..7, AVX512 0..15).
  // soar_distance_normal also uses this with begin=0 for the full dimension.
  for (int64_t i = begin; i < dimension; ++i) {
    const float candidate_residual = vector[i] - candidate_centroid[i];
    candidate_square_distance += candidate_residual * candidate_residual;
    projection += primary_residual[i] * candidate_residual;
  }
}

OB_INLINE static float soar_distance_normal(const float *vector,
                                            const float *candidate_centroid,
                                            const float *primary_residual,
                                            const int64_t dimension,
                                            const float lambda,
                                            const float primary_square_distance)
{
  float candidate_square_distance = 0.0f;
  float projection = 0.0f;
  accumulate_soar_tail(vector,
                       candidate_centroid,
                       primary_residual,
                       0,
                       dimension,
                       candidate_square_distance,
                       projection);
  return finish_soar_distance(
      candidate_square_distance, projection, lambda, primary_square_distance);
}

OB_INLINE static void soar_distance_bulk4_normal(const float *vector,
                                                 const float *candidate_centroid0,
                                                 const float *candidate_centroid1,
                                                 const float *candidate_centroid2,
                                                 const float *candidate_centroid3,
                                                 const float *primary_residual,
                                                 const int64_t dimension,
                                                 const float lambda,
                                                 const float primary_square_distance,
                                                 float distances[4])
{
  float candidate_square_distances[4] = {};
  float projections[4] = {};
  for (int64_t i = 0; i < dimension; ++i) {
    const float candidate_residual0 = vector[i] - candidate_centroid0[i];
    const float candidate_residual1 = vector[i] - candidate_centroid1[i];
    const float candidate_residual2 = vector[i] - candidate_centroid2[i];
    const float candidate_residual3 = vector[i] - candidate_centroid3[i];
    candidate_square_distances[0] += candidate_residual0 * candidate_residual0;
    candidate_square_distances[1] += candidate_residual1 * candidate_residual1;
    candidate_square_distances[2] += candidate_residual2 * candidate_residual2;
    candidate_square_distances[3] += candidate_residual3 * candidate_residual3;
    projections[0] += primary_residual[i] * candidate_residual0;
    projections[1] += primary_residual[i] * candidate_residual1;
    projections[2] += primary_residual[i] * candidate_residual2;
    projections[3] += primary_residual[i] * candidate_residual3;
  }

  for (int64_t i = 0; i < 4; ++i) {
    distances[i] = finish_soar_distance(
        candidate_square_distances[i], projections[i], lambda, primary_square_distance);
  }
}

OB_DECLARE_SSE42_SPECIFIC_CODE(
    OB_INLINE static float soar_reduce_add_ps(const __m128 value) {
      float values[4] CACHE_ALIGNED;
      _mm_store_ps(values, value);
      return values[0] + values[1] + values[2] + values[3];
    }

    inline static float soar_distance(const float *vector,
                                         const float *candidate_centroid,
                                         const float *primary_residual,
                                         const int64_t dimension,
                                         const float lambda,
                                         const float primary_square_distance) {
      __m128 square_sum = _mm_setzero_ps();
      __m128 projection_sum = _mm_setzero_ps();
      int64_t i = 0;
      for (; i + 3 < dimension; i += 4) {
        const __m128 candidate_residual =
            _mm_sub_ps(_mm_loadu_ps(vector + i), _mm_loadu_ps(candidate_centroid + i));
        square_sum = _mm_add_ps(square_sum, _mm_mul_ps(candidate_residual, candidate_residual));
        projection_sum = _mm_add_ps(
            projection_sum, _mm_mul_ps(_mm_loadu_ps(primary_residual + i), candidate_residual));
      }
      float candidate_square_distance = soar_reduce_add_ps(square_sum);
      float projection = soar_reduce_add_ps(projection_sum);
      accumulate_soar_tail(vector,
                           candidate_centroid,
                           primary_residual,
                           i,
                           dimension,
                           candidate_square_distance,
                           projection);
      return finish_soar_distance(
          candidate_square_distance, projection, lambda, primary_square_distance);
    }

    inline static void soar_distance_bulk4(const float *vector,
                                              const float *candidate_centroid0,
                                              const float *candidate_centroid1,
                                              const float *candidate_centroid2,
                                              const float *candidate_centroid3,
                                              const float *primary_residual,
                                              const int64_t dimension,
                                              const float lambda,
                                              const float primary_square_distance,
                                              float distances[4]) {
      __m128 square_sums[4] = {
          _mm_setzero_ps(), _mm_setzero_ps(), _mm_setzero_ps(), _mm_setzero_ps()};
      __m128 projection_sums[4] = {
          _mm_setzero_ps(), _mm_setzero_ps(), _mm_setzero_ps(), _mm_setzero_ps()};
      int64_t i = 0;
      for (; i + 3 < dimension; i += 4) {
        const __m128 vector_value = _mm_loadu_ps(vector + i);
        const __m128 primary_residual_value = _mm_loadu_ps(primary_residual + i);
        const __m128 candidate_residuals[4] = {
            _mm_sub_ps(vector_value, _mm_loadu_ps(candidate_centroid0 + i)),
            _mm_sub_ps(vector_value, _mm_loadu_ps(candidate_centroid1 + i)),
            _mm_sub_ps(vector_value, _mm_loadu_ps(candidate_centroid2 + i)),
            _mm_sub_ps(vector_value, _mm_loadu_ps(candidate_centroid3 + i))};
        for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
          square_sums[candidate_idx] = _mm_add_ps(
              square_sums[candidate_idx],
              _mm_mul_ps(candidate_residuals[candidate_idx], candidate_residuals[candidate_idx]));
          projection_sums[candidate_idx] =
              _mm_add_ps(projection_sums[candidate_idx],
                         _mm_mul_ps(primary_residual_value, candidate_residuals[candidate_idx]));
        }
      }

      float candidate_square_distances[4];
      float projections[4];
      const float *candidate_centroids[4] = {
          candidate_centroid0, candidate_centroid1, candidate_centroid2, candidate_centroid3};
      for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
        candidate_square_distances[candidate_idx] = soar_reduce_add_ps(square_sums[candidate_idx]);
        projections[candidate_idx] = soar_reduce_add_ps(projection_sums[candidate_idx]);
        accumulate_soar_tail(vector,
                             candidate_centroids[candidate_idx],
                             primary_residual,
                             i,
                             dimension,
                             candidate_square_distances[candidate_idx],
                             projections[candidate_idx]);
        distances[candidate_idx] = finish_soar_distance(candidate_square_distances[candidate_idx],
                                                        projections[candidate_idx],
                                                        lambda,
                                                        primary_square_distance);
      }
    })

OB_DECLARE_AVX_SPECIFIC_CODE(
    OB_INLINE static float soar_reduce_add_ps(const __m256 value) {
      float values[8] CACHE_ALIGNED;
      _mm256_store_ps(values, value);
      return values[0] + values[1] + values[2] + values[3] + values[4] + values[5] + values[6] +
             values[7];
    }

    inline static float soar_distance(const float *vector,
                                         const float *candidate_centroid,
                                         const float *primary_residual,
                                         const int64_t dimension,
                                         const float lambda,
                                         const float primary_square_distance) {
      __m256 square_sum = _mm256_setzero_ps();
      __m256 projection_sum = _mm256_setzero_ps();
      int64_t i = 0;
      for (; i + 7 < dimension; i += 8) {
        const __m256 candidate_residual =
            _mm256_sub_ps(_mm256_loadu_ps(vector + i), _mm256_loadu_ps(candidate_centroid + i));
        square_sum =
            _mm256_add_ps(square_sum, _mm256_mul_ps(candidate_residual, candidate_residual));
        projection_sum =
            _mm256_add_ps(projection_sum,
                          _mm256_mul_ps(_mm256_loadu_ps(primary_residual + i), candidate_residual));
      }
      float candidate_square_distance = soar_reduce_add_ps(square_sum);
      float projection = soar_reduce_add_ps(projection_sum);
      accumulate_soar_tail(vector,
                           candidate_centroid,
                           primary_residual,
                           i,
                           dimension,
                           candidate_square_distance,
                           projection);
      return finish_soar_distance(
          candidate_square_distance, projection, lambda, primary_square_distance);
    }

    inline static void soar_distance_bulk4(const float *vector,
                                              const float *candidate_centroid0,
                                              const float *candidate_centroid1,
                                              const float *candidate_centroid2,
                                              const float *candidate_centroid3,
                                              const float *primary_residual,
                                              const int64_t dimension,
                                              const float lambda,
                                              const float primary_square_distance,
                                              float distances[4]) {
      __m256 square_sums[4] = {
          _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps()};
      __m256 projection_sums[4] = {
          _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps()};
      int64_t i = 0;
      for (; i + 7 < dimension; i += 8) {
        const __m256 vector_value = _mm256_loadu_ps(vector + i);
        const __m256 primary_residual_value = _mm256_loadu_ps(primary_residual + i);
        const __m256 candidate_residuals[4] = {
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid0 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid1 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid2 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid3 + i))};
        for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
          square_sums[candidate_idx] =
              _mm256_add_ps(square_sums[candidate_idx],
                            _mm256_mul_ps(candidate_residuals[candidate_idx],
                                          candidate_residuals[candidate_idx]));
          projection_sums[candidate_idx] = _mm256_add_ps(
              projection_sums[candidate_idx],
              _mm256_mul_ps(primary_residual_value, candidate_residuals[candidate_idx]));
        }
      }

      float candidate_square_distances[4];
      float projections[4];
      const float *candidate_centroids[4] = {
          candidate_centroid0, candidate_centroid1, candidate_centroid2, candidate_centroid3};
      for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
        candidate_square_distances[candidate_idx] = soar_reduce_add_ps(square_sums[candidate_idx]);
        projections[candidate_idx] = soar_reduce_add_ps(projection_sums[candidate_idx]);
        accumulate_soar_tail(vector,
                             candidate_centroids[candidate_idx],
                             primary_residual,
                             i,
                             dimension,
                             candidate_square_distances[candidate_idx],
                             projections[candidate_idx]);
        distances[candidate_idx] = finish_soar_distance(candidate_square_distances[candidate_idx],
                                                        projections[candidate_idx],
                                                        lambda,
                                                        primary_square_distance);
      }
    })

OB_DECLARE_AVX2_SPECIFIC_CODE(
    OB_INLINE static float soar_reduce_add_ps(const __m256 value) {
      float values[8] CACHE_ALIGNED;
      _mm256_store_ps(values, value);
      return values[0] + values[1] + values[2] + values[3] + values[4] + values[5] + values[6] +
             values[7];
    }

    inline static float soar_distance(const float *vector,
                                         const float *candidate_centroid,
                                         const float *primary_residual,
                                         const int64_t dimension,
                                         const float lambda,
                                         const float primary_square_distance) {
      __m256 square_sum = _mm256_setzero_ps();
      __m256 projection_sum = _mm256_setzero_ps();
      int64_t i = 0;
      for (; i + 7 < dimension; i += 8) {
        const __m256 candidate_residual =
            _mm256_sub_ps(_mm256_loadu_ps(vector + i), _mm256_loadu_ps(candidate_centroid + i));
        square_sum = _mm256_fmadd_ps(candidate_residual, candidate_residual, square_sum);
        projection_sum = _mm256_fmadd_ps(
            _mm256_loadu_ps(primary_residual + i), candidate_residual, projection_sum);
      }
      float candidate_square_distance = soar_reduce_add_ps(square_sum);
      float projection = soar_reduce_add_ps(projection_sum);
      accumulate_soar_tail(vector,
                           candidate_centroid,
                           primary_residual,
                           i,
                           dimension,
                           candidate_square_distance,
                           projection);
      return finish_soar_distance(
          candidate_square_distance, projection, lambda, primary_square_distance);
    }

    inline static void soar_distance_bulk4(const float *vector,
                                              const float *candidate_centroid0,
                                              const float *candidate_centroid1,
                                              const float *candidate_centroid2,
                                              const float *candidate_centroid3,
                                              const float *primary_residual,
                                              const int64_t dimension,
                                              const float lambda,
                                              const float primary_square_distance,
                                              float distances[4]) {
      __m256 square_sums[4] = {
          _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps()};
      __m256 projection_sums[4] = {
          _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps(), _mm256_setzero_ps()};
      int64_t i = 0;
      for (; i + 7 < dimension; i += 8) {
        const __m256 vector_value = _mm256_loadu_ps(vector + i);
        const __m256 primary_residual_value = _mm256_loadu_ps(primary_residual + i);
        const __m256 candidate_residuals[4] = {
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid0 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid1 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid2 + i)),
            _mm256_sub_ps(vector_value, _mm256_loadu_ps(candidate_centroid3 + i))};
        for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
          square_sums[candidate_idx] = _mm256_fmadd_ps(candidate_residuals[candidate_idx],
                                                       candidate_residuals[candidate_idx],
                                                       square_sums[candidate_idx]);
          projection_sums[candidate_idx] = _mm256_fmadd_ps(primary_residual_value,
                                                           candidate_residuals[candidate_idx],
                                                           projection_sums[candidate_idx]);
        }
      }

      float candidate_square_distances[4];
      float projections[4];
      const float *candidate_centroids[4] = {
          candidate_centroid0, candidate_centroid1, candidate_centroid2, candidate_centroid3};
      for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
        candidate_square_distances[candidate_idx] = soar_reduce_add_ps(square_sums[candidate_idx]);
        projections[candidate_idx] = soar_reduce_add_ps(projection_sums[candidate_idx]);
        accumulate_soar_tail(vector,
                             candidate_centroids[candidate_idx],
                             primary_residual,
                             i,
                             dimension,
                             candidate_square_distances[candidate_idx],
                             projections[candidate_idx]);
        distances[candidate_idx] = finish_soar_distance(candidate_square_distances[candidate_idx],
                                                        projections[candidate_idx],
                                                        lambda,
                                                        primary_square_distance);
      }
    })

OB_DECLARE_AVX512_SPECIFIC_CODE(
    inline static float soar_distance(const float *vector,
                                         const float *candidate_centroid,
                                         const float *primary_residual,
                                         const int64_t dimension,
                                         const float lambda,
                                         const float primary_square_distance) {
      __m512 square_sum = _mm512_setzero_ps();
      __m512 projection_sum = _mm512_setzero_ps();
      int64_t i = 0;
      for (; i + 15 < dimension; i += 16) {
        const __m512 candidate_residual =
            _mm512_sub_ps(_mm512_loadu_ps(vector + i), _mm512_loadu_ps(candidate_centroid + i));
        square_sum = _mm512_fmadd_ps(candidate_residual, candidate_residual, square_sum);
        projection_sum = _mm512_fmadd_ps(
            _mm512_loadu_ps(primary_residual + i), candidate_residual, projection_sum);
      }
      float candidate_square_distance = _mm512_reduce_add_ps(square_sum);
      float projection = _mm512_reduce_add_ps(projection_sum);
      accumulate_soar_tail(vector,
                           candidate_centroid,
                           primary_residual,
                           i,
                           dimension,
                           candidate_square_distance,
                           projection);
      return finish_soar_distance(
          candidate_square_distance, projection, lambda, primary_square_distance);
    }

    inline static void soar_distance_bulk4(const float *vector,
                                              const float *candidate_centroid0,
                                              const float *candidate_centroid1,
                                              const float *candidate_centroid2,
                                              const float *candidate_centroid3,
                                              const float *primary_residual,
                                              const int64_t dimension,
                                              const float lambda,
                                              const float primary_square_distance,
                                              float distances[4]) {
      __m512 square_sums[4] = {
          _mm512_setzero_ps(), _mm512_setzero_ps(), _mm512_setzero_ps(), _mm512_setzero_ps()};
      __m512 projection_sums[4] = {
          _mm512_setzero_ps(), _mm512_setzero_ps(), _mm512_setzero_ps(), _mm512_setzero_ps()};
      int64_t i = 0;
      for (; i + 15 < dimension; i += 16) {
        const __m512 vector_value = _mm512_loadu_ps(vector + i);
        const __m512 primary_residual_value = _mm512_loadu_ps(primary_residual + i);
        const __m512 candidate_residuals[4] = {
            _mm512_sub_ps(vector_value, _mm512_loadu_ps(candidate_centroid0 + i)),
            _mm512_sub_ps(vector_value, _mm512_loadu_ps(candidate_centroid1 + i)),
            _mm512_sub_ps(vector_value, _mm512_loadu_ps(candidate_centroid2 + i)),
            _mm512_sub_ps(vector_value, _mm512_loadu_ps(candidate_centroid3 + i))};
        for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
          square_sums[candidate_idx] = _mm512_fmadd_ps(candidate_residuals[candidate_idx],
                                                       candidate_residuals[candidate_idx],
                                                       square_sums[candidate_idx]);
          projection_sums[candidate_idx] = _mm512_fmadd_ps(primary_residual_value,
                                                           candidate_residuals[candidate_idx],
                                                           projection_sums[candidate_idx]);
        }
      }

      float candidate_square_distances[4];
      float projections[4];
      const float *candidate_centroids[4] = {
          candidate_centroid0, candidate_centroid1, candidate_centroid2, candidate_centroid3};
      for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
        candidate_square_distances[candidate_idx] =
            _mm512_reduce_add_ps(square_sums[candidate_idx]);
        projections[candidate_idx] = _mm512_reduce_add_ps(projection_sums[candidate_idx]);
        accumulate_soar_tail(vector,
                             candidate_centroids[candidate_idx],
                             primary_residual,
                             i,
                             dimension,
                             candidate_square_distances[candidate_idx],
                             projections[candidate_idx]);
        distances[candidate_idx] = finish_soar_distance(candidate_square_distances[candidate_idx],
                                                        projections[candidate_idx],
                                                        lambda,
                                                        primary_square_distance);
      }
    })

OB_DECLARE_DEFAULT_CODE(
    OB_INLINE static float soar_distance(const float *vector,
                                         const float *candidate_centroid,
                                         const float *primary_residual,
                                         const int64_t dimension,
                                         const float lambda,
                                         const float primary_square_distance) {
      return soar_distance_normal(
          vector, candidate_centroid, primary_residual, dimension, lambda, primary_square_distance);
    }

    OB_INLINE static void soar_distance_bulk4(const float *vector,
                                              const float *candidate_centroid0,
                                              const float *candidate_centroid1,
                                              const float *candidate_centroid2,
                                              const float *candidate_centroid3,
                                              const float *primary_residual,
                                              const int64_t dimension,
                                              const float lambda,
                                              const float primary_square_distance,
                                              float distances[4]) {
      soar_distance_bulk4_normal(vector,
                                 candidate_centroid0,
                                 candidate_centroid1,
                                 candidate_centroid2,
                                 candidate_centroid3,
                                 primary_residual,
                                 dimension,
                                 lambda,
                                 primary_square_distance,
                                 distances);
    })

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_TYPE_OB_VECTOR_SOAR_DISTANCE_H_
