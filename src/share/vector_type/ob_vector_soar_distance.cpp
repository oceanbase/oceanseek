/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_vector_soar_distance.h"

namespace oceanbase
{
namespace common
{

template <>
float ObVectorSoarDistance<float>::soar_distance(const float *vector,
                                                 const float *candidate_centroid,
                                                 const float *primary_residual,
                                                 const int64_t dimension,
                                                 const float lambda,
                                                 const float primary_square_distance)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    return common::specific::avx512::soar_distance(vector,
                                                   candidate_centroid,
                                                   primary_residual,
                                                   dimension,
                                                   lambda,
                                                   primary_square_distance);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    return common::specific::avx2::soar_distance(vector,
                                                 candidate_centroid,
                                                 primary_residual,
                                                 dimension,
                                                 lambda,
                                                 primary_square_distance);
  } else if (common::is_arch_supported(ObTargetArch::AVX)) {
    return common::specific::avx::soar_distance(vector,
                                                candidate_centroid,
                                                primary_residual,
                                                dimension,
                                                lambda,
                                                primary_square_distance);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    return common::specific::sse42::soar_distance(vector,
                                                  candidate_centroid,
                                                  primary_residual,
                                                  dimension,
                                                  lambda,
                                                  primary_square_distance);
  }
#endif
  return common::specific::normal::soar_distance(
      vector, candidate_centroid, primary_residual, dimension, lambda, primary_square_distance);
}

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
                                                      float distances[4])
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    return common::specific::avx512::soar_distance_bulk4(vector,
                                                         candidate_centroid0,
                                                         candidate_centroid1,
                                                         candidate_centroid2,
                                                         candidate_centroid3,
                                                         primary_residual,
                                                         dimension,
                                                         lambda,
                                                         primary_square_distance,
                                                         distances);
  } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
    return common::specific::avx2::soar_distance_bulk4(vector,
                                                       candidate_centroid0,
                                                       candidate_centroid1,
                                                       candidate_centroid2,
                                                       candidate_centroid3,
                                                       primary_residual,
                                                       dimension,
                                                       lambda,
                                                       primary_square_distance,
                                                       distances);
  } else if (common::is_arch_supported(ObTargetArch::AVX)) {
    return common::specific::avx::soar_distance_bulk4(vector,
                                                      candidate_centroid0,
                                                      candidate_centroid1,
                                                      candidate_centroid2,
                                                      candidate_centroid3,
                                                      primary_residual,
                                                      dimension,
                                                      lambda,
                                                      primary_square_distance,
                                                      distances);
  } else if (common::is_arch_supported(ObTargetArch::SSE42)) {
    return common::specific::sse42::soar_distance_bulk4(vector,
                                                        candidate_centroid0,
                                                        candidate_centroid1,
                                                        candidate_centroid2,
                                                        candidate_centroid3,
                                                        primary_residual,
                                                        dimension,
                                                        lambda,
                                                        primary_square_distance,
                                                        distances);
  }
#endif
  common::specific::normal::soar_distance_bulk4(vector,
                                                candidate_centroid0,
                                                candidate_centroid1,
                                                candidate_centroid2,
                                                candidate_centroid3,
                                                primary_residual,
                                                dimension,
                                                lambda,
                                                primary_square_distance,
                                                distances);
}

} // namespace common
} // namespace oceanbase
