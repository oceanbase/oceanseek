/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>

#include "share/vector_type/ob_vector_soar_distance.h"

namespace oceanbase
{
namespace common
{

using SoarDistance = ObVectorSoarDistance<float>;

using SoarDistanceFunc =
    float (*)(const float *, const float *, const float *, int64_t, float, float);

using SoarDistanceBulk4Func = void (*)(const float *,
                                       const float *,
                                       const float *,
                                       const float *,
                                       const float *,
                                       const float *,
                                       int64_t,
                                       float,
                                       float,
                                       float *);

void expect_kernel_matches_normal(const char *kernel_name,
                                  const SoarDistanceFunc soar_distance_func,
                                  const SoarDistanceBulk4Func soar_distance_bulk4_func,
                                  const int64_t dimension)
{
  SCOPED_TRACE(kernel_name);
  SCOPED_TRACE(dimension);
  std::vector<float> vector(dimension);
  std::vector<float> primary_residual(dimension);
  std::vector<float> candidates[4] = {std::vector<float>(dimension),
                                      std::vector<float>(dimension),
                                      std::vector<float>(dimension),
                                      std::vector<float>(dimension)};
  float primary_square_distance = 0.0f;
  for (int64_t i = 0; i < dimension; ++i) {
    vector[i] = static_cast<float>((i % 11) - 5) * 0.25f;
    primary_residual[i] = static_cast<float>((i % 7) + 1) * 0.125f;
    primary_square_distance += primary_residual[i] * primary_residual[i];
    candidates[0][i] = static_cast<float>((i % 5) - 2) * 0.5f;
    candidates[1][i] = static_cast<float>((i % 13) - 6) * 0.125f;
    candidates[2][i] = vector[i] - primary_residual[i];
    candidates[3][i] = -vector[i] + static_cast<float>(i % 3) * 0.25f;
  }

  const float lambda = 0.75f;
  const float expected = common::specific::normal::soar_distance(vector.data(),
                                                                 candidates[0].data(),
                                                                 primary_residual.data(),
                                                                 dimension,
                                                                 lambda,
                                                                 primary_square_distance);
  const float actual = soar_distance_func(vector.data(),
                                          candidates[0].data(),
                                          primary_residual.data(),
                                          dimension,
                                          lambda,
                                          primary_square_distance);
  const float tolerance = 1.0e-5f * std::max(1.0f, std::fabs(expected));
  EXPECT_NEAR(expected, actual, tolerance);

  float expected_bulk4[4] = {};
  float actual_bulk4[4] = {};
  common::specific::normal::soar_distance_bulk4(vector.data(),
                                                candidates[0].data(),
                                                candidates[1].data(),
                                                candidates[2].data(),
                                                candidates[3].data(),
                                                primary_residual.data(),
                                                dimension,
                                                lambda,
                                                primary_square_distance,
                                                expected_bulk4);
  soar_distance_bulk4_func(vector.data(),
                           candidates[0].data(),
                           candidates[1].data(),
                           candidates[2].data(),
                           candidates[3].data(),
                           primary_residual.data(),
                           dimension,
                           lambda,
                           primary_square_distance,
                           actual_bulk4);
  for (int64_t candidate_idx = 0; candidate_idx < 4; ++candidate_idx) {
    const float bulk_tolerance = 1.0e-5f * std::max(1.0f, std::fabs(expected_bulk4[candidate_idx]));
    EXPECT_NEAR(expected_bulk4[candidate_idx], actual_bulk4[candidate_idx], bulk_tolerance);
  }
}

TEST(ObVectorSoarDistanceTest, computes_hand_derived_single_candidate_distances)
{
  const float vector[] = {2.0f, 0.0f};
  const float primary_residual[] = {1.0f, 0.0f};
  const float parallel_candidate[] = {0.0f, 0.0f};
  const float orthogonal_candidate[] = {2.0f, -2.0f};

  const float normal_parallel_distance = common::specific::normal::soar_distance(
      vector, parallel_candidate, primary_residual, 2, 0.5f, 1.0f);
  const float simd_parallel_distance =
      SoarDistance::soar_distance(vector, parallel_candidate, primary_residual, 2, 0.5f, 1.0f);
  EXPECT_FLOAT_EQ(6.0f, normal_parallel_distance);
  EXPECT_FLOAT_EQ(6.0f, simd_parallel_distance);
  EXPECT_FLOAT_EQ(normal_parallel_distance, simd_parallel_distance);

  const float normal_orthogonal_distance = common::specific::normal::soar_distance(
      vector, orthogonal_candidate, primary_residual, 2, 0.5f, 1.0f);
  const float simd_orthogonal_distance =
      SoarDistance::soar_distance(vector, orthogonal_candidate, primary_residual, 2, 0.5f, 1.0f);
  EXPECT_FLOAT_EQ(4.0f, normal_orthogonal_distance);
  EXPECT_FLOAT_EQ(4.0f, simd_orthogonal_distance);
  EXPECT_FLOAT_EQ(normal_orthogonal_distance, simd_orthogonal_distance);

  const float normal_zero_lambda_distance = common::specific::normal::soar_distance(
      vector, parallel_candidate, primary_residual, 2, 0.0f, 1.0f);
  const float simd_zero_lambda_distance =
      SoarDistance::soar_distance(vector, parallel_candidate, primary_residual, 2, 0.0f, 1.0f);
  EXPECT_FLOAT_EQ(4.0f, normal_zero_lambda_distance);
  EXPECT_FLOAT_EQ(4.0f, simd_zero_lambda_distance);
  EXPECT_FLOAT_EQ(normal_zero_lambda_distance, simd_zero_lambda_distance);
}

TEST(ObVectorSoarDistanceTest, keeps_bulk4_candidate_output_mapping)
{
  const float vector[] = {2.0f, 0.0f};
  const float primary_residual[] = {1.0f, 0.0f};
  const float candidate0[] = {0.0f, 0.0f};
  const float candidate1[] = {2.0f, -2.0f};
  const float candidate2[] = {1.0f, 0.0f};
  const float candidate3[] = {3.0f, 0.0f};
  float distances[4] = {};

  SoarDistance::soar_distance_bulk4(vector,
                                    candidate0,
                                    candidate1,
                                    candidate2,
                                    candidate3,
                                    primary_residual,
                                    2,
                                    0.5f,
                                    1.0f,
                                    distances);

  EXPECT_FLOAT_EQ(6.0f, distances[0]);
  EXPECT_FLOAT_EQ(4.0f, distances[1]);
  EXPECT_FLOAT_EQ(1.5f, distances[2]);
  EXPECT_FLOAT_EQ(1.5f, distances[3]);
}

TEST(ObVectorSoarDistanceTest, public_dispatch_matches_normal_at_dimension_thresholds)
{
  const int64_t dimensions[] = {3, 4, 8, 9, 15, 16, 17};
  for (const int64_t dimension : dimensions) {
    expect_kernel_matches_normal("public dispatch",
                                 SoarDistance::soar_distance,
                                 SoarDistance::soar_distance_bulk4,
                                 dimension);
  }
}

TEST(ObVectorSoarDistanceTest, random_data_performance)
{
  using namespace std::chrono;
  const int VEC_SIZE = 1000;
  const int LOOP_SIZE = 100000;
  std::vector<float> vector(VEC_SIZE);
  std::vector<float> candidate(VEC_SIZE);
  std::vector<float> primary_residual(VEC_SIZE);
  std::srand(static_cast<unsigned int>(std::time(nullptr)));
  nanoseconds dur_normal(0);
  nanoseconds dur_simd(0);
  for (int j = 0; j < LOOP_SIZE; ++j) {
    float primary_square_distance = 0.0f;
    for (int i = 0; i < VEC_SIZE; ++i) {
      vector[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
      candidate[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
      primary_residual[i] = static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
      primary_square_distance += primary_residual[i] * primary_residual[i];
    }

    const steady_clock::time_point start_normal = steady_clock::now();
    const float distance_normal = common::specific::normal::soar_distance(vector.data(),
                                                                          candidate.data(),
                                                                          primary_residual.data(),
                                                                          VEC_SIZE,
                                                                          0.75f,
                                                                          primary_square_distance);
    dur_normal += duration_cast<nanoseconds>(steady_clock::now() - start_normal);

    const steady_clock::time_point start_simd = steady_clock::now();
    const float distance_simd = SoarDistance::soar_distance(vector.data(),
                                                            candidate.data(),
                                                            primary_residual.data(),
                                                            VEC_SIZE,
                                                            0.75f,
                                                            primary_square_distance);
    dur_simd += duration_cast<nanoseconds>(steady_clock::now() - start_simd);

    const float tolerance = 1.0e-5f * std::max(1.0f, std::fabs(distance_normal));
    ASSERT_NEAR(distance_normal, distance_simd, tolerance);
  }

  std::cout << "normal SOAR distance: " << dur_normal.count() / 1000000.0
            << "ms, simd SOAR distance: " << dur_simd.count() / 1000000.0 << "ms"
            << std::endl;
  std::cout << "simd is " << static_cast<double>(dur_normal.count()) /
                                    static_cast<double>(dur_simd.count())
            << "x faster than normal" << std::endl;
}

#if OB_USE_MULTITARGET_CODE
TEST(ObVectorSoarDistanceTest, isa_kernels_match_normal)
{
  const int64_t dimensions[] = {3, 4, 5, 8, 9, 16, 17, 32, 64, 128, 256, 512, 768, 1024, 1536};
  for (const int64_t dimension : dimensions) {
    if (common::is_arch_supported(ObTargetArch::SSE42)) {
      expect_kernel_matches_normal("SSE4.2",
                                   common::specific::sse42::soar_distance,
                                   common::specific::sse42::soar_distance_bulk4,
                                   dimension);
    }
    if (common::is_arch_supported(ObTargetArch::AVX)) {
      expect_kernel_matches_normal("AVX",
                                   common::specific::avx::soar_distance,
                                   common::specific::avx::soar_distance_bulk4,
                                   dimension);
    }
    if (common::is_arch_supported(ObTargetArch::AVX2)) {
      expect_kernel_matches_normal("AVX2",
                                   common::specific::avx2::soar_distance,
                                   common::specific::avx2::soar_distance_bulk4,
                                   dimension);
    }
    if (common::is_arch_supported(ObTargetArch::AVX512)) {
      expect_kernel_matches_normal("AVX512",
                                   common::specific::avx512::soar_distance,
                                   common::specific::avx512::soar_distance_bulk4,
                                   dimension);
    }
  }
}
#endif

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::init_arches();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
