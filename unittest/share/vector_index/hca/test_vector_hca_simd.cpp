/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>

#include "common/ob_target_specific.h"
#include "share/vector_index/hca/ob_vector_hca_simd.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <unistd.h>
#include <vector>

namespace oceanbase {
namespace share {
namespace {

TEST(TestVectorHcaSimd, FourQueryDistancesMatchSingleQueryBitsAndTails)
{
  common::init_arches();
  const auto kernel = hca_simd::select_square_distance4();
  for (int64_t dimension : {1, 2, 3, 4, 7, 8, 9, 15, 16, 31, 64, 767, 768, 769, 1536}) {
    std::vector<float> center(dimension + 1), values(4 * (dimension + 1));
    for (int64_t d = 0; d < dimension; ++d) { center[d + 1] = std::sin(d * 0.7); }
    const float *queries[4];
    for (int64_t q = 0; q < 4; ++q) {
      queries[q] = values.data() + q * (dimension + 1) + 1;
      for (int64_t d = 0; d < dimension; ++d) {
        values[q * (dimension + 1) + d + 1] = q == 0 ? center[d + 1]
            : q == 1 ? std::nextafter(center[d + 1], 1.0F) : std::cos(d * 0.3 + q);
      }
    }
    float distances[4];
    kernel(queries, center.data() + 1, dimension, distances);
    for (int64_t q = 0; q < 4; ++q) {
      const float expected = hca_square_distance(queries[q], center.data() + 1, dimension);
      EXPECT_EQ(0, std::memcmp(&expected, distances + q, sizeof(float))) << dimension << ":" << q;
    }
  }
}

void init_arches_quietly() {
  std::fflush(stdout);
  const int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    std::freopen("/dev/null", "w", stdout);
    common::init_arches();
    std::fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    common::init_arches();
  }
}

void expect_float_vectors_near(const std::vector<float> &expected,
                               const std::vector<float> &actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (int64_t d = 0; d < static_cast<int64_t>(expected.size()); ++d) {
    const float tolerance =
        1e-5F * std::max(1.0F, std::abs(expected[d]));
    EXPECT_NEAR(expected[d], actual[d], tolerance) << "dimension=" << d;
  }
}

#if defined(__GNUC__)
__attribute__((noinline))
#endif
float run_axpy_benchmark(float *destination, const float *source,
                         const int64_t dimension, const int64_t iterations,
                         const hca_simd::AxpyKernel kernel) {
  for (int64_t iteration = 0; iteration < iterations; ++iteration) {
    kernel(destination, source, 1e-4F, dimension);
  }
  return destination[iterations % dimension];
}

#if defined(__GNUC__)
__attribute__((noinline))
#endif
float run_axpby_benchmark(float *destination, const float *source,
                          const int64_t dimension, const int64_t iterations,
                          const hca_simd::AxpbyKernel kernel) {
  for (int64_t iteration = 0; iteration < iterations; ++iteration) {
    kernel(destination, source, 0.07F, 0.93F, dimension);
  }
  return destination[iterations % dimension];
}

int64_t measure_axpy_kernel(
    const hca_simd::AxpyKernel kernel,
    const std::vector<float> &initial, const std::vector<float> &source,
    const int64_t iterations, float &checksum) {
  std::vector<float> destination = initial;
  const auto begin = std::chrono::steady_clock::now();
  checksum = run_axpy_benchmark(
      destination.data(), source.data(), static_cast<int64_t>(initial.size()),
      iterations, kernel);
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

int64_t measure_axpby_kernel(const hca_simd::AxpbyKernel kernel,
                             const std::vector<float> &initial,
                             const std::vector<float> &source,
                             const int64_t iterations, float &checksum) {
  std::vector<float> destination = initial;
  const auto begin = std::chrono::steady_clock::now();
  checksum = run_axpby_benchmark(destination.data(), source.data(),
                                 static_cast<int64_t>(initial.size()),
                                 iterations, kernel);
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

// Compare runtime dispatch and every CPU-supported implementation against
// the original scalar formulas, including dimensions with SIMD tail elements.
TEST(TestVectorHcaSimd, VectorKernelCorrectness) {
  const int64_t dimensions[] = {1,  3,  4,   7,   8,   15,  16,
                                17, 63, 64,  65, 127, 128, 129, 768, 1536};
  for (const int64_t dimension : dimensions) {
    SCOPED_TRACE(::testing::Message() << "dimension=" << dimension);
    std::vector<float> source(dimension, 0.0F);
    std::vector<float> initial(dimension, 0.0F);
    long double squared_norm_reference = 0.0L;
    for (int64_t d = 0; d < dimension; ++d) {
      source[d] = static_cast<float>((d * 17 + 5) % 37 - 18) * 0.125F;
      initial[d] = static_cast<float>((d * 11 + 3) % 29 - 14) * 0.25F;
      squared_norm_reference +=
          static_cast<long double>(source[d]) * source[d];
    }
    const float expected_squared_norm =
        static_cast<float>(squared_norm_reference);
    const float squared_norm_tolerance =
        1e-5F * std::max(1.0F, std::abs(expected_squared_norm));
    EXPECT_NEAR(expected_squared_norm,
                hca_simd::squared_norm_scalar(source.data(), dimension),
                squared_norm_tolerance);
    EXPECT_NEAR(expected_squared_norm,
                hca_simd::squared_norm(source.data(), dimension),
                squared_norm_tolerance);

    std::vector<float> expected_add = initial;
    std::vector<float> actual_add = initial;
    hca_simd::axpy_scalar(expected_add.data(), source.data(), 0.37F,
                          dimension);
    hca_simd::axpy(actual_add.data(), source.data(), 0.37F, dimension);
    expect_float_vectors_near(expected_add, actual_add);

    std::vector<float> expected_blend = initial;
    std::vector<float> actual_blend = initial;
    hca_simd::axpby_scalar(expected_blend.data(), source.data(), 0.31F, 0.69F,
                           dimension);
    hca_simd::axpby(actual_blend.data(), source.data(), 0.31F, 0.69F,
                    dimension);
    expect_float_vectors_near(expected_blend, actual_blend);

#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::SSE42)) {
      EXPECT_NEAR(expected_squared_norm,
                  hca_simd::squared_norm_sse42(source.data(), dimension),
                  squared_norm_tolerance);
      std::vector<float> sse_add = initial;
      hca_simd::axpy_sse42(sse_add.data(), source.data(), 0.37F, dimension);
      expect_float_vectors_near(expected_add, sse_add);

      std::vector<float> sse_blend = initial;
      hca_simd::axpby_sse42(sse_blend.data(), source.data(), 0.31F, 0.69F,
                             dimension);
      expect_float_vectors_near(expected_blend, sse_blend);
    }
    if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
      EXPECT_NEAR(expected_squared_norm,
                  hca_simd::squared_norm_avx2(source.data(), dimension),
                  squared_norm_tolerance);
      std::vector<float> avx2_add = initial;
      hca_simd::axpy_avx2(avx2_add.data(), source.data(), 0.37F, dimension);
      expect_float_vectors_near(expected_add, avx2_add);

      std::vector<float> avx2_blend = initial;
      hca_simd::axpby_avx2(avx2_blend.data(), source.data(), 0.31F, 0.69F,
                            dimension);
      expect_float_vectors_near(expected_blend, avx2_blend);
    }
    if (common::is_arch_supported(common::ObTargetArch::AVX512)) {
      EXPECT_NEAR(expected_squared_norm,
                  hca_simd::squared_norm_avx512(source.data(), dimension),
                  squared_norm_tolerance);
      std::vector<float> avx512_add = initial;
      hca_simd::axpy_avx512(avx512_add.data(), source.data(), 0.37F,
                            dimension);
      expect_float_vectors_near(expected_add, avx512_add);

      std::vector<float> avx512_blend = initial;
      hca_simd::axpby_avx512(avx512_blend.data(), source.data(), 0.31F, 0.69F,
                             dimension);
      expect_float_vectors_near(expected_blend, avx512_blend);
    }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::NEON)) {
      EXPECT_NEAR(expected_squared_norm,
                  hca_simd::squared_norm_neon(source.data(), dimension),
                  squared_norm_tolerance);
      std::vector<float> neon_add = initial;
      hca_simd::axpy_neon(neon_add.data(), source.data(), 0.37F, dimension);
      expect_float_vectors_near(expected_add, neon_add);

      std::vector<float> neon_blend = initial;
      hca_simd::axpby_neon(neon_blend.data(), source.data(), 0.31F, 0.69F,
                           dimension);
      expect_float_vectors_near(expected_blend, neon_blend);
    }
#endif
  }
}

TEST(TestVectorHcaSimd, ClusterStatisticsAndWeights) {
  const int64_t counts[] = {1, 0, 3, 5, 2};
  int64_t sum = 0, maximum = 0, zeros = 0;
  double squared_deviations = 0;
  ASSERT_TRUE(hca_simd::count_statistics(
      counts, 5, sum, maximum, zeros, squared_deviations));
  EXPECT_EQ(11, sum);
  EXPECT_EQ(5, maximum);
  EXPECT_EQ(1, zeros);
  double expected_squares = 0;
  for (const int64_t count : counts) {
    const double delta = static_cast<double>(count) - 11.0 / 5;
    expected_squares += delta * delta;
  }
  EXPECT_NEAR(expected_squares, squared_deviations, 1e-12);
  const int64_t overflowing[] = {1, std::numeric_limits<int64_t>::max()};
  EXPECT_FALSE(hca_simd::count_statistics(
      overflowing, 2, sum, maximum, zeros, squared_deviations));

  float minimum[] = {4.0F, 2.0F, 0.0F, 5.0F, 0.0F, 6.0F, 7.0F};
  const float distances[] = {3.0F, 3.0F, 1.0F, 5.0F, 0.5F, 2.0F, 8.0F};
  double weights[] = {9, 9, 9, 9, 9, 9, 9};
  hca_simd::update_min_distance_weights(minimum, weights, distances, 0.5, 7);
  EXPECT_FLOAT_EQ(3.0F, minimum[0]);
  EXPECT_FLOAT_EQ(2.0F, minimum[1]);
  EXPECT_FLOAT_EQ(0.0F, minimum[2]);
  EXPECT_FLOAT_EQ(5.0F, minimum[3]);
  EXPECT_FLOAT_EQ(0.0F, minimum[4]);
  EXPECT_FLOAT_EQ(2.0F, minimum[5]);
  EXPECT_FLOAT_EQ(7.0F, minimum[6]);
  EXPECT_DOUBLE_EQ(1.5, weights[0]);
  EXPECT_DOUBLE_EQ(9, weights[2]);
  EXPECT_DOUBLE_EQ(1, weights[5]);
  EXPECT_DOUBLE_EQ(47.5, hca_simd::sum_weights(weights, 7));

  const int64_t mass[] = {1, 2, 3, 4, 5, 6, 7};
  double output_weights[7];
  const double total = hca_simd::weighted_distances(
      distances, mass, 7, output_weights);
  double expected_total = 0;
  for (int i = 0; i < 7; ++i) {
    EXPECT_DOUBLE_EQ(static_cast<double>(mass[i]) * distances[i], output_weights[i]);
    expected_total += output_weights[i];
  }
  EXPECT_NEAR(expected_total, total, 1e-12);
}

TEST(TestVectorHcaSimd, MinimumUpdatesAcrossVectorAndTail) {
  float minimum[] = {10.0F, 1.0F, 0.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F,
                     8.0F, 9.0F, 10.0F, 11.0F, 12.0F, 13.0F, 14.0F};
  const float distances[] = {9.0F, 2.0F, 0.0F, 2.0F, 5.0F, 4.0F, 6.0F, 8.0F,
                             7.0F, 10.0F, 9.0F, 12.0F, 11.0F, 14.0F, 13.0F};
  const float expected[] = {9.0F, 1.0F, 0.0F, 2.0F, 4.0F, 4.0F, 6.0F, 7.0F,
                            7.0F, 9.0F, 9.0F, 11.0F, 11.0F, 13.0F, 13.0F};
  constexpr int64_t count = sizeof(minimum) / sizeof(minimum[0]);
  hca_simd::update_min_distances(minimum, distances, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_FLOAT_EQ(expected[i], minimum[i]) << "index=" << i;
  }
}

// Compare the original compiler-optimized scalar loops with runtime-dispatched
// SIMD over the same number of float elements. Timings are diagnostic only:
// shared CI machines are too noisy for a stable speedup assertion.
TEST(TestVectorHcaSimd, VectorKernelPerformance) {
  const int64_t dimensions[] = {128, 768, 1536};
  const int64_t target_values = 64L * 1024L * 1024L;
  const char *simd_arch = "scalar";
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
    simd_arch = "avx2";
  } else if (common::is_arch_supported(common::ObTargetArch::SSE42)) {
    simd_arch = "sse4.2";
  }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) {
    simd_arch = "neon";
  }
#endif
  for (const int64_t dimension : dimensions) {
    const int64_t iterations =
        std::max<int64_t>(1024, target_values / dimension);
    std::vector<float> source(dimension, 0.0F);
    std::vector<float> initial(dimension, 0.0F);
    for (int64_t d = 0; d < dimension; ++d) {
      source[d] = static_cast<float>((d * 13 + 7) % 101 - 50) * 0.01F;
      initial[d] = static_cast<float>((d * 19 + 3) % 97 - 48) * 0.01F;
    }

    float scalar_add_checksum = 0.0F;
    float runtime_add_checksum = 0.0F;
    float scalar_blend_checksum = 0.0F;
    float runtime_blend_checksum = 0.0F;
    const int64_t scalar_add_ns = measure_axpy_kernel(
        hca_simd::axpy_scalar, initial, source, iterations,
        scalar_add_checksum);
    const int64_t runtime_add_ns = measure_axpy_kernel(
        hca_simd::axpy, initial, source, iterations, runtime_add_checksum);
    const int64_t scalar_blend_ns = measure_axpby_kernel(
        hca_simd::axpby_scalar, initial, source, iterations,
        scalar_blend_checksum);
    const int64_t runtime_blend_ns = measure_axpby_kernel(
        hca_simd::axpby, initial, source, iterations, runtime_blend_checksum);

    EXPECT_GT(scalar_add_ns, 0);
    EXPECT_GT(runtime_add_ns, 0);
    EXPECT_GT(scalar_blend_ns, 0);
    EXPECT_GT(runtime_blend_ns, 0);
    EXPECT_TRUE(std::isfinite(scalar_add_checksum));
    EXPECT_TRUE(std::isfinite(runtime_add_checksum));
    EXPECT_TRUE(std::isfinite(scalar_blend_checksum));
    EXPECT_TRUE(std::isfinite(runtime_blend_checksum));
    std::fprintf(
        stderr,
        "[HCA SIMD runtime] arch=%s dim=%ld iterations=%ld "
        "axpy_scalar_ns=%ld axpy_runtime_ns=%ld axpy_speedup=%.3f "
        "axpby_scalar_ns=%ld axpby_runtime_ns=%ld axpby_speedup=%.3f\n",
        simd_arch, dimension, iterations, scalar_add_ns, runtime_add_ns,
        static_cast<double>(scalar_add_ns) / runtime_add_ns, scalar_blend_ns,
        runtime_blend_ns,
        static_cast<double>(scalar_blend_ns) / runtime_blend_ns);

#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
      float avx2_add_checksum = 0.0F;
      float avx2_blend_checksum = 0.0F;
      const int64_t avx2_add_ns = measure_axpy_kernel(
          hca_simd::axpy_avx2, initial, source, iterations,
          avx2_add_checksum);
      const int64_t avx2_blend_ns = measure_axpby_kernel(
          hca_simd::axpby_avx2, initial, source, iterations,
          avx2_blend_checksum);
      EXPECT_TRUE(std::isfinite(avx2_add_checksum));
      EXPECT_TRUE(std::isfinite(avx2_blend_checksum));
      std::fprintf(stderr,
                   "[HCA SIMD direct] arch=avx2 dim=%ld "
                   "axpy_ns=%ld axpy_speedup=%.3f axpby_ns=%ld "
                   "axpby_speedup=%.3f\n",
                   dimension, avx2_add_ns,
                   static_cast<double>(scalar_add_ns) / avx2_add_ns,
                   avx2_blend_ns,
                   static_cast<double>(scalar_blend_ns) / avx2_blend_ns);
    }
    if (common::is_arch_supported(common::ObTargetArch::AVX512)) {
      float avx512_add_checksum = 0.0F;
      float avx512_blend_checksum = 0.0F;
      const int64_t avx512_add_ns = measure_axpy_kernel(
          hca_simd::axpy_avx512, initial, source, iterations,
          avx512_add_checksum);
      const int64_t avx512_blend_ns = measure_axpby_kernel(
          hca_simd::axpby_avx512, initial, source, iterations,
          avx512_blend_checksum);
      EXPECT_TRUE(std::isfinite(avx512_add_checksum));
      EXPECT_TRUE(std::isfinite(avx512_blend_checksum));
      std::fprintf(stderr,
                   "[HCA SIMD direct] arch=avx512 dim=%ld "
                   "axpy_ns=%ld axpy_speedup=%.3f axpby_ns=%ld "
                   "axpby_speedup=%.3f\n",
                   dimension, avx512_add_ns,
                   static_cast<double>(scalar_add_ns) / avx512_add_ns,
                   avx512_blend_ns,
                   static_cast<double>(scalar_blend_ns) / avx512_blend_ns);
    }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::NEON)) {
      float neon_add_checksum = 0.0F;
      float neon_blend_checksum = 0.0F;
      const int64_t neon_add_ns = measure_axpy_kernel(
          hca_simd::axpy_neon, initial, source, iterations,
          neon_add_checksum);
      const int64_t neon_blend_ns = measure_axpby_kernel(
          hca_simd::axpby_neon, initial, source, iterations,
          neon_blend_checksum);
      EXPECT_TRUE(std::isfinite(neon_add_checksum));
      EXPECT_TRUE(std::isfinite(neon_blend_checksum));
      std::fprintf(stderr,
                   "[HCA SIMD direct] arch=neon dim=%ld "
                   "axpy_ns=%ld axpy_speedup=%.3f axpby_ns=%ld "
                   "axpby_speedup=%.3f\n",
                   dimension, neon_add_ns,
                   static_cast<double>(scalar_add_ns) / neon_add_ns,
                   neon_blend_ns,
                   static_cast<double>(scalar_blend_ns) / neon_blend_ns);
    }
#endif
  }
}

} // namespace
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv) {
  oceanbase::share::init_arches_quietly();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
