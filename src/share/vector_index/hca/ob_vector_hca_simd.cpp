/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "share/vector_index/hca/ob_vector_hca_simd.h"

#include "common/ob_target_specific.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include <algorithm>
#include <climits>
#if defined(__x86_64__)
#include <immintrin.h>
#endif
#if OB_ARM_USE_MULTITARGET_CODE
#include <arm_neon.h>
#endif

namespace oceanbase {
namespace share {
namespace hca_simd {

static void square_distance4_default(const float *const *queries,
    const float *center, const int64_t dimension, float *distances)
{
  for (int64_t q = 0; q < 4; ++q) {
    distances[q] = common::ObVectorL2Distance<float>::l2_square_flt_func(
        queries[q], center, dimension);
  }
}

#if OB_USE_MULTITARGET_CODE
OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE static void square_distance4_avx2(
    const float *const *queries, const float *center,
    const int64_t dimension, float *distances)
{
  __m256 s0 = _mm256_setzero_ps();
  __m256 s1 = s0, s2 = s0, s3 = s0;
  int64_t d = 0;
  for (; d + 8 <= dimension; d += 8) {
    const __m256 c = _mm256_loadu_ps(center + d);
    const __m256 v0 = _mm256_sub_ps(_mm256_loadu_ps(queries[0] + d), c);
    const __m256 v1 = _mm256_sub_ps(_mm256_loadu_ps(queries[1] + d), c);
    const __m256 v2 = _mm256_sub_ps(_mm256_loadu_ps(queries[2] + d), c);
    const __m256 v3 = _mm256_sub_ps(_mm256_loadu_ps(queries[3] + d), c);
    s0 = _mm256_fmadd_ps(v0, v0, s0);
    s1 = _mm256_fmadd_ps(v1, v1, s1);
    s2 = _mm256_fmadd_ps(v2, v2, s2);
    s3 = _mm256_fmadd_ps(v3, v3, s3);
  }
  const __m256 sums[4] = {s0, s1, s2, s3};
  for (int64_t q = 0; q < 4; ++q) {
    alignas(32) float parts[8];
    _mm256_store_ps(parts, sums[q]);
    distances[q] = parts[0] + parts[1] + parts[2] + parts[3]
        + parts[4] + parts[5] + parts[6] + parts[7];
    // Match the AVX2 single-query kernel's AVX/SSE tail, including short dims.
    distances[q] += common::specific::avx::l2_square_flt(
        queries[q] + d, center + d, dimension - d);
  }
}
#endif

#if OB_ARM_USE_MULTITARGET_CODE
// Keep the same accumulator layout and reduction order as l2_square_flt_neon.
// This lets the four-query path share center loads without changing HCA scores.
static void square_distance4_neon(const float *const *queries,
    const float *center, const int64_t dimension, float *distances)
{
  // The single-query implementation also uses its scalar tail at this size.
  if (dimension < 4) {
    square_distance4_default(queries, center, dimension, distances);
    return;
  }
  float32x4_t sums[4][4];
  for (int q = 0; q < 4; ++q) {
    for (int part = 0; part < 4; ++part) { sums[q][part] = vdupq_n_f32(0.0F); }
  }
  int64_t d = 0;
  for (; d + 16 <= dimension; d += 16) {
    const float32x4x4_t c = vld1q_f32_x4(center + d);
    for (int q = 0; q < 4; ++q) {
      const float32x4x4_t v = vld1q_f32_x4(queries[q] + d);
      for (int part = 0; part < 4; ++part) {
        const float32x4_t delta = vsubq_f32(v.val[part], c.val[part]);
        sums[q][part] = vfmaq_f32(sums[q][part], delta, delta);
      }
    }
  }
  if (d + 8 <= dimension) {
    const float32x4x2_t c = vld1q_f32_x2(center + d);
    for (int q = 0; q < 4; ++q) {
      const float32x4x2_t v = vld1q_f32_x2(queries[q] + d);
      for (int part = 0; part < 2; ++part) {
        const float32x4_t delta = vsubq_f32(v.val[part], c.val[part]);
        sums[q][part] = vfmaq_f32(sums[q][part], delta, delta);
      }
    }
    d += 8;
  }
  if (d + 4 <= dimension) {
    const float32x4_t c = vld1q_f32(center + d);
    for (int q = 0; q < 4; ++q) {
      const float32x4_t delta = vsubq_f32(vld1q_f32(queries[q] + d), c);
      sums[q][0] = vfmaq_f32(sums[q][0], delta, delta);
    }
    d += 4;
  }
  for (int q = 0; q < 4; ++q) {
    const float32x4_t first = vaddq_f32(sums[q][0], sums[q][1]);
    const float32x4_t second = vaddq_f32(sums[q][2], sums[q][3]);
    float result = vaddvq_f32(vaddq_f32(first, second));
    for (int64_t tail = d; tail < dimension; ++tail) {
      const float delta = queries[q][tail] - center[tail];
      result += delta * delta;
    }
    distances[q] = result;
  }
}
#endif

SquareDistance4Kernel select_square_distance4()
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
    return square_distance4_avx2;
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) {
    return square_distance4_neon;
  }
#endif
  return square_distance4_default;
}

// Scalar reference kernels also handle the remainder of SIMD loops.
float squared_norm_scalar(const float *x, const int64_t size) {
  float result = 0.0F;
  for (int64_t i = 0; i < size; ++i) {
    result += x[i] * x[i];
  }
  return result;
}

void axpy_scalar(float *y, const float *x, const float alpha, const int64_t size) {
  for (int64_t i = 0; i < size; ++i) {
    y[i] += alpha * x[i];
  }
}

void axpby_scalar(float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  for (int64_t i = 0; i < size; ++i) {
    y[i] = alpha * x[i] + beta * y[i];
  }
}

// x86 target-specific kernels use unaligned loads because callers do not
// promise SIMD alignment for a vector or center row.
#if OB_USE_MULTITARGET_CODE
OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE float squared_norm_sse42(const float *x, const int64_t size) {
  __m128 sum4 = _mm_setzero_ps();
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const __m128 value4 = _mm_loadu_ps(x + i);
    sum4 = _mm_add_ps(sum4, _mm_mul_ps(value4, value4));
  }
  float lanes[4];
  _mm_storeu_ps(lanes, sum4);
  float result = lanes[0] + lanes[1] + lanes[2] + lanes[3];
  return result + squared_norm_scalar(x + i, size - i);
}

OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE float squared_norm_avx2(const float *x, const int64_t size) {
  __m256 sum8 = _mm256_setzero_ps();
  int64_t i = 0;
  for (; i + 8 <= size; i += 8) {
    const __m256 value8 = _mm256_loadu_ps(x + i);
    sum8 = _mm256_fmadd_ps(value8, value8, sum8);
  }
  float lanes[8];
  _mm256_storeu_ps(lanes, sum8);
  float result = 0.0F;
  for (int64_t lane = 0; lane < 8; ++lane) {
    result += lanes[lane];
  }
  return result + squared_norm_scalar(x + i, size - i);
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE float squared_norm_avx512(const float *x, const int64_t size) {
  __m512 sum16 = _mm512_setzero_ps();
  int64_t i = 0;
  for (; i + 16 <= size; i += 16) {
    const __m512 value16 = _mm512_loadu_ps(x + i);
    sum16 = _mm512_fmadd_ps(value16, value16, sum16);
  }
  float lanes[16];
  _mm512_storeu_ps(lanes, sum16);
  float result = 0.0F;
  for (int64_t lane = 0; lane < 16; ++lane) {
    result += lanes[lane];
  }
  return result + squared_norm_scalar(x + i, size - i);
}

OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE void axpy_sse42(float *y, const float *x, const float alpha, const int64_t size) {
  const __m128 alpha4 = _mm_set1_ps(alpha);
  int64_t i = 0;
  for (; i + 8 <= size; i += 8) {
    const __m128 x0 = _mm_loadu_ps(x + i);
    const __m128 x1 = _mm_loadu_ps(x + i + 4);
    const __m128 y0 = _mm_loadu_ps(y + i);
    const __m128 y1 = _mm_loadu_ps(y + i + 4);
    _mm_storeu_ps(y + i, _mm_add_ps(y0, _mm_mul_ps(alpha4, x0)));
    _mm_storeu_ps(y + i + 4, _mm_add_ps(y1, _mm_mul_ps(alpha4, x1)));
  }
  for (; i + 4 <= size; i += 4) {
    const __m128 x4 = _mm_loadu_ps(x + i);
    const __m128 y4 = _mm_loadu_ps(y + i);
    _mm_storeu_ps(y + i, _mm_add_ps(y4, _mm_mul_ps(alpha4, x4)));
  }
  axpy_scalar(y + i, x + i, alpha, size - i);
}

OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void axpy_avx2(float *y, const float *x, const float alpha, const int64_t size) {
  const __m256 alpha8 = _mm256_set1_ps(alpha);
  int64_t i = 0;
  for (; i + 16 <= size; i += 16) {
    const __m256 x0 = _mm256_loadu_ps(x + i);
    const __m256 x1 = _mm256_loadu_ps(x + i + 8);
    const __m256 y0 = _mm256_loadu_ps(y + i);
    const __m256 y1 = _mm256_loadu_ps(y + i + 8);
    _mm256_storeu_ps(y + i, _mm256_fmadd_ps(alpha8, x0, y0));
    _mm256_storeu_ps(y + i + 8, _mm256_fmadd_ps(alpha8, x1, y1));
  }
  for (; i + 8 <= size; i += 8) {
    const __m256 x8 = _mm256_loadu_ps(x + i);
    const __m256 y8 = _mm256_loadu_ps(y + i);
    _mm256_storeu_ps(y + i, _mm256_fmadd_ps(alpha8, x8, y8));
  }
  axpy_scalar(y + i, x + i, alpha, size - i);
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE void axpy_avx512(
    float *y, const float *x, const float alpha, const int64_t size) {
  const __m512 alpha16 = _mm512_set1_ps(alpha);
  int64_t i = 0;
  for (; i + 32 <= size; i += 32) {
    const __m512 x0 = _mm512_loadu_ps(x + i);
    const __m512 x1 = _mm512_loadu_ps(x + i + 16);
    const __m512 y0 = _mm512_loadu_ps(y + i);
    const __m512 y1 = _mm512_loadu_ps(y + i + 16);
    _mm512_storeu_ps(y + i, _mm512_fmadd_ps(alpha16, x0, y0));
    _mm512_storeu_ps(y + i + 16, _mm512_fmadd_ps(alpha16, x1, y1));
  }
  for (; i + 16 <= size; i += 16) {
    const __m512 x16 = _mm512_loadu_ps(x + i);
    const __m512 y16 = _mm512_loadu_ps(y + i);
    _mm512_storeu_ps(y + i, _mm512_fmadd_ps(alpha16, x16, y16));
  }
  axpy_scalar(y + i, x + i, alpha, size - i);
}

OB_SSE42_FUNCTION_SPECIFIC_ATTRIBUTE void axpby_sse42(
    float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  const __m128 alpha4 = _mm_set1_ps(alpha);
  const __m128 beta4 = _mm_set1_ps(beta);
  int64_t i = 0;
  for (; i + 8 <= size; i += 8) {
    const __m128 x0 = _mm_loadu_ps(x + i);
    const __m128 x1 = _mm_loadu_ps(x + i + 4);
    const __m128 y0 = _mm_loadu_ps(y + i);
    const __m128 y1 = _mm_loadu_ps(y + i + 4);
    const __m128 result0 = _mm_add_ps(_mm_mul_ps(alpha4, x0), _mm_mul_ps(beta4, y0));
    const __m128 result1 = _mm_add_ps(_mm_mul_ps(alpha4, x1), _mm_mul_ps(beta4, y1));
    _mm_storeu_ps(y + i, result0);
    _mm_storeu_ps(y + i + 4, result1);
  }
  for (; i + 4 <= size; i += 4) {
    const __m128 x4 = _mm_loadu_ps(x + i);
    const __m128 y4 = _mm_loadu_ps(y + i);
    const __m128 result4 = _mm_add_ps(_mm_mul_ps(alpha4, x4), _mm_mul_ps(beta4, y4));
    _mm_storeu_ps(y + i, result4);
  }
  axpby_scalar(y + i, x + i, alpha, beta, size - i);
}

OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void axpby_avx2(
    float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  const __m256 alpha8 = _mm256_set1_ps(alpha);
  const __m256 beta8 = _mm256_set1_ps(beta);
  int64_t i = 0;
  for (; i + 16 <= size; i += 16) {
    const __m256 x0 = _mm256_loadu_ps(x + i);
    const __m256 x1 = _mm256_loadu_ps(x + i + 8);
    const __m256 y0 = _mm256_loadu_ps(y + i);
    const __m256 y1 = _mm256_loadu_ps(y + i + 8);
    const __m256 scaled_y0 = _mm256_mul_ps(beta8, y0);
    const __m256 scaled_y1 = _mm256_mul_ps(beta8, y1);
    _mm256_storeu_ps(y + i, _mm256_fmadd_ps(alpha8, x0, scaled_y0));
    _mm256_storeu_ps(y + i + 8, _mm256_fmadd_ps(alpha8, x1, scaled_y1));
  }
  for (; i + 8 <= size; i += 8) {
    const __m256 x8 = _mm256_loadu_ps(x + i);
    const __m256 y8 = _mm256_loadu_ps(y + i);
    const __m256 scaled_y8 = _mm256_mul_ps(beta8, y8);
    _mm256_storeu_ps(y + i, _mm256_fmadd_ps(alpha8, x8, scaled_y8));
  }
  axpby_scalar(y + i, x + i, alpha, beta, size - i);
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE void axpby_avx512(
    float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  const __m512 alpha16 = _mm512_set1_ps(alpha);
  const __m512 beta16 = _mm512_set1_ps(beta);
  int64_t i = 0;
  for (; i + 32 <= size; i += 32) {
    const __m512 x0 = _mm512_loadu_ps(x + i);
    const __m512 x1 = _mm512_loadu_ps(x + i + 16);
    const __m512 y0 = _mm512_loadu_ps(y + i);
    const __m512 y1 = _mm512_loadu_ps(y + i + 16);
    const __m512 scaled_y0 = _mm512_mul_ps(beta16, y0);
    const __m512 scaled_y1 = _mm512_mul_ps(beta16, y1);
    _mm512_storeu_ps(y + i, _mm512_fmadd_ps(alpha16, x0, scaled_y0));
    _mm512_storeu_ps(y + i + 16, _mm512_fmadd_ps(alpha16, x1, scaled_y1));
  }
  for (; i + 16 <= size; i += 16) {
    const __m512 x16 = _mm512_loadu_ps(x + i);
    const __m512 y16 = _mm512_loadu_ps(y + i);
    const __m512 scaled_y16 = _mm512_mul_ps(beta16, y16);
    _mm512_storeu_ps(y + i, _mm512_fmadd_ps(alpha16, x16, scaled_y16));
  }
  axpby_scalar(y + i, x + i, alpha, beta, size - i);
}
#endif

#if OB_ARM_USE_MULTITARGET_CODE
float squared_norm_neon(const float *x, const int64_t size) {
  float32x4_t sum = vdupq_n_f32(0.0F);
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const float32x4_t value = vld1q_f32(x + i);
    sum = vfmaq_f32(sum, value, value);
  }
  return vaddvq_f32(sum) + squared_norm_scalar(x + i, size - i);
}

void axpy_neon(float *y, const float *x, const float alpha, const int64_t size) {
  const float32x4_t factor = vdupq_n_f32(alpha);
  int64_t i = 0;
  for (; i + 16 <= size; i += 16) {
    const float32x4_t x0 = vld1q_f32(x + i);
    const float32x4_t x1 = vld1q_f32(x + i + 4);
    const float32x4_t x2 = vld1q_f32(x + i + 8);
    const float32x4_t x3 = vld1q_f32(x + i + 12);
    const float32x4_t y0 = vld1q_f32(y + i);
    const float32x4_t y1 = vld1q_f32(y + i + 4);
    const float32x4_t y2 = vld1q_f32(y + i + 8);
    const float32x4_t y3 = vld1q_f32(y + i + 12);
    vst1q_f32(y + i, vfmaq_f32(y0, factor, x0));
    vst1q_f32(y + i + 4, vfmaq_f32(y1, factor, x1));
    vst1q_f32(y + i + 8, vfmaq_f32(y2, factor, x2));
    vst1q_f32(y + i + 12, vfmaq_f32(y3, factor, x3));
  }
  for (; i + 4 <= size; i += 4) {
    vst1q_f32(y + i, vfmaq_f32(vld1q_f32(y + i), factor, vld1q_f32(x + i)));
  }
  axpy_scalar(y + i, x + i, alpha, size - i);
}

void axpby_neon(float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  const float32x4_t alpha4 = vdupq_n_f32(alpha);
  const float32x4_t beta4 = vdupq_n_f32(beta);
  int64_t i = 0;
  for (; i + 16 <= size; i += 16) {
    const float32x4_t x0 = vld1q_f32(x + i);
    const float32x4_t x1 = vld1q_f32(x + i + 4);
    const float32x4_t x2 = vld1q_f32(x + i + 8);
    const float32x4_t x3 = vld1q_f32(x + i + 12);
    const float32x4_t y0 = vld1q_f32(y + i);
    const float32x4_t y1 = vld1q_f32(y + i + 4);
    const float32x4_t y2 = vld1q_f32(y + i + 8);
    const float32x4_t y3 = vld1q_f32(y + i + 12);
    vst1q_f32(y + i, vfmaq_f32(vmulq_f32(beta4, y0), alpha4, x0));
    vst1q_f32(y + i + 4, vfmaq_f32(vmulq_f32(beta4, y1), alpha4, x1));
    vst1q_f32(y + i + 8, vfmaq_f32(vmulq_f32(beta4, y2), alpha4, x2));
    vst1q_f32(y + i + 12, vfmaq_f32(vmulq_f32(beta4, y3), alpha4, x3));
  }
  for (; i + 4 <= size; i += 4) {
    const float32x4_t scaled_y = vmulq_f32(beta4, vld1q_f32(y + i));
    vst1q_f32(y + i, vfmaq_f32(scaled_y, alpha4, vld1q_f32(x + i)));
  }
  axpby_scalar(y + i, x + i, alpha, beta, size - i);
}
#endif

// Runtime entry points deliberately prefer AVX2 over AVX-512. AVX-512 is not
// consistently faster for these memory-heavy kernels and can reduce CPU clock
// frequency on older Xeon processors.
float squared_norm(const float *x, const int64_t size) {
  float result = 0.0F;
  if (size > 0) {
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
      result = squared_norm_avx2(x, size);
    } else if (common::is_arch_supported(common::ObTargetArch::SSE42)) {
      result = squared_norm_sse42(x, size);
    } else {
      result = squared_norm_scalar(x, size);
    }
#elif OB_ARM_USE_MULTITARGET_CODE
    result = common::is_arch_supported(common::ObTargetArch::NEON)
        ? squared_norm_neon(x, size) : squared_norm_scalar(x, size);
#else
    result = squared_norm_scalar(x, size);
#endif
  }
  return result;
}

void axpy(float *y, const float *x, const float alpha, const int64_t size) {
  if (size > 0) {
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
      axpy_avx2(y, x, alpha, size);
    } else if (common::is_arch_supported(common::ObTargetArch::SSE42)) {
      axpy_sse42(y, x, alpha, size);
    } else {
      axpy_scalar(y, x, alpha, size);
    }
#elif OB_ARM_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::NEON)) {
      axpy_neon(y, x, alpha, size);
    } else {
      axpy_scalar(y, x, alpha, size);
    }
#else
    axpy_scalar(y, x, alpha, size);
#endif
  }
}

void axpby(float *y, const float *x, const float alpha, const float beta, const int64_t size) {
  if (size > 0) {
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
      axpby_avx2(y, x, alpha, beta, size);
    } else if (common::is_arch_supported(common::ObTargetArch::SSE42)) {
      axpby_sse42(y, x, alpha, beta, size);
    } else {
      axpby_scalar(y, x, alpha, beta, size);
    }
#elif OB_ARM_USE_MULTITARGET_CODE
    if (common::is_arch_supported(common::ObTargetArch::NEON)) {
      axpby_neon(y, x, alpha, beta, size);
    } else {
      axpby_scalar(y, x, alpha, beta, size);
    }
#else
    axpby_scalar(y, x, alpha, beta, size);
#endif
  }
}

static bool count_statistics_scalar(const int64_t *counts, const int64_t size, int64_t &sum,
    int64_t &maximum, int64_t &zeros, double &squared_deviations)
{
  sum = maximum = zeros = 0;
  squared_deviations = 0;
  for (int64_t i = 0; i < size; ++i) {
    if (counts[i] < 0 || counts[i] > INT64_MAX - sum) { return false; }
    sum += counts[i];
    maximum = std::max(maximum, counts[i]);
    zeros += counts[i] == 0;
  }
  const double mean = size > 0 ? static_cast<double>(sum) / size : 0;
  for (int64_t i = 0; i < size; ++i) {
    const double delta = static_cast<double>(counts[i]) - mean;
    squared_deviations += delta * delta;
  }
  return true;
}

#if OB_USE_MULTITARGET_CODE
static OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE bool count_statistics_avx2(
    const int64_t *counts, const int64_t size, int64_t &sum,
    int64_t &maximum, int64_t &zeros, double &squared_deviations)
{
  sum = maximum = zeros = 0;
  squared_deviations = 0;
  __m256i totals = _mm256_setzero_si256();
  __m256i maxima = totals;
  const __m256i zero = totals;
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const __m256i values = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(counts + i));
    if (_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(zero, values)))) { return false; }
    const __m256i next = _mm256_add_epi64(totals, values);
    if (_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(totals, next)))) { return false; }
    totals = next;
    maxima = _mm256_blendv_epi8(maxima, values, _mm256_cmpgt_epi64(values, maxima));
    zeros += __builtin_popcount(_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(values, zero))));
  }
  int64_t lanes[4];
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(lanes), totals);
  for (int lane = 0; lane < 4; ++lane) {
    if (lanes[lane] > INT64_MAX - sum) { return false; }
    sum += lanes[lane];
  }
  _mm256_storeu_si256(reinterpret_cast<__m256i *>(lanes), maxima);
  for (int lane = 0; lane < 4; ++lane) { maximum = std::max(maximum, lanes[lane]); }
  for (; i < size; ++i) {
    if (counts[i] < 0 || counts[i] > INT64_MAX - sum) { return false; }
    sum += counts[i]; maximum = std::max(maximum, counts[i]); zeros += counts[i] == 0;
  }
  const double mean = size > 0 ? static_cast<double>(sum) / size : 0;
  const __m256d means = _mm256_set1_pd(mean);
  __m256d squared = _mm256_setzero_pd();
  i = 0;
  for (; i + 4 <= size; i += 4) {
    // AVX2 has no packed int64-to-double conversion. Preserve full count precision.
    const __m256d values = _mm256_set_pd(static_cast<double>(counts[i + 3]),
        static_cast<double>(counts[i + 2]), static_cast<double>(counts[i + 1]),
        static_cast<double>(counts[i]));
    const __m256d delta = _mm256_sub_pd(values, means);
    squared = _mm256_add_pd(squared, _mm256_mul_pd(delta, delta));
  }
  double parts[4];
  _mm256_storeu_pd(parts, squared);
  squared_deviations = (parts[0] + parts[1]) + (parts[2] + parts[3]);
  for (; i < size; ++i) {
    const double delta = static_cast<double>(counts[i]) - mean;
    squared_deviations += delta * delta;
  }
  return true;
}

static OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void update_min_avx2(
    float *minimum, const float *distances, const int64_t size)
{
  int64_t i = 0;
  for (; i + 8 <= size; i += 8) {
    _mm256_storeu_ps(minimum + i, _mm256_min_ps(_mm256_loadu_ps(minimum + i), _mm256_loadu_ps(distances + i)));
  }
  for (; i < size; ++i) { minimum[i] = std::min(minimum[i], distances[i]); }
}

static OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void update_weights_avx2(
    float *minimum, double *weights, const float *distances, const double scale, const int64_t size)
{
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const __m128 distance = _mm_loadu_ps(distances + i);
    const __m128 previous = _mm_loadu_ps(minimum + i);
    const __m128 mask = _mm_cmplt_ps(distance, previous);
    const __m256d mask64 = _mm256_castsi256_pd(_mm256_cvtepi32_epi64(_mm_castps_si128(mask)));
    _mm256_storeu_pd(weights + i, _mm256_blendv_pd(_mm256_loadu_pd(weights + i),
        _mm256_mul_pd(_mm256_cvtps_pd(distance), _mm256_set1_pd(scale)), mask64));
    _mm_storeu_ps(minimum + i, _mm_min_ps(previous, distance));
  }
  for (; i < size; ++i) {
    if (distances[i] < minimum[i]) { minimum[i] = distances[i]; weights[i] = scale * distances[i]; }
  }
}

static OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE double sum_weights_avx2(const double *weights, const int64_t size)
{
  __m256d sum = _mm256_setzero_pd();
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) { sum = _mm256_add_pd(sum, _mm256_loadu_pd(weights + i)); }
  double parts[4]; _mm256_storeu_pd(parts, sum);
  double result = (parts[0] + parts[1]) + (parts[2] + parts[3]);
  for (; i < size; ++i) { result += weights[i]; }
  return result;
}

static OB_AVX2_FUNCTION_SPECIFIC_ATTRIBUTE double weighted_distances_avx2(
    const float *distances, const int64_t *mass, const int64_t size, double *output_weights)
{
  __m256d sum = _mm256_setzero_pd();
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const __m256d weights = _mm256_set_pd(static_cast<double>(mass[i + 3]),
        static_cast<double>(mass[i + 2]), static_cast<double>(mass[i + 1]), static_cast<double>(mass[i]));
    const __m256d products = _mm256_mul_pd(weights, _mm256_cvtps_pd(_mm_loadu_ps(distances + i)));
    sum = _mm256_add_pd(sum, products);
    if (output_weights != nullptr) { _mm256_storeu_pd(output_weights + i, products); }
  }
  double parts[4]; _mm256_storeu_pd(parts, sum);
  double result = (parts[0] + parts[1]) + (parts[2] + parts[3]);
  for (; i < size; ++i) {
    const double weight = static_cast<double>(mass[i]) * distances[i];
    if (output_weights != nullptr) { output_weights[i] = weight; }
    result += weight;
  }
  return result;
}
#endif

#if OB_ARM_USE_MULTITARGET_CODE
static bool count_statistics_neon(const int64_t *counts, const int64_t size, int64_t &sum,
    int64_t &maximum, int64_t &zeros, double &squared_deviations)
{
  // Keep the overflow check in integer arithmetic and in input order.
  sum = maximum = zeros = 0;
  squared_deviations = 0;
  for (int64_t i = 0; i < size; ++i) {
    if (counts[i] < 0 || counts[i] > INT64_MAX - sum) { return false; }
    sum += counts[i];
    maximum = std::max(maximum, counts[i]);
    zeros += counts[i] == 0;
  }
  const double mean = size > 0 ? static_cast<double>(sum) / size : 0;
  const float64x2_t means = vdupq_n_f64(mean);
  float64x2_t squares = vdupq_n_f64(0);
  int64_t i = 0;
  for (; i + 2 <= size; i += 2) {
    const float64x2_t values = vcvtq_f64_s64(vld1q_s64(counts + i));
    const float64x2_t delta = vsubq_f64(values, means);
    squares = vaddq_f64(squares, vmulq_f64(delta, delta));
  }
  squared_deviations = vaddvq_f64(squares);
  for (; i < size; ++i) {
    const double delta = static_cast<double>(counts[i]) - mean;
    squared_deviations += delta * delta;
  }
  return true;
}

static void update_min_neon(float *minimum, const float *distances, const int64_t size)
{
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const float32x4_t previous = vld1q_f32(minimum + i);
    const float32x4_t distance = vld1q_f32(distances + i);
    vst1q_f32(minimum + i, vbslq_f32(vcltq_f32(distance, previous), distance, previous));
  }
  for (; i < size; ++i) { minimum[i] = std::min(minimum[i], distances[i]); }
}

static void update_weights_neon(float *minimum, double *weights,
    const float *distances, const double scale, const int64_t size)
{
  const float64x2_t scales = vdupq_n_f64(scale);
  int64_t i = 0;
  for (; i + 4 <= size; i += 4) {
    const float32x4_t previous = vld1q_f32(minimum + i);
    const float32x4_t distance = vld1q_f32(distances + i);
    const uint32x4_t mask = vcltq_f32(distance, previous);
    const uint64x2_t low_mask = vceqq_u64(vmovl_u32(vget_low_u32(mask)), vdupq_n_u64(UINT32_MAX));
    const uint64x2_t high_mask = vceqq_u64(vmovl_u32(vget_high_u32(mask)), vdupq_n_u64(UINT32_MAX));
    const float64x2_t low = vmulq_f64(scales, vcvt_f64_f32(vget_low_f32(distance)));
    const float64x2_t high = vmulq_f64(scales, vcvt_f64_f32(vget_high_f32(distance)));
    vst1q_f64(weights + i, vbslq_f64(low_mask, low, vld1q_f64(weights + i)));
    vst1q_f64(weights + i + 2, vbslq_f64(high_mask, high, vld1q_f64(weights + i + 2)));
    vst1q_f32(minimum + i, vbslq_f32(mask, distance, previous));
  }
  for (; i < size; ++i) {
    if (distances[i] < minimum[i]) { minimum[i] = distances[i]; weights[i] = scale * distances[i]; }
  }
}

static double sum_weights_neon(const double *weights, const int64_t size)
{
  float64x2_t sum = vdupq_n_f64(0);
  int64_t i = 0;
  for (; i + 2 <= size; i += 2) { sum = vaddq_f64(sum, vld1q_f64(weights + i)); }
  double result = vaddvq_f64(sum);
  for (; i < size; ++i) { result += weights[i]; }
  return result;
}

static double weighted_distances_neon(const float *distances, const int64_t *mass,
    const int64_t size, double *output_weights)
{
  float64x2_t sum = vdupq_n_f64(0);
  int64_t i = 0;
  for (; i + 2 <= size; i += 2) {
    const float64x2_t weights = vmulq_f64(vcvtq_f64_s64(vld1q_s64(mass + i)),
        vcvt_f64_f32(vld1_f32(distances + i)));
    sum = vaddq_f64(sum, weights);
    if (output_weights != nullptr) { vst1q_f64(output_weights + i, weights); }
  }
  double result = vaddvq_f64(sum);
  for (; i < size; ++i) {
    const double weight = static_cast<double>(mass[i]) * distances[i];
    if (output_weights != nullptr) { output_weights[i] = weight; }
    result += weight;
  }
  return result;
}
#endif

bool count_statistics(const int64_t *counts, const int64_t size, int64_t &sum,
    int64_t &maximum, int64_t &zeros, double &squared_deviations)
{
  if (size < 0 || (size > 0 && counts == nullptr)) { return false; }
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) {
    return count_statistics_avx2(counts, size, sum, maximum, zeros, squared_deviations);
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) {
    return count_statistics_neon(counts, size, sum, maximum, zeros, squared_deviations);
  }
#endif
  return count_statistics_scalar(counts, size, sum, maximum, zeros, squared_deviations);
}

void update_min_distances(float *minimum, const float *distances, const int64_t size)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) { update_min_avx2(minimum, distances, size); return; }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) { update_min_neon(minimum, distances, size); return; }
#endif
  for (int64_t i = 0; i < size; ++i) { minimum[i] = std::min(minimum[i], distances[i]); }
}

void update_min_distance_weights(float *minimum, double *weights,
    const float *distances, const double scale, const int64_t size)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) { update_weights_avx2(minimum, weights, distances, scale, size); return; }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) {
    update_weights_neon(minimum, weights, distances, scale, size);
    return;
  }
#endif
  for (int64_t i = 0; i < size; ++i) {
    if (distances[i] < minimum[i]) { minimum[i] = distances[i]; weights[i] = scale * distances[i]; }
  }
}

double sum_weights(const double *weights, const int64_t size)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) { return sum_weights_avx2(weights, size); }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) { return sum_weights_neon(weights, size); }
#endif
  double sum = 0; for (int64_t i = 0; i < size; ++i) { sum += weights[i]; } return sum;
}

double weighted_distances(const float *distances, const int64_t *mass, const int64_t size, double *weights)
{
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::AVX2)) { return weighted_distances_avx2(distances, mass, size, weights); }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(common::ObTargetArch::NEON)) {
    return weighted_distances_neon(distances, mass, size, weights);
  }
#endif
  double sum = 0;
  for (int64_t i = 0; i < size; ++i) {
    const double weight = static_cast<double>(mass[i]) * distances[i];
    if (weights != nullptr) { weights[i] = weight; }
    sum += weight;
  }
  return sum;
}

} // namespace hca_simd
} // namespace share
} // namespace oceanbase
