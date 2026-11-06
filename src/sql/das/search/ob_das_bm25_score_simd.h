/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef SQL_DAS_SEARCH_OB_DAS_BM25_SCORE_SIMD_H_
#define SQL_DAS_SEARCH_OB_DAS_BM25_SCORE_SIMD_H_

#include "common/ob_target_specific.h"
#include "sql/engine/expr/ob_expr_bm25.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#elif OB_ARM_USE_MULTITARGET_CODE
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace sql
{

#if OB_USE_MULTITARGET_CODE
// AVX512F suffices: the project's AVX512 dispatch does not require AVX512DQ.
// Both uint32 halves are exactly representable as double, the power-of-two
// scaling is exact, and their addition rounds exactly as uint64 -> double.
OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
static OB_INLINE __m512d relevance_to_double8(const uint64_t *values)
{
  static constexpr double HIGH32_SCALE = static_cast<double>(1ULL << 32);
  const __m512i integers = _mm512_loadu_si512(values);
  const __m512d lo = _mm512_cvtepu32_pd(_mm512_cvtepi64_epi32(integers));
  const __m512d hi = _mm512_cvtepu32_pd(
      _mm512_cvtepi64_epi32(_mm512_srli_epi64(integers, 32)));
  return _mm512_add_pd(lo, _mm512_mul_pd(hi, _mm512_set1_pd(HIGH32_SCALE)));
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
static OB_INLINE __m512d relevance_to_double8(const uint32_t *values)
{
  return _mm512_cvtepu32_pd(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(values)));
}

// Match ObExprBM25::eval_with_precomputed_params, including its floating-point order.
// Parameters are broadcast by the caller; active selects lanes that may divide.
OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static OB_INLINE __m512d eval_bm25_avx512(
    const __m512d tf, const __m512d dl,
    const __m512d bias, const __m512d scale,
    const __m512d weight, const __m512d boost,
    const __mmask8 active)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  const __m512d biased_tf = _mm512_add_pd(tf, bias);
  const __m512d scaled_dl = _mm512_mul_pd(scale, dl);
  const __m512d denom = _mm512_add_pd(biased_tf, scaled_dl);
  return _mm512_mul_pd(_mm512_mul_pd(weight, _mm512_maskz_div_pd(active, tf, denom)), boost);
}
#endif

#if OB_ARM_USE_MULTITARGET_CODE
namespace specific
{
namespace neon
{
static OB_INLINE float64x2_t relevance_to_double2(const uint64_t *values)
{
  return vcvtq_f64_u64(vld1q_u64(values));
}

static OB_INLINE float64x2_t relevance_to_double2(const uint32_t *values)
{
  return vcvtq_f64_u64(vmovl_u32(vld1_u32(values)));
}

#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static OB_INLINE float64x2_t eval_bm25_neon(
    const float64x2_t tf, const float64x2_t dl,
    const float64x2_t bias, const float64x2_t scale,
    const float64x2_t weight, const float64x2_t boost,
    const uint64x2_t active)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  const float64x2_t biased_tf = vaddq_f64(tf, bias);
  const float64x2_t scaled_dl = vmulq_f64(scale, dl);
  const float64x2_t denom = vaddq_f64(biased_tf, scaled_dl);
  // NEON has no masked division; inactive lanes evaluate 0 / 1.
  const float64x2_t active_tf = vbslq_f64(active, tf, vdupq_n_f64(0.0));
  const float64x2_t active_denom = vbslq_f64(active, denom, vdupq_n_f64(1.0));
  return vmulq_f64(vmulq_f64(weight, vdivq_f64(active_tf, active_denom)), boost);
}

} // namespace neon
} // namespace specific
#endif

} // namespace sql
} // namespace oceanbase

#endif // SQL_DAS_SEARCH_OB_DAS_BM25_SCORE_SIMD_H_
