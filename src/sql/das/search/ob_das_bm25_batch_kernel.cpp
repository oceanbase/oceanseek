/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/das/search/ob_das_bm25_score_simd.h"
#include "common/ob_target_specific.h"
#include "storage/blocksstable/async_index/ob_inv_idx_bm25_max_score_aggregator.h"
#include <algorithm>
#include <cstddef>

namespace oceanbase
{
namespace sql
{

using blocksstable::ObInvIdxBM25ParamPair;

static bool is_bm25_arch_supported(const common::ObTargetArch arch)
{
  bool supported = common::ObTargetArch::Default == arch;
#if OB_USE_MULTITARGET_CODE
  supported = supported || (common::ObTargetArch::AVX512 == arch
      && common::is_arch_supported(common::ObTargetArch::AVX512));
#elif OB_ARM_USE_MULTITARGET_CODE
  supported = supported || (common::ObTargetArch::NEON == arch
      && common::is_arch_supported(common::ObTargetArch::NEON));
#endif
  return supported;
}

static common::ObTargetArch select_bm25_arch()
{
  common::ObTargetArch arch = common::ObTargetArch::Default;
#if OB_USE_MULTITARGET_CODE
  if (is_bm25_arch_supported(common::ObTargetArch::AVX512)) {
    arch = common::ObTargetArch::AVX512;
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (is_bm25_arch_supported(common::ObTargetArch::NEON)) {
    arch = common::ObTargetArch::NEON;
  }
#endif
  return arch;
}

template <bool ONLY_EXISTING>
BM25AccumulationKernels<ONLY_EXISTING> get_bm25_accumulation_kernels()
{
  return BM25AccumulationKernels<ONLY_EXISTING>::make(select_bm25_arch());
}

template BM25AccumulationKernels<false> get_bm25_accumulation_kernels<false>();
template BM25AccumulationKernels<true> get_bm25_accumulation_kernels<true>();

template <bool ONLY_EXISTING>
int get_bm25_accumulation_kernels_for_arch(
    const common::ObTargetArch arch, BM25AccumulationKernels<ONLY_EXISTING> &kernels)
{
  int ret = OB_SUCCESS;
  kernels = {};
  if (!is_bm25_arch_supported(arch)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported bm25 accumulation kernel architecture", K(ret), K(arch), K(ONLY_EXISTING));
  } else {
    kernels = BM25AccumulationKernels<ONLY_EXISTING>::make(arch);
  }
  return ret;
}

template int get_bm25_accumulation_kernels_for_arch<false>(
    const common::ObTargetArch arch, BM25AccumulationKernels<false> &kernels);
template int get_bm25_accumulation_kernels_for_arch<true>(
    const common::ObTargetArch arch, BM25AccumulationKernels<true> &kernels);

template <typename Relevance>
static void score_bm25_scalar(
    const Relevance *token_freqs,
    const Relevance *doc_lengths,
    const int64_t count,
    const ObBM25PrecomputedParams &params,
    double *scores)
{
  for (int64_t i = 0; i < count; ++i) {
    scores[i] = ObExprBM25::eval_with_precomputed_params(token_freqs[i], doc_lengths[i], params);
  }
}

static_assert(sizeof(ObInvIdxBM25ParamPair) == 8, "frontier pair must contain two uint32 values");
static_assert(offsetof(ObInvIdxBM25ParamPair, token_freq_) == 0
    && offsetof(ObInvIdxBM25ParamPair, doc_length_) == 4, "unexpected frontier pair layout");

static double frontier_max_scalar(
    const ObInvIdxBM25ParamPair *pairs, const int64_t count,
    const ObBM25PrecomputedParams &params, double maximum)
{
  for (int64_t i = 0; i < count; ++i) {
    maximum = std::max(maximum, ObExprBM25::eval_with_precomputed_params(
        pairs[i].token_freq_, pairs[i].doc_length_, params));
  }
  return maximum;
}

#if OB_USE_MULTITARGET_CODE
template <typename Relevance>
OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static void score_bm25_avx512(
    const Relevance *token_freqs,
    const Relevance *doc_lengths,
    const int64_t count,
    const ObBM25PrecomputedParams &params,
    double *scores)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  const __m512d bias = _mm512_set1_pd(params.denom_bias_);
  const __m512d scale = _mm512_set1_pd(params.doc_len_scale_);
  const __m512d weight = _mm512_set1_pd(params.query_weight_);
  const __m512d boost = _mm512_set1_pd(params.score_multiplier_);
  int64_t i = 0;
  for (; i + 8 <= count; i += 8) {
    const __m512d tf = relevance_to_double8(token_freqs + i);
    const __m512d dl = relevance_to_double8(doc_lengths + i);
    const __m512d values = eval_bm25_avx512(tf, dl, bias, scale, weight, boost, 0xFF);
    _mm512_storeu_pd(scores + i, values);
  }
  if (i < count) {
    score_bm25_scalar(token_freqs + i, doc_lengths + i, count - i, params, scores + i);
  }
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static double frontier_max_avx512(
    const ObInvIdxBM25ParamPair *pairs, const int64_t count,
    const ObBM25PrecomputedParams &params, double maximum)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  if (count < 8) {
    maximum = frontier_max_scalar(pairs, count, params, maximum);
  } else {
    const __m512d bias = _mm512_set1_pd(params.denom_bias_);
    const __m512d scale = _mm512_set1_pd(params.doc_len_scale_);
    const __m512d weight = _mm512_set1_pd(params.query_weight_);
    const __m512d boost = _mm512_set1_pd(params.score_multiplier_);
    __m512d maxima = _mm512_set1_pd(maximum);
    int64_t i = 0;
    for (; i + 8 <= count; i += 8) {
      const __m512i packed = _mm512_loadu_si512(pairs + i);
      const __m512d tf = _mm512_cvtepu32_pd(_mm512_cvtepi64_epi32(packed));
      const __m512d dl = _mm512_cvtepu32_pd(
          _mm512_cvtepi64_epi32(_mm512_srli_epi64(packed, 32)));
      const __m512d scores = eval_bm25_avx512(tf, dl, bias, scale, weight, boost, 0xff);
      // Match std::max's ordered comparison even for unusual NaN parameters.
      maxima = _mm512_mask_mov_pd(maxima, _mm512_cmp_pd_mask(scores, maxima, _CMP_GT_OQ), scores);
    }
    maximum = _mm512_reduce_max_pd(maxima);
    if (i < count) {
      maximum = frontier_max_scalar(pairs + i, count - i, params, maximum);
    }
  }
  return maximum;
}
#endif

#if OB_ARM_USE_MULTITARGET_CODE
namespace specific
{
namespace neon
{
template <typename Relevance>
#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static void score_bm25_neon(
    const Relevance *token_freqs,
    const Relevance *doc_lengths,
    const int64_t count,
    const ObBM25PrecomputedParams &params,
    double *scores)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  const float64x2_t bias = vdupq_n_f64(params.denom_bias_);
  const float64x2_t scale = vdupq_n_f64(params.doc_len_scale_);
  const float64x2_t weight = vdupq_n_f64(params.query_weight_);
  const float64x2_t boost = vdupq_n_f64(params.score_multiplier_);
  int64_t i = 0;
  for (; i + 2 <= count; i += 2) {
    const float64x2_t tf = relevance_to_double2(token_freqs + i);
    const float64x2_t dl = relevance_to_double2(doc_lengths + i);
    const float64x2_t values = eval_bm25_neon(tf, dl, bias, scale, weight, boost, vdupq_n_u64(UINT64_MAX));
    vst1q_f64(scores + i, values);
  }
  if (i < count) {
    score_bm25_scalar(token_freqs + i, doc_lengths + i, count - i, params, scores + i);
  }
}

#if !defined(__clang__)
__attribute__((optimize("fp-contract=off")))
#endif
static double frontier_max_neon(
    const ObInvIdxBM25ParamPair *pairs, const int64_t count,
    const ObBM25PrecomputedParams &params, double maximum)
{
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
  if (count < 2) {
    maximum = frontier_max_scalar(pairs, count, params, maximum);
  } else {
    const float64x2_t bias = vdupq_n_f64(params.denom_bias_);
    const float64x2_t scale = vdupq_n_f64(params.doc_len_scale_);
    const float64x2_t weight = vdupq_n_f64(params.query_weight_);
    const float64x2_t boost = vdupq_n_f64(params.score_multiplier_);
    float64x2_t maxima = vdupq_n_f64(maximum);
    int64_t i = 0;
    for (; i + 2 <= count; i += 2) {
      const uint32x2x2_t packed = vld2_u32(reinterpret_cast<const uint32_t *>(pairs + i));
      const float64x2_t tf = vcvtq_f64_u64(vmovl_u32(packed.val[0]));
      const float64x2_t dl = vcvtq_f64_u64(vmovl_u32(packed.val[1]));
      const float64x2_t scores = eval_bm25_neon(tf, dl, bias, scale, weight, boost, vdupq_n_u64(UINT64_MAX));
      maxima = vbslq_f64(vcgtq_f64(scores, maxima), scores, maxima);
    }
    maximum = std::max(vgetq_lane_f64(maxima, 0), vgetq_lane_f64(maxima, 1));
    if (i < count) {
      maximum = frontier_max_scalar(pairs + i, count - i, params, maximum);
    }
  }
  return maximum;
}

} // namespace neon
} // namespace specific
#endif

BM25ScoreKernels BM25ScoreKernels::make(const common::ObTargetArch arch)
{
  BM25ScoreKernels kernels(
      score_bm25_scalar<uint32_t>, score_bm25_scalar<uint64_t>, arch);
#if OB_USE_MULTITARGET_CODE
  if (common::ObTargetArch::AVX512 == arch) {
    kernels = {score_bm25_avx512<uint32_t>, score_bm25_avx512<uint64_t>, arch};
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::ObTargetArch::NEON == arch) {
    kernels = {specific::neon::score_bm25_neon<uint32_t>, specific::neon::score_bm25_neon<uint64_t>, arch};
  }
#endif
  return kernels;
}

BM25ScoreKernels get_bm25_score_kernels()
{
  return BM25ScoreKernels::make(select_bm25_arch());
}

int get_bm25_score_kernels_for_arch(const common::ObTargetArch arch, BM25ScoreKernels &kernels)
{
  int ret = OB_SUCCESS;
  kernels = {};
  if (!is_bm25_arch_supported(arch)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported bm25 score kernel architecture", K(ret), K(arch));
  } else {
    kernels = BM25ScoreKernels::make(arch);
  }
  return ret;
}

BM25FrontierMaxKernels BM25FrontierMaxKernels::make(const common::ObTargetArch arch)
{
  BM25FrontierMaxKernels kernels(frontier_max_scalar, arch);
#if OB_USE_MULTITARGET_CODE
  if (common::ObTargetArch::AVX512 == arch) {
    kernels = {frontier_max_avx512, arch};
  }
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::ObTargetArch::NEON == arch) {
    kernels = {specific::neon::frontier_max_neon, arch};
  }
#endif
  return kernels;
}

BM25FrontierMaxKernels get_bm25_frontier_max_kernels()
{
  return BM25FrontierMaxKernels::make(select_bm25_arch());
}

int get_bm25_frontier_max_kernels_for_arch(
    const common::ObTargetArch arch, BM25FrontierMaxKernels &kernels)
{
  int ret = OB_SUCCESS;
  kernels = {};
  if (!is_bm25_arch_supported(arch)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported bm25 frontier max kernel architecture", K(ret), K(arch));
  } else {
    kernels = BM25FrontierMaxKernels::make(arch);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
