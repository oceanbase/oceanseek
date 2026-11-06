/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/das/search/ob_das_bm25_score_simd.h"

namespace oceanbase
{
namespace sql
{

#if OB_USE_MULTITARGET_CODE
OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
static OB_INLINE __m512i load_doc_ids8(const uint64_t *values)
{
  return _mm512_loadu_si512(values);
}

OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
static OB_INLINE __m512i load_doc_ids8(const uint32_t *values)
{
  return _mm512_cvtepu32_epi64(_mm256_loadu_si256(reinterpret_cast<const __m256i *>(values)));
}

template <int SHIFT>
OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
static OB_INLINE __m512i presence_prefix_or(const __m512i word_offsets, const __m512i bits)
{
  const __m512i previous_words = _mm512_alignr_epi64(word_offsets, word_offsets, 8 - SHIFT);
  const __mmask8 same_word = _mm512_mask_cmpeq_epi64_mask(
      static_cast<__mmask8>(0xffU << SHIFT), word_offsets, previous_words);
  return _mm512_mask_or_epi64(bits, same_word, bits,
      _mm512_alignr_epi64(bits, bits, 8 - SHIFT));
}

#endif

class DocScoreAccumulator::FusedExecutor final
{
public:
  template <bool ONLY_EXISTING, typename DocId, typename Relevance>
  static int64_t accumulate_bm25_scalar(
      DocScoreAccumulator &accumulator,
      const void *doc_id_values,
      const void *token_freq_values,
      const void *doc_length_values,
      const int64_t count,
      const ObBM25PrecomputedParams &params)
  {
    const uint64_t start_id = accumulator.start_id_;
    const int64_t capacity = accumulator.capacity_;
    double *scores = accumulator.scores_.get_data();
    uint64_t *words = accumulator.bitset_->reinterpret_data<uint64_t>();
    double &max_score = accumulator.max_score_;
    const DocId *doc_ids = static_cast<const DocId *>(doc_id_values);
    const Relevance *token_freqs = static_cast<const Relevance *>(token_freq_values);
    const Relevance *doc_lengths = static_cast<const Relevance *>(doc_length_values);
    int64_t matched = 0;
    for (int64_t i = 0; i < count; ++i) {
      const uint64_t offset = doc_ids[i] - start_id;
      if (!ONLY_EXISTING || (doc_ids[i] >= start_id
          && offset < static_cast<uint64_t>(capacity))) {
        const uint64_t mask = 1ULL << (offset & 63);
        const bool present = 0 != (words[offset >> 6] & mask);
        if (!ONLY_EXISTING || present) {
          const double score = ObExprBM25::eval_with_precomputed_params(
              token_freqs[i], doc_lengths[i], params);
          if (present) {
            scores[offset] += score;
          } else {
            scores[offset] = score;
            words[offset >> 6] |= mask;
          }
          max_score = MAX(max_score, scores[offset]);
          ++matched;
        }
      }
    }
    return matched;
  }

#if OB_USE_MULTITARGET_CODE
  template <bool ONLY_EXISTING, typename DocId, typename Relevance>
  OB_AVX512_FUNCTION_SPECIFIC_ATTRIBUTE
#if !defined(__clang__)
  __attribute__((optimize("fp-contract=off")))
#endif
  static int64_t accumulate_bm25_avx512(
      DocScoreAccumulator &accumulator,
      const void *doc_id_values,
      const void *token_freq_values,
      const void *doc_length_values,
      const int64_t count,
      const ObBM25PrecomputedParams &params)
  {
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
    const uint64_t start_id = accumulator.start_id_;
    const int64_t capacity = accumulator.capacity_;
    double *scores = accumulator.scores_.get_data();
    uint64_t *words = accumulator.bitset_->reinterpret_data<uint64_t>();
    double &max_score = accumulator.max_score_;
    const DocId *doc_ids = static_cast<const DocId *>(doc_id_values);
    const Relevance *token_freqs = static_cast<const Relevance *>(token_freq_values);
    const Relevance *doc_lengths = static_cast<const Relevance *>(doc_length_values);
    int64_t matched = 0;
    if (count < 8) {
      matched = accumulate_bm25_scalar<ONLY_EXISTING, DocId, Relevance>(
          accumulator, doc_ids, token_freqs, doc_lengths, count, params);
    } else {
      const __m512i start_ids = _mm512_set1_epi64(start_id);
      const __m512i capacities = _mm512_set1_epi64(capacity);
      const __m512i ones = _mm512_set1_epi64(1);
      const __m512i bit_mask = _mm512_set1_epi64(63);
      const __m512d bias = _mm512_set1_pd(params.denom_bias_);
      const __m512d scale = _mm512_set1_pd(params.doc_len_scale_);
      const __m512d weight = _mm512_set1_pd(params.query_weight_);
      const __m512d boost = _mm512_set1_pd(params.score_multiplier_);
      __m512d maxima = _mm512_set1_pd(max_score);
      int64_t i = 0;
      for (; i + 8 <= count; i += 8) {
        const __m512i ids = load_doc_ids8(doc_ids + i);
        const __m512i offsets = _mm512_sub_epi64(ids, start_ids);
        const __m512i word_offsets = _mm512_srli_epi64(offsets, 6);
        const __m512i masks = _mm512_sllv_epi64(ones, _mm512_and_si512(offsets, bit_mask));
        const __mmask8 in_window = ONLY_EXISTING
            ? (_mm512_cmp_epu64_mask(offsets, capacities, _MM_CMPINT_LT)
               & _mm512_cmp_epu64_mask(ids, start_ids, _MM_CMPINT_NLT)) : 0xFF;
        const __m512i old_words = _mm512_mask_i64gather_epi64(
            _mm512_setzero_si512(), in_window, word_offsets, words, sizeof(uint64_t));
        const __mmask8 present = _mm512_cmpneq_epi64_mask(
            _mm512_and_si512(old_words, masks), _mm512_setzero_si512());
        const __mmask8 active = ONLY_EXISTING ? present : 0xFF;
        if (!ONLY_EXISTING || 0 != active) {
          const __m512d tf = relevance_to_double8(token_freqs + i);
          const __m512d dl = relevance_to_double8(doc_lengths + i);
          __m512d values = eval_bm25_avx512(tf, dl, bias, scale, weight, boost, active);
          // Unset accumulator slots are uninitialized. Mask the gather itself rather
          // than merely masking the addition after reading an unset slot.
          const __m512d previous = _mm512_mask_i64gather_pd(
              _mm512_setzero_pd(), present, offsets, scores, sizeof(double));
          values = _mm512_mask_add_pd(values, present, previous, values);
          _mm512_mask_i64scatter_pd(scores, active, offsets, values, sizeof(double));
          maxima = _mm512_mask_max_pd(maxima, active, maxima, values);
          if (!ONLY_EXISTING) {
            // Strictly increasing DocIDs keep lanes for each bitmap word contiguous.
            // Merge each group and write it back only from its last lane.
            __m512i combined = presence_prefix_or<1>(word_offsets, masks);
            combined = presence_prefix_or<2>(word_offsets, combined);
            combined = presence_prefix_or<4>(word_offsets, combined);
            const __m512i next_words = _mm512_alignr_epi64(word_offsets, word_offsets, 1);
            const __mmask8 last_in_word = _mm512_cmpneq_epi64_mask(word_offsets, next_words) | 0x80;
            // Scores are initialized; preserve existing bits when publishing presence.
            // Each selected lane writes to a distinct bitmap word.
            _mm512_mask_i64scatter_epi64(words, last_in_word, word_offsets,
                _mm512_or_si512(old_words, combined), sizeof(uint64_t));
          }
          matched += ONLY_EXISTING ? __builtin_popcount(static_cast<unsigned int>(active)) : 8;
        }
      }
      max_score = _mm512_reduce_max_pd(maxima);
      if (i < count) {
        matched += accumulate_bm25_scalar<ONLY_EXISTING, DocId, Relevance>(
            accumulator, doc_ids + i, token_freqs + i, doc_lengths + i, count - i, params);
      }
    }
    return matched;
  }
#endif

#if OB_ARM_USE_MULTITARGET_CODE
  template <bool ONLY_EXISTING>
  static OB_INLINE void accumulate_bm25_neon_lane(
      DocScoreAccumulator &accumulator, const uint64_t offset, const bool present, const double score,
      int64_t &matched)
  {
    double *scores = accumulator.scores_.get_data();
    uint64_t *words = accumulator.bitset_->reinterpret_data<uint64_t>();
    double &max_score = accumulator.max_score_;
    if (!ONLY_EXISTING || present) {
      if (present) {
        scores[offset] += score;
      } else {
        scores[offset] = score;
        words[offset >> 6] |= 1ULL << (offset & 63);
      }
      max_score = MAX(max_score, scores[offset]);
      ++matched;
    }
  }

  template <bool ONLY_EXISTING, typename DocId, typename Relevance>
#if !defined(__clang__)
  __attribute__((optimize("fp-contract=off")))
#endif
  static int64_t accumulate_bm25_neon(
      DocScoreAccumulator &accumulator,
      const void *doc_id_values,
      const void *token_freq_values,
      const void *doc_length_values,
      const int64_t count,
      const ObBM25PrecomputedParams &params)
  {
#if defined(__clang__)
#pragma clang fp contract(off)
#endif
    const uint64_t start_id = accumulator.start_id_;
    const int64_t capacity = accumulator.capacity_;
    uint64_t *words = accumulator.bitset_->reinterpret_data<uint64_t>();
    const DocId *doc_ids = static_cast<const DocId *>(doc_id_values);
    const Relevance *token_freqs = static_cast<const Relevance *>(token_freq_values);
    const Relevance *doc_lengths = static_cast<const Relevance *>(doc_length_values);
    const float64x2_t bias = vdupq_n_f64(params.denom_bias_);
    const float64x2_t scale = vdupq_n_f64(params.doc_len_scale_);
    const float64x2_t weight = vdupq_n_f64(params.query_weight_);
    const float64x2_t boost = vdupq_n_f64(params.score_multiplier_);
    int64_t matched = 0;
    int64_t i = 0;
    for (; i + 2 <= count; i += 2) {
      const uint64_t offset0 = doc_ids[i] - start_id;
      const uint64_t offset1 = doc_ids[i + 1] - start_id;
      // Check the window before accessing bitmap words; absent scores remain unread.
      const bool present0 = (!ONLY_EXISTING || (doc_ids[i] >= start_id
          && offset0 < static_cast<uint64_t>(capacity)))
          && 0 != (words[offset0 >> 6] & (1ULL << (offset0 & 63)));
      const bool present1 = (!ONLY_EXISTING || (doc_ids[i + 1] >= start_id
          && offset1 < static_cast<uint64_t>(capacity)))
          && 0 != (words[offset1 >> 6] & (1ULL << (offset1 & 63)));
      if (!ONLY_EXISTING || present0 || present1) {
        const float64x2_t tf = specific::neon::relevance_to_double2(token_freqs + i);
        const float64x2_t dl = specific::neon::relevance_to_double2(doc_lengths + i);
        const uint64x2_t active = ONLY_EXISTING
            ? vsetq_lane_u64(present1 ? UINT64_MAX : 0, vdupq_n_u64(present0 ? UINT64_MAX : 0), 1)
            : vdupq_n_u64(UINT64_MAX);
        const float64x2_t values = specific::neon::eval_bm25_neon(tf, dl, bias, scale, weight, boost, active);
        // Sequential OR updates preserve both lanes when they share a bitmap word.
        accumulate_bm25_neon_lane<ONLY_EXISTING>(
            accumulator, offset0, present0, vgetq_lane_f64(values, 0), matched);
        accumulate_bm25_neon_lane<ONLY_EXISTING>(
            accumulator, offset1, present1, vgetq_lane_f64(values, 1), matched);
      }
    }
    if (i < count) {
      matched += accumulate_bm25_scalar<ONLY_EXISTING, DocId, Relevance>(
          accumulator, doc_ids + i, token_freqs + i, doc_lengths + i, count - i, params);
    }
    return matched;
  }
#endif
};

template <bool ONLY_EXISTING>
BM25AccumulationKernels<ONLY_EXISTING> BM25AccumulationKernels<ONLY_EXISTING>::make(
    const common::ObTargetArch arch)
{
  BM25AccumulationKernels<ONLY_EXISTING> kernels;
#if OB_USE_MULTITARGET_CODE
  if (common::ObTargetArch::AVX512 == arch) {
    kernels = {DocScoreAccumulator::FusedExecutor::accumulate_bm25_avx512<ONLY_EXISTING, uint32_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_avx512<ONLY_EXISTING, uint32_t, uint64_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_avx512<ONLY_EXISTING, uint64_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_avx512<ONLY_EXISTING, uint64_t, uint64_t>, arch};
  } else
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::ObTargetArch::NEON == arch) {
    kernels = {DocScoreAccumulator::FusedExecutor::accumulate_bm25_neon<ONLY_EXISTING, uint32_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_neon<ONLY_EXISTING, uint32_t, uint64_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_neon<ONLY_EXISTING, uint64_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_neon<ONLY_EXISTING, uint64_t, uint64_t>, arch};
  } else
#endif
  {
    kernels = {DocScoreAccumulator::FusedExecutor::accumulate_bm25_scalar<ONLY_EXISTING, uint32_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_scalar<ONLY_EXISTING, uint32_t, uint64_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_scalar<ONLY_EXISTING, uint64_t, uint32_t>,
        DocScoreAccumulator::FusedExecutor::accumulate_bm25_scalar<ONLY_EXISTING, uint64_t, uint64_t>, arch};
  }
  return kernels;
}

template BM25AccumulationKernels<false> BM25AccumulationKernels<false>::make(const common::ObTargetArch arch);
template BM25AccumulationKernels<true> BM25AccumulationKernels<true>::make(const common::ObTargetArch arch);

} // namespace sql
} // namespace oceanbase
