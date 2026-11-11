/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_fast_scan.h"

#include <algorithm>
#include <cfloat>
#include <climits>
#include <cmath>

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif
#if OB_ARM_USE_MULTITARGET_CODE
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace quantization
{

static_assert(ObRaBitQFastScanConstants::BATCH_SIZE == 32,
              "FastScan SIMD kernels require 32-vector batches");
static_assert(ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE == 16,
              "FastScan SIMD kernels require 16-entry nibble LUTs");
static_assert(ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE == 32,
              "FastScan SIMD kernels require two adjacent nibble LUTs");

namespace specific
{
namespace normal
{

const float SQRT_TWO = 1.4142135623730950488F;

static void pack_sign_codes(const float *normalized_residual,
                            const int64_t dim,
                            uint8_t *packed_codes)
{
  int64_t index = 0;
  const int64_t packed_size = dim / 8 + (dim % 8 == 0 ? 0 : 1);
  for (int64_t byte_index = 0; byte_index < packed_size; ++byte_index) {
    uint8_t packed = 0;
    for (int64_t bit = 0; bit < 8 && index < dim; ++bit, ++index) {
      if (!(normalized_residual[index] < 0.0F)) {
        packed |= static_cast<uint8_t>(1U << bit);
      }
    }
    packed_codes[byte_index] = packed;
  }
}

static float dot_packed_codes(const float *query,
                              const uint8_t *packed_codes,
                              const int64_t dim,
                              const float inv_sqrt_dim)
{
  if (0 == dim) {
    return 0.0F;
  }
  const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
  const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
  float result = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    const bool bit =
        (packed_codes[i / 8] & static_cast<uint8_t>(1U << (i % 8))) != 0;
    result += (bit ? positive : negative) * query[i];
  }
  return result;
}

static void apply_signs(const int64_t dim,
                        const uint8_t *signs,
                        float *values)
{
  for (int64_t i = 0; i < dim; ++i) {
    if ((signs[i / 8] & (static_cast<uint8_t>(1U) << (i % 8))) != 0) {
      values[i] = -values[i];
    }
  }
}

static void fht(const int64_t length, float *values)
{
  int64_t half = 1;
  while (half < length) {
    const int64_t block = half * 2;
    for (int64_t base = 0; base < length; base += block) {
      for (int64_t i = 0; i < half; ++i) {
        const float lhs = values[base + i];
        const float rhs = values[base + half + i];
        values[base + i] = lhs + rhs;
        values[base + half + i] = lhs - rhs;
      }
    }
    if (half == length / 2) {
      break;
    }
    half <<= 1;
  }
}

static void scale(const int64_t length,
                  const float factor,
                  float *values)
{
  for (int64_t i = 0; i < length; ++i) {
    values[i] *= factor;
  }
}

static void kac(const int64_t length, float *values)
{
  const int64_t base = length % 2;
  const int64_t offset = base + length / 2;
  for (int64_t i = 0; i < length / 2; ++i) {
    const float lhs = values[i];
    const float rhs = values[i + offset];
    values[i] = lhs + rhs;
    values[i + offset] = lhs - rhs;
  }
  if (base != 0) {
    values[length / 2] *= SQRT_TWO;
  }
}

static void build_luts(const float *normalized_residual,
                                 const int64_t dim,
                                 const float positive,
                                 const float negative,
                                 float *luts,
                                 float &lut_min,
                                 float &lut_max)
{
  const int64_t code_bytes = (dim + CHAR_BIT - 1) / CHAR_BIT;
  lut_min = FLT_MAX;
  lut_max = -FLT_MAX;
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    float *low_lut = luts
        + byte_index * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    float *high_lut = low_lut + ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE;
    for (int64_t mask = 0; mask < ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE; ++mask) {
      float low_sum = 0.0F;
      float high_sum = 0.0F;
      for (int64_t bit = 0; bit < ObRaBitQFastScanConstants::NIBBLE_BITS; ++bit) {
        const float sign = 0 != (mask & (1 << bit)) ? positive : negative;
        const int64_t low_dim = byte_index * CHAR_BIT + bit;
        const int64_t high_dim = low_dim + ObRaBitQFastScanConstants::NIBBLE_BITS;
        if (low_dim < dim) {
          low_sum += normalized_residual[low_dim] * sign;
        }
        if (high_dim < dim) {
          high_sum += normalized_residual[high_dim] * sign;
        }
      }
      low_lut[mask] = low_sum;
      high_lut[mask] = high_sum;
      lut_min = std::min(lut_min, std::min(low_sum, high_sum));
      lut_max = std::max(lut_max, std::max(low_sum, high_sum));
    }
  }
}

static void quantize_luts(const float *float_luts,
                                    const int64_t lut_entry_count,
                                    const float lut_min,
                                    const float lut_scale,
                                    uint8_t *low_byte_luts,
                                    uint8_t *high_byte_luts)
{
  for (int64_t i = 0; i < lut_entry_count; ++i) {
    const int64_t rounded = lut_scale > 0.0F
        ? static_cast<int64_t>(std::lround((float_luts[i] - lut_min) / lut_scale))
        : 0;
    const uint16_t quantized = static_cast<uint16_t>(
        std::max<int64_t>(0, std::min<int64_t>(UINT16_MAX, rounded)));
    low_byte_luts[i] = static_cast<uint8_t>(quantized & UINT8_MAX);
    high_byte_luts[i] = static_cast<uint8_t>(quantized >> CHAR_BIT);
  }
}

static void accumulate_luts(const uint8_t *packed_codes,
                            const uint8_t *luts,
                            const int64_t code_bytes,
                            uint32_t *results)
{
  MEMSET(results, 0, ObRaBitQFastScanConstants::BATCH_SIZE * sizeof(uint32_t));
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const uint8_t *column = packed_codes
        + byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    const uint8_t *first_nibble_lut = luts
        + byte_index * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    const uint8_t *second_nibble_lut =
        first_nibble_lut + ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE;
    for (int64_t packed_slot = 0;
         packed_slot < ObRaBitQFastScanConstants::BATCH_SIZE / 2;
         ++packed_slot) {
      const int64_t first_row = packed_slot;
      const int64_t second_row = first_row + ObRaBitQFastScanConstants::BATCH_SIZE / 2;
      // The two bytes cover the first / second four dimensions. Within each
      // byte, the low nibble selects first_row and the high nibble second_row.
      const uint8_t first_nibble_codes = column[packed_slot];
      const uint8_t second_nibble_codes =
          column[packed_slot + ObRaBitQFastScanConstants::BATCH_SIZE / 2];
      results[first_row] +=
          first_nibble_lut[first_nibble_codes & ObRaBitQFastScanConstants::NIBBLE_MASK]
          + second_nibble_lut[second_nibble_codes & ObRaBitQFastScanConstants::NIBBLE_MASK];
      results[second_row] +=
          first_nibble_lut[first_nibble_codes >> ObRaBitQFastScanConstants::NIBBLE_BITS]
          + second_nibble_lut[second_nibble_codes >> ObRaBitQFastScanConstants::NIBBLE_BITS];
    }
  }
}

static void accumulate_lut_pairs(const uint8_t *packed_codes,
                                 const uint8_t *low_byte_luts,
                                 const uint8_t *high_byte_luts,
                                 const int64_t code_bytes,
                                 uint32_t *low_byte_sums,
                                 uint32_t *high_byte_sums)
{
  accumulate_luts(
      packed_codes, low_byte_luts, code_bytes, low_byte_sums);
  accumulate_luts(
      packed_codes, high_byte_luts, code_bytes, high_byte_sums);
}

static int64_t recover_metrics(
    const uint32_t *low_byte_sums,
    const uint32_t *high_byte_sums,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *metric_values)
{
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float lut_offset = computer.get_lut_offset();
  const float lut_scale = computer.get_lut_scale();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  int64_t invalid_index = -1;
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  const bool is_query_zero = is_ip_or_cosine
      && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON;
  for (int64_t i = 0; i < vector_count && invalid_index < 0; ++i) {
    if (residual_norms[i] < 0.0F || raw_norms[i] < 0.0F) {
      invalid_index = i;
    } else {
      const uint32_t quantized_sum = low_byte_sums[i] + (high_byte_sums[i] << CHAR_BIT);
      const float normalized_dot =
          lut_offset + lut_scale * static_cast<float>(quantized_sum);
      const float correction =
          std::abs(corrections[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON
          ? (corrections[i] > 0.0F ? 1.0F : -1.0F) : corrections[i];
      float metric_value = 1.0F;
      if (!is_query_zero
          && !(is_ip_or_cosine
               && std::abs(raw_norms[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON)) {
        const float corrected_dot = normalized_dot / correction;
        const float estimated_l2_squared = residual_norms[i] * residual_norms[i]
            + query_residual_norm * query_residual_norm
            - 2.0F * residual_norms[i] * query_residual_norm
                * corrected_dot;
        if (share::VIDA_IP == metric) {
          metric_value = 1.0F - (query_raw_norm * query_raw_norm
              + raw_norms[i] * raw_norms[i] - estimated_l2_squared) * 0.5F;
        } else if (share::VIDA_COS == metric) {
          metric_value = 1.0F - (query_raw_norm * query_raw_norm
              + raw_norms[i] * raw_norms[i] - estimated_l2_squared)
              * 0.5F / (query_raw_norm * raw_norms[i]);
        } else {
          metric_value = estimated_l2_squared;
        }
      }
      metric_values[i] = metric_value;
    }
  }
  return invalid_index;
}

static void compute_lower_bounds(
    const float *metric_values,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *lower_bounds)
{
  const int64_t dim = computer.get_dim();
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  const float dim_error_scale = computer.get_dim_error_scale();
  const float lut_dot_error = computer.get_lut_dot_error();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  for (int64_t i = 0; i < vector_count; ++i) {
    if (is_ip_or_cosine
        && (std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON
            || std::abs(raw_norms[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON)) {
      lower_bounds[i] = metric_values[i];
    } else if (dim <= 1
               || std::abs(corrections[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON) {
      lower_bounds[i] = -FLT_MAX;
    } else {
      const float correction_squared = corrections[i] * corrections[i];
      const float normalized_dot_error = dim_error_scale
          * std::sqrt(std::max(0.0F, 1.0F - correction_squared) / correction_squared)
          + lut_dot_error / std::abs(corrections[i]);
      float error = query_residual_norm * residual_norms[i] * normalized_dot_error;
      if (share::VIDA_L2 == metric) {
        error *= 2.0F;
      } else if (share::VIDA_COS == metric) {
        error /= query_raw_norm * raw_norms[i];
      }
      lower_bounds[i] = metric_values[i] - error;
    }
  }
}

const ObRaBitQSimdOps RABITQ_OPS = {pack_sign_codes, dot_packed_codes, nullptr};
const ObFhtKacSimdOps FHT_KAC_OPS = {apply_signs, fht, scale, kac};
const ObRaBitQFastScanSimdOps RABITQ_FAST_SCAN_OPS = {
    build_luts, quantize_luts,
    accumulate_lut_pairs, recover_metrics,
    compute_lower_bounds};

} // namespace normal
} // namespace specific

#if OB_USE_MULTITARGET_CODE

OB_DECLARE_AVX2_SPECIFIC_CODE(

static float dot_packed_codes(const float *query,
                              const uint8_t *packed_codes,
                              const int64_t dim,
                              const float inv_sqrt_dim)
{
  if (0 == dim) {
    return 0.0F;
  }
  const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
  const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
  const __m256 positive_values = _mm256_set1_ps(positive);
  const __m256 negative_values = _mm256_set1_ps(negative);
  const __m256i lane_bit_masks =
      _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08,
                        0x10, 0x20, 0x40, 0x80);
  const __m256i zero = _mm256_setzero_si256();
  __m256 accumulator = _mm256_setzero_ps();
  int64_t i = 0;
  for (; dim - i >= 8; i += 8) {
    const __m256 query_values = _mm256_loadu_ps(query + i);
    const __m256i packed =
        _mm256_set1_epi32(static_cast<int32_t>(packed_codes[i / 8]));
    const __m256i bit_is_set =
        _mm256_cmpgt_epi32(_mm256_and_si256(packed, lane_bit_masks), zero);
    const __m256 signed_values =
        _mm256_blendv_ps(negative_values,
                         positive_values,
                         _mm256_castsi256_ps(bit_is_set));
    accumulator =
        _mm256_fmadd_ps(query_values, signed_values, accumulator);
  }

  const __m128 lane_sums =
      _mm_add_ps(_mm256_castps256_ps128(accumulator),
                 _mm256_extractf128_ps(accumulator, 1));
  const __m128 pair_sums = _mm_hadd_ps(lane_sums, lane_sums);
  const float result = _mm_cvtss_f32(_mm_hadd_ps(pair_sums, pair_sums));
  return result + specific::normal::dot_packed_codes(
                      query + i, packed_codes + i / 8,
                      dim - i, inv_sqrt_dim);
}

static const ObRaBitQSimdOps RABITQ_OPS = {
    specific::normal::pack_sign_codes, dot_packed_codes, nullptr};

static void apply_signs(const int64_t dim,
                        const uint8_t *signs,
                        float *values)
{
  const __m256i lane_bit_masks =
      _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08,
                        0x10, 0x20, 0x40, 0x80);
  const __m256i sign_bit =
      _mm256_set1_epi32(static_cast<int32_t>(0x80000000U));
  const __m256i zero = _mm256_setzero_si256();
  int64_t i = 0;
  for (; dim - i >= 8; i += 8) {
    const __m256i packed =
        _mm256_set1_epi32(static_cast<int32_t>(signs[i / 8]));
    const __m256i bit_is_set =
        _mm256_cmpgt_epi32(_mm256_and_si256(packed, lane_bit_masks), zero);
    const __m256i sign_bits = _mm256_and_si256(bit_is_set, sign_bit);
    const __m256 value = _mm256_loadu_ps(values + i);
    const __m256 result = _mm256_castsi256_ps(
        _mm256_xor_si256(_mm256_castps_si256(value), sign_bits));
    _mm256_storeu_ps(values + i, result);
  }
  specific::normal::apply_signs(dim - i, signs + i / 8, values + i);
}

static void fht(const int64_t length, float *values)
{
  int64_t half = 1;
  while (half < length) {
    const int64_t block = half * 2;
    for (int64_t base = 0; base < length; base += block) {
      int64_t i = 0;
      if (half >= 8) {
        for (; half - i >= 8; i += 8) {
          const __m256 lhs = _mm256_loadu_ps(values + base + i);
          const __m256 rhs = _mm256_loadu_ps(values + base + half + i);
          _mm256_storeu_ps(values + base + i, _mm256_add_ps(lhs, rhs));
          _mm256_storeu_ps(values + base + half + i,
                           _mm256_sub_ps(lhs, rhs));
        }
      }
      for (; i < half; ++i) {
        const float lhs = values[base + i];
        const float rhs = values[base + half + i];
        values[base + i] = lhs + rhs;
        values[base + half + i] = lhs - rhs;
      }
    }
    if (half == length / 2) {
      break;
    }
    half <<= 1;
  }
}

static void scale(const int64_t length,
                  const float factor,
                  float *values)
{
  const __m256 factors = _mm256_set1_ps(factor);
  int64_t i = 0;
  for (; length - i >= 8; i += 8) {
    const __m256 value = _mm256_loadu_ps(values + i);
    _mm256_storeu_ps(values + i, _mm256_mul_ps(value, factors));
  }
  specific::normal::scale(length - i, factor, values + i);
}

static void kac_pairs(const int64_t pair_count,
                      const int64_t offset,
                      float *values)
{
  int64_t i = 0;
  for (; pair_count - i >= 8; i += 8) {
    const __m256 lhs = _mm256_loadu_ps(values + i);
    const __m256 rhs = _mm256_loadu_ps(values + offset + i);
    _mm256_storeu_ps(values + i, _mm256_add_ps(lhs, rhs));
    _mm256_storeu_ps(values + offset + i, _mm256_sub_ps(lhs, rhs));
  }
  for (; i < pair_count; ++i) {
    const float lhs = values[i];
    const float rhs = values[offset + i];
    values[i] = lhs + rhs;
    values[offset + i] = lhs - rhs;
  }
}

static void kac(const int64_t length, float *values)
{
  const int64_t base = length % 2;
  const int64_t pair_count = length / 2;
  kac_pairs(pair_count, base + pair_count, values);
  if (base != 0) {
    values[pair_count] *= specific::normal::SQRT_TWO;
  }
}

static const ObFhtKacSimdOps FHT_KAC_OPS = {
    apply_signs, fht, scale, kac};

static void build_fast_scan_nibble_lut(const float *residual,
                                       const float positive,
                                       const float negative,
                                       float *lut,
                                       __m256 &min_values,
                                       __m256 &max_values)
{
  const __m256 positive_values = _mm256_set1_ps(positive);
  const __m256 negative_values = _mm256_set1_ps(negative);
  const __m256 sign0 = _mm256_blend_ps(negative_values, positive_values, 0xAA);
  const __m256 sign1 = _mm256_blend_ps(negative_values, positive_values, 0xCC);
  const __m256 sign2 = _mm256_blend_ps(negative_values, positive_values, 0xF0);
  __m256 sums = _mm256_mul_ps(_mm256_set1_ps(residual[0]), sign0);
  sums = _mm256_add_ps(
      sums, _mm256_mul_ps(_mm256_set1_ps(residual[1]), sign1));
  sums = _mm256_add_ps(
      sums, _mm256_mul_ps(_mm256_set1_ps(residual[2]), sign2));
  const __m256 low_sums = _mm256_add_ps(
      sums, _mm256_mul_ps(_mm256_set1_ps(residual[3]), negative_values));
  const __m256 high_sums = _mm256_add_ps(
      sums, _mm256_mul_ps(_mm256_set1_ps(residual[3]), positive_values));
  _mm256_storeu_ps(lut, low_sums);
  _mm256_storeu_ps(lut + 8, high_sums);
  min_values = _mm256_min_ps(min_values, _mm256_min_ps(low_sums, high_sums));
  max_values = _mm256_max_ps(max_values, _mm256_max_ps(low_sums, high_sums));
}

static void build_luts(const float *normalized_residual,
                                 const int64_t dim,
                                 const float positive,
                                 const float negative,
                                 float *luts,
                                 float &lut_min,
                                 float &lut_max)
{
  const int64_t code_bytes = (dim + CHAR_BIT - 1) / CHAR_BIT;
  __m256 min_values = _mm256_set1_ps(FLT_MAX);
  __m256 max_values = _mm256_set1_ps(-FLT_MAX);
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const int64_t offset = byte_index * CHAR_BIT;
    float tail[CHAR_BIT] = {};
    const float *residual = normalized_residual + offset;
    if (dim - offset < CHAR_BIT) {
      MEMCPY(tail, residual, (dim - offset) * sizeof(float));
      residual = tail;
    }
    float *low_lut = luts
        + byte_index * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    build_fast_scan_nibble_lut(
        residual, positive, negative, low_lut, min_values, max_values);
    build_fast_scan_nibble_lut(
        residual + ObRaBitQFastScanConstants::NIBBLE_BITS, positive, negative,
        low_lut + ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE, min_values, max_values);
  }
  float min_lanes[8];
  float max_lanes[8];
  _mm256_storeu_ps(min_lanes, min_values);
  _mm256_storeu_ps(max_lanes, max_values);
  lut_min = FLT_MAX;
  lut_max = -FLT_MAX;
  for (int64_t lane = 0; lane < 8; ++lane) {
    lut_min = std::min(lut_min, min_lanes[lane]);
    lut_max = std::max(lut_max, max_lanes[lane]);
  }
}

static void quantize_luts(const float *float_luts,
                                    const int64_t lut_entry_count,
                                    const float lut_min,
                                    const float lut_scale,
                                    uint8_t *low_byte_luts,
                                    uint8_t *high_byte_luts)
{
  if (lut_scale <= 0.0F) {
    MEMSET(low_byte_luts, 0, lut_entry_count);
    MEMSET(high_byte_luts, 0, lut_entry_count);
  } else {
    const __m256 min_values = _mm256_set1_ps(lut_min);
    const __m256 scale_values = _mm256_set1_ps(lut_scale);
    const __m256 half = _mm256_set1_ps(0.5F);
    const __m256i zero = _mm256_setzero_si256();
    const __m256i max_value = _mm256_set1_epi32(UINT16_MAX);
    int64_t i = 0;
    for (; lut_entry_count - i >= 8; i += 8) {
      const __m256 scaled = _mm256_div_ps(
          _mm256_sub_ps(_mm256_loadu_ps(float_luts + i), min_values),
          scale_values);
      const __m256i rounded = _mm256_min_epi32(
          max_value,
          _mm256_max_epi32(zero, _mm256_cvttps_epi32(_mm256_add_ps(scaled, half))));
      uint32_t quantized[8];
      _mm256_storeu_si256(reinterpret_cast<__m256i *>(quantized), rounded);
      for (int64_t lane = 0; lane < 8; ++lane) {
        low_byte_luts[i + lane] =
            static_cast<uint8_t>(quantized[lane] & UINT8_MAX);
        high_byte_luts[i + lane] =
            static_cast<uint8_t>(quantized[lane] >> CHAR_BIT);
      }
    }
    specific::normal::quantize_luts(
        float_luts + i, lut_entry_count - i, lut_min, lut_scale,
        low_byte_luts + i, high_byte_luts + i);
  }
}

// Each 128-bit half is one four-dimension LUT group for the same 16 rows.
// Widen each eight-row half before summing the two dimension groups.
static void add_fast_scan_values(const __m256i values,
                                 __m256i &rows_0_7,
                                 __m256i &rows_8_15)
{
  const __m128i first_group = _mm256_castsi256_si128(values);
  const __m128i second_group = _mm256_extracti128_si256(values, 1);
  rows_0_7 = _mm256_add_epi32(rows_0_7, _mm256_add_epi32(
      _mm256_cvtepu8_epi32(first_group),
      _mm256_cvtepu8_epi32(second_group)));
  rows_8_15 = _mm256_add_epi32(rows_8_15, _mm256_add_epi32(
      _mm256_cvtepu8_epi32(_mm_srli_si128(first_group, 8)),
      _mm256_cvtepu8_epi32(_mm_srli_si128(second_group, 8))));
}

static void accumulate_lut_pairs(const uint8_t *packed_codes,
                                 const uint8_t *low_byte_luts,
                                 const uint8_t *high_byte_luts,
                                 const int64_t code_bytes,
                                 uint32_t *low_byte_sums,
                                 uint32_t *high_byte_sums)
{
  const __m256i nibble_mask = _mm256_set1_epi8(ObRaBitQFastScanConstants::NIBBLE_MASK);
  // Four consecutive eight-row groups per LUT byte plane, all in uint32.
  __m256i low_byte_sum_groups[4] = {};
  __m256i high_byte_sum_groups[4] = {};
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    const __m256i codes = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(packed_codes + offset));
    const __m256i rows_0_15 = _mm256_and_si256(codes, nibble_mask);
    const __m256i rows_16_31 = _mm256_and_si256(
        _mm256_srli_epi16(codes, ObRaBitQFastScanConstants::NIBBLE_BITS), nibble_mask);
    const __m256i low_lut = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(low_byte_luts + offset));
    const __m256i high_lut = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(high_byte_luts + offset));
    add_fast_scan_values(_mm256_shuffle_epi8(low_lut, rows_0_15), low_byte_sum_groups[0], low_byte_sum_groups[1]);
    add_fast_scan_values(_mm256_shuffle_epi8(low_lut, rows_16_31), low_byte_sum_groups[2], low_byte_sum_groups[3]);
    add_fast_scan_values(_mm256_shuffle_epi8(high_lut, rows_0_15), high_byte_sum_groups[0], high_byte_sum_groups[1]);
    add_fast_scan_values(_mm256_shuffle_epi8(high_lut, rows_16_31), high_byte_sum_groups[2], high_byte_sum_groups[3]);
  }
  for (int64_t group = 0; group < 4; ++group) {
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(low_byte_sums + group * 8), low_byte_sum_groups[group]);
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(high_byte_sums + group * 8), high_byte_sum_groups[group]);
  }
}

static int64_t recover_metrics(
    const uint32_t *low_byte_sums,
    const uint32_t *high_byte_sums,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *metric_values)
{
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float lut_offset = computer.get_lut_offset();
  const float lut_scale = computer.get_lut_scale();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  const bool is_query_zero = is_ip_or_cosine
      && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON;
  const __m256 zero = _mm256_setzero_ps();
  const __m256 one = _mm256_set1_ps(1.0F);
  const __m256 negative_one = _mm256_set1_ps(-1.0F);
  const __m256 two = _mm256_set1_ps(2.0F);
  const __m256 half = _mm256_set1_ps(0.5F);
  const __m256 epsilon = _mm256_set1_ps(ObRaBitQFastScanConstants::ZERO_EPSILON);
  const __m256 sign_mask = _mm256_castsi256_ps(_mm256_set1_epi32(INT32_MIN));
  const __m256 offset = _mm256_set1_ps(lut_offset);
  const __m256 scale = _mm256_set1_ps(lut_scale);
  const __m256 query_residual = _mm256_set1_ps(query_residual_norm);
  const __m256 query_residual_squared = _mm256_mul_ps(query_residual, query_residual);
  const __m256 query_raw = _mm256_set1_ps(query_raw_norm);
  const __m256 query_raw_squared = _mm256_mul_ps(query_raw, query_raw);
  int64_t i = 0;
  for (; vector_count - i >= 8; i += 8) {
    const __m256 residual = _mm256_loadu_ps(residual_norms + i);
    const __m256 raw = _mm256_loadu_ps(raw_norms + i);
    const int invalid_mask = _mm256_movemask_ps(_mm256_or_ps(
        _mm256_cmp_ps(residual, zero, _CMP_LT_OQ),
        _mm256_cmp_ps(raw, zero, _CMP_LT_OQ)));
    if (OB_UNLIKELY(0 != invalid_mask)) {
      const int64_t invalid_lane = __builtin_ctz(static_cast<unsigned int>(invalid_mask));
      specific::normal::recover_metrics(
          low_byte_sums + i, high_byte_sums + i, residual_norms + i, corrections + i,
          raw_norms + i, invalid_lane, computer, metric_values + i);
      return i + invalid_lane;
    }

    __m256 metric_value = one;
    if (!is_query_zero) {
      const __m256i quantized_sum = _mm256_add_epi32(
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(low_byte_sums + i)),
          _mm256_slli_epi32(_mm256_loadu_si256(
              reinterpret_cast<const __m256i *>(high_byte_sums + i)), CHAR_BIT));
      const __m256 normalized_dot = _mm256_add_ps(
          offset, _mm256_mul_ps(scale, _mm256_cvtepi32_ps(quantized_sum)));
      const __m256 correction = _mm256_loadu_ps(corrections + i);
      const __m256 abs_correction = _mm256_andnot_ps(sign_mask, correction);
      const __m256 small_correction =
          _mm256_cmp_ps(abs_correction, epsilon, _CMP_LT_OQ);
      const __m256 correction_sign = _mm256_blendv_ps(
          negative_one, one, _mm256_cmp_ps(correction, zero, _CMP_GT_OQ));
      const __m256 safe_correction =
          _mm256_blendv_ps(correction, correction_sign, small_correction);
      const __m256 corrected_dot = _mm256_div_ps(normalized_dot, safe_correction);
      const __m256 estimated_l2_squared = _mm256_sub_ps(
          _mm256_add_ps(_mm256_mul_ps(residual, residual), query_residual_squared),
          _mm256_mul_ps(_mm256_mul_ps(
              two, _mm256_mul_ps(residual, query_residual)), corrected_dot));
      if (!is_ip_or_cosine) {
        metric_value = estimated_l2_squared;
      } else {
        const __m256 raw_dot_twice = _mm256_sub_ps(
            _mm256_add_ps(query_raw_squared, _mm256_mul_ps(raw, raw)),
            estimated_l2_squared);
        const __m256 abs_raw = _mm256_andnot_ps(sign_mask, raw);
        const __m256 small_raw = _mm256_cmp_ps(abs_raw, epsilon, _CMP_LT_OQ);
        if (share::VIDA_IP == metric) {
          metric_value = _mm256_sub_ps(one, _mm256_mul_ps(half, raw_dot_twice));
        } else {
          const __m256 safe_raw = _mm256_blendv_ps(raw, one, small_raw);
          metric_value = _mm256_sub_ps(one, _mm256_div_ps(
              _mm256_mul_ps(half, raw_dot_twice),
              _mm256_mul_ps(query_raw, safe_raw)));
        }
        metric_value = _mm256_blendv_ps(metric_value, one, small_raw);
      }
    }
    _mm256_storeu_ps(metric_values + i, metric_value);
  }
  int64_t invalid_index = -1;
  if (i < vector_count) {
    const int64_t tail_invalid = specific::normal::recover_metrics(
        low_byte_sums + i, high_byte_sums + i, residual_norms + i, corrections + i,
        raw_norms + i, vector_count - i, computer, metric_values + i);
    invalid_index = tail_invalid < 0 ? -1 : i + tail_invalid;
  }
  return invalid_index;
}

static void compute_lower_bounds(
    const float *metric_values,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *lower_bounds)
{
  const int64_t dim = computer.get_dim();
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  const float dim_error_scale = computer.get_dim_error_scale();
  const float lut_dot_error = computer.get_lut_dot_error();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  int64_t i = 0;
  if (dim > 1
      && !(is_ip_or_cosine
           && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON)) {
    const __m256 zero = _mm256_setzero_ps();
    const __m256 one = _mm256_set1_ps(1.0F);
    const __m256 two = _mm256_set1_ps(2.0F);
    const __m256 epsilon = _mm256_set1_ps(ObRaBitQFastScanConstants::ZERO_EPSILON);
    const __m256 negative_max = _mm256_set1_ps(-FLT_MAX);
    const __m256 sign_mask = _mm256_castsi256_ps(_mm256_set1_epi32(INT32_MIN));
    const __m256 query_residual = _mm256_set1_ps(query_residual_norm);
    const __m256 query_raw = _mm256_set1_ps(query_raw_norm);
    const __m256 error_scale = _mm256_set1_ps(dim_error_scale);
    const __m256 lut_error = _mm256_set1_ps(lut_dot_error);
    for (; vector_count - i >= 8; i += 8) {
      const __m256 metrics = _mm256_loadu_ps(metric_values + i);
      const __m256 residuals = _mm256_loadu_ps(residual_norms + i);
      const __m256 correction = _mm256_loadu_ps(corrections + i);
      const __m256 raw = _mm256_loadu_ps(raw_norms + i);
      const __m256 abs_correction = _mm256_andnot_ps(sign_mask, correction);
      const __m256 small_correction =
          _mm256_cmp_ps(abs_correction, epsilon, _CMP_LT_OQ);
      const __m256 safe_abs_correction =
          _mm256_blendv_ps(abs_correction, one, small_correction);
      const __m256 correction_squared =
          _mm256_mul_ps(safe_abs_correction, safe_abs_correction);
      const __m256 ratio = _mm256_div_ps(
          _mm256_max_ps(zero, _mm256_sub_ps(one, correction_squared)),
          correction_squared);
      const __m256 normalized_dot_error = _mm256_add_ps(
          _mm256_mul_ps(error_scale, _mm256_sqrt_ps(ratio)),
          _mm256_div_ps(lut_error, safe_abs_correction));
      __m256 error = _mm256_mul_ps(
          _mm256_mul_ps(query_residual, residuals), normalized_dot_error);
      if (share::VIDA_L2 == metric) {
        error = _mm256_mul_ps(error, two);
      } else if (share::VIDA_COS == metric) {
        const __m256 abs_raw = _mm256_andnot_ps(sign_mask, raw);
        const __m256 small_raw = _mm256_cmp_ps(abs_raw, epsilon, _CMP_LT_OQ);
        const __m256 safe_raw = _mm256_blendv_ps(raw, one, small_raw);
        error = _mm256_div_ps(error, _mm256_mul_ps(query_raw, safe_raw));
      }
      __m256 lower = _mm256_sub_ps(metrics, error);
      lower = _mm256_blendv_ps(lower, negative_max, small_correction);
      if (is_ip_or_cosine) {
        const __m256 abs_raw = _mm256_andnot_ps(sign_mask, raw);
        const __m256 small_raw = _mm256_cmp_ps(abs_raw, epsilon, _CMP_LT_OQ);
        lower = _mm256_blendv_ps(lower, metrics, small_raw);
      }
      _mm256_storeu_ps(lower_bounds + i, lower);
    }
  }
  if (i < vector_count) {
    specific::normal::compute_lower_bounds(
        metric_values + i, residual_norms + i, corrections + i, raw_norms + i,
        vector_count - i, computer, lower_bounds + i);
  }
}

static const ObRaBitQFastScanSimdOps RABITQ_FAST_SCAN_OPS = {
    build_luts, quantize_luts,
    accumulate_lut_pairs, recover_metrics,
    compute_lower_bounds};

)

OB_DECLARE_AVX512_SPECIFIC_CODE(

static float dot_packed_codes(const float *query,
                              const uint8_t *packed_codes,
                              const int64_t dim,
                              const float inv_sqrt_dim)
{
  if (0 == dim) {
    return 0.0F;
  }
  const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
  const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
  const __m512 positive_values = _mm512_set1_ps(positive);
  const __m512 negative_values = _mm512_set1_ps(negative);
  __m512 accumulator = _mm512_setzero_ps();
  int64_t i = 0;
  for (; dim - i >= 16; i += 16) {
    const uint16_t bits =
        static_cast<uint16_t>(packed_codes[i / 8]) |
        static_cast<uint16_t>(static_cast<uint16_t>(packed_codes[i / 8 + 1])
                              << 8);
    const __m512 signed_values =
        _mm512_mask_blend_ps(static_cast<__mmask16>(bits),
                             negative_values, positive_values);
    const __m512 query_values = _mm512_loadu_ps(query + i);
    accumulator =
        _mm512_fmadd_ps(query_values, signed_values, accumulator);
  }

  const float result = _mm512_reduce_add_ps(accumulator);
  return result + specific::avx2::dot_packed_codes(
                      query + i, packed_codes + i / 8,
                      dim - i, inv_sqrt_dim);
}

template <int ROWS>
static void dot_packed_rows(const float *query, const uint8_t *const *codes,
                            const int64_t dim, const float inv_sqrt_dim, float *results)
{
  const __m512 positive = _mm512_set1_ps(inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F);
  const __m512 negative = _mm512_set1_ps(inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F);
  __m512 accumulators[ROWS];
  for (int row = 0; row < ROWS; ++row) { accumulators[row] = _mm512_setzero_ps(); }
  int64_t i = 0;
  for (; dim - i >= 16; i += 16) {
    const __m512 values = _mm512_loadu_ps(query + i);
    // Independent rows hide FMA latency and share the query-vector load.
    // Dimensions within each row still accumulate in their original order.
    _Pragma("unroll")
    for (int row = 0; row < ROWS; ++row) {
      const uint16_t bits = static_cast<uint16_t>(codes[row][i / 8])
          | static_cast<uint16_t>(static_cast<uint16_t>(codes[row][i / 8 + 1]) << 8);
      accumulators[row] = _mm512_fmadd_ps(values,
          _mm512_mask_blend_ps(static_cast<__mmask16>(bits), negative, positive),
          accumulators[row]);
    }
  }
  for (int row = 0; row < ROWS; ++row) {
    results[row] = _mm512_reduce_add_ps(accumulators[row])
        + specific::avx2::dot_packed_codes(query + i, codes[row] + i / 8,
            dim - i, inv_sqrt_dim);
  }
}

static void dot_packed_codes_batch(const float *query, const uint8_t *const *codes,
                                   const int64_t count, const int64_t dim,
                                   const float inv_sqrt_dim, float *results)
{
  int64_t row = 0;
  for (; count - row >= 4; row += 4) {
    dot_packed_rows<4>(query, codes + row, dim, inv_sqrt_dim, results + row);
  }
  switch (count - row) {
  case 3: dot_packed_rows<3>(query, codes + row, dim, inv_sqrt_dim, results + row); break;
  case 2: dot_packed_rows<2>(query, codes + row, dim, inv_sqrt_dim, results + row); break;
  case 1: results[row] = dot_packed_codes(query, codes[row], dim, inv_sqrt_dim); break;
  default: break;
  }
}

static const ObRaBitQSimdOps RABITQ_OPS = {
    specific::normal::pack_sign_codes, dot_packed_codes, dot_packed_codes_batch};

static void apply_signs(const int64_t dim,
                        const uint8_t *signs,
                        float *values)
{
  const __m512i sign_bit =
      _mm512_set1_epi32(static_cast<int32_t>(0x80000000U));
  int64_t i = 0;
  for (; dim - i >= 16; i += 16) {
    const uint16_t bits =
        static_cast<uint16_t>(signs[i / 8]) |
        static_cast<uint16_t>(static_cast<uint16_t>(signs[i / 8 + 1]) << 8);
    const __m512i sign_bits =
        _mm512_maskz_mov_epi32(static_cast<__mmask16>(bits), sign_bit);
    const __m512 value = _mm512_loadu_ps(values + i);
    const __m512 result = _mm512_castsi512_ps(
        _mm512_xor_si512(_mm512_castps_si512(value), sign_bits));
    _mm512_storeu_ps(values + i, result);
  }
  specific::avx2::apply_signs(dim - i, signs + i / 8, values + i);
}

static void fht(const int64_t length, float *values)
{
  int64_t half = 1;
  while (half < length) {
    const int64_t block = half * 2;
    for (int64_t base = 0; base < length; base += block) {
      int64_t i = 0;
      if (half >= 16) {
        for (; half - i >= 16; i += 16) {
          const __m512 lhs = _mm512_loadu_ps(values + base + i);
          const __m512 rhs = _mm512_loadu_ps(values + base + half + i);
          _mm512_storeu_ps(values + base + i, _mm512_add_ps(lhs, rhs));
          _mm512_storeu_ps(values + base + half + i,
                           _mm512_sub_ps(lhs, rhs));
        }
      }
      for (; i < half; ++i) {
        const float lhs = values[base + i];
        const float rhs = values[base + half + i];
        values[base + i] = lhs + rhs;
        values[base + half + i] = lhs - rhs;
      }
    }
    if (half == length / 2) {
      break;
    }
    half <<= 1;
  }
}

static void scale(const int64_t length,
                  const float factor,
                  float *values)
{
  const __m512 factors = _mm512_set1_ps(factor);
  int64_t i = 0;
  for (; length - i >= 16; i += 16) {
    const __m512 value = _mm512_loadu_ps(values + i);
    _mm512_storeu_ps(values + i, _mm512_mul_ps(value, factors));
  }
  specific::avx2::scale(length - i, factor, values + i);
}

static void kac(const int64_t length, float *values)
{
  const int64_t base = length % 2;
  const int64_t pair_count = length / 2;
  const int64_t offset = base + pair_count;
  int64_t i = 0;
  for (; pair_count - i >= 16; i += 16) {
    const __m512 lhs = _mm512_loadu_ps(values + i);
    const __m512 rhs = _mm512_loadu_ps(values + offset + i);
    _mm512_storeu_ps(values + i, _mm512_add_ps(lhs, rhs));
    _mm512_storeu_ps(values + offset + i, _mm512_sub_ps(lhs, rhs));
  }
  specific::avx2::kac_pairs(pair_count - i, offset, values + i);
  if (base != 0) {
    values[pair_count] *= specific::normal::SQRT_TWO;
  }
}

static const ObFhtKacSimdOps FHT_KAC_OPS = {
    apply_signs, fht, scale, kac};

static void build_fast_scan_nibble_lut(const float *residual,
                                       const float positive,
                                       const float negative,
                                       float *lut,
                                       __m512 &min_values,
                                       __m512 &max_values)
{
  const __m512 positive_values = _mm512_set1_ps(positive);
  const __m512 negative_values = _mm512_set1_ps(negative);
  const __m512 sign0 = _mm512_mask_blend_ps(0xAAAA, negative_values, positive_values);
  const __m512 sign1 = _mm512_mask_blend_ps(0xCCCC, negative_values, positive_values);
  const __m512 sign2 = _mm512_mask_blend_ps(0xF0F0, negative_values, positive_values);
  const __m512 sign3 = _mm512_mask_blend_ps(0xFF00, negative_values, positive_values);
  __m512 sums = _mm512_mul_ps(_mm512_set1_ps(residual[0]), sign0);
  sums = _mm512_add_ps(
      sums, _mm512_mul_ps(_mm512_set1_ps(residual[1]), sign1));
  sums = _mm512_add_ps(
      sums, _mm512_mul_ps(_mm512_set1_ps(residual[2]), sign2));
  sums = _mm512_add_ps(
      sums, _mm512_mul_ps(_mm512_set1_ps(residual[3]), sign3));
  _mm512_storeu_ps(lut, sums);
  min_values = _mm512_min_ps(min_values, sums);
  max_values = _mm512_max_ps(max_values, sums);
}

static void build_luts(const float *normalized_residual,
                                 const int64_t dim,
                                 const float positive,
                                 const float negative,
                                 float *luts,
                                 float &lut_min,
                                 float &lut_max)
{
  const int64_t code_bytes = (dim + CHAR_BIT - 1) / CHAR_BIT;
  __m512 min_values = _mm512_set1_ps(FLT_MAX);
  __m512 max_values = _mm512_set1_ps(-FLT_MAX);
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const int64_t offset = byte_index * CHAR_BIT;
    float tail[CHAR_BIT] = {};
    const float *residual = normalized_residual + offset;
    if (dim - offset < CHAR_BIT) {
      MEMCPY(tail, residual, (dim - offset) * sizeof(float));
      residual = tail;
    }
    float *low_lut = luts
        + byte_index * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    build_fast_scan_nibble_lut(
        residual, positive, negative, low_lut, min_values, max_values);
    build_fast_scan_nibble_lut(
        residual + ObRaBitQFastScanConstants::NIBBLE_BITS, positive, negative,
        low_lut + ObRaBitQFastScanConstants::NIBBLE_LUT_SIZE, min_values, max_values);
  }
  float min_lanes[16];
  float max_lanes[16];
  _mm512_storeu_ps(min_lanes, min_values);
  _mm512_storeu_ps(max_lanes, max_values);
  lut_min = FLT_MAX;
  lut_max = -FLT_MAX;
  for (int64_t lane = 0; lane < 16; ++lane) {
    lut_min = std::min(lut_min, min_lanes[lane]);
    lut_max = std::max(lut_max, max_lanes[lane]);
  }
}

static void quantize_luts(const float *float_luts,
                                    const int64_t lut_entry_count,
                                    const float lut_min,
                                    const float lut_scale,
                                    uint8_t *low_byte_luts,
                                    uint8_t *high_byte_luts)
{
  if (lut_scale <= 0.0F) {
    MEMSET(low_byte_luts, 0, lut_entry_count);
    MEMSET(high_byte_luts, 0, lut_entry_count);
  } else {
    const __m512 min_values = _mm512_set1_ps(lut_min);
    const __m512 scale_values = _mm512_set1_ps(lut_scale);
    const __m512 half = _mm512_set1_ps(0.5F);
    const __m512i zero = _mm512_setzero_si512();
    const __m512i max_value = _mm512_set1_epi32(UINT16_MAX);
    int64_t i = 0;
    for (; lut_entry_count - i >= 16; i += 16) {
      const __m512 scaled = _mm512_div_ps(
          _mm512_sub_ps(_mm512_loadu_ps(float_luts + i), min_values),
          scale_values);
      const __m512i rounded = _mm512_min_epi32(
          max_value,
          _mm512_max_epi32(zero, _mm512_cvttps_epi32(_mm512_add_ps(scaled, half))));
      uint32_t quantized[16];
      _mm512_storeu_si512(quantized, rounded);
      for (int64_t lane = 0; lane < 16; ++lane) {
        low_byte_luts[i + lane] =
            static_cast<uint8_t>(quantized[lane] & UINT8_MAX);
        high_byte_luts[i + lane] =
            static_cast<uint8_t>(quantized[lane] >> CHAR_BIT);
      }
    }
    specific::normal::quantize_luts(
        float_luts + i, lut_entry_count - i, lut_min, lut_scale,
        low_byte_luts + i, high_byte_luts + i);
  }
}

// A 64-byte lookup contains four four-dimension groups for the same 16 rows.
// Widen each group to uint32 and sum by row; no packed-u16 carries or row remap.
static __m512i sum_fast_scan_groups(const __m512i values)
{
  const __m512i first_pair = _mm512_add_epi32(
      _mm512_cvtepu8_epi32(_mm512_castsi512_si128(values)),
      _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(values, 1)));
  const __m512i second_pair = _mm512_add_epi32(
      _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(values, 2)),
      _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(values, 3)));
  return _mm512_add_epi32(first_pair, second_pair);
}

// Exactly one 32-byte column remains. AVX2 reads no bytes beyond that column.
static void add_fast_scan_tail(const uint8_t *packed_codes,
                               const uint8_t *low_byte_luts,
                               const uint8_t *high_byte_luts,
                               uint32_t *low_byte_sums,
                               uint32_t *high_byte_sums)
{
  uint32_t low_tail[ObRaBitQFastScanConstants::BATCH_SIZE];
  uint32_t high_tail[ObRaBitQFastScanConstants::BATCH_SIZE];
  specific::avx2::accumulate_lut_pairs(
      packed_codes, low_byte_luts, high_byte_luts, 1, low_tail, high_tail);
  for (int64_t row = 0; row < ObRaBitQFastScanConstants::BATCH_SIZE; ++row) {
    low_byte_sums[row] += low_tail[row];
    high_byte_sums[row] += high_tail[row];
  }
}

static void accumulate_lut_pairs(const uint8_t *packed_codes,
                                 const uint8_t *low_byte_luts,
                                 const uint8_t *high_byte_luts,
                                 const int64_t code_bytes,
                                 uint32_t *low_byte_sums,
                                 uint32_t *high_byte_sums)
{
  const __m512i nibble_mask = _mm512_set1_epi8(ObRaBitQFastScanConstants::NIBBLE_MASK);
  __m512i low_byte_rows_0_15 = _mm512_setzero_si512();
  __m512i low_byte_rows_16_31 = _mm512_setzero_si512();
  __m512i high_byte_rows_0_15 = _mm512_setzero_si512();
  __m512i high_byte_rows_16_31 = _mm512_setzero_si512();
  int64_t byte_index = 0;
  for (; code_bytes - byte_index >= 2; byte_index += 2) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    const __m512i codes = _mm512_loadu_si512(packed_codes + offset);
    const __m512i rows_0_15 = _mm512_and_si512(codes, nibble_mask);
    const __m512i rows_16_31 = _mm512_and_si512(
        _mm512_srli_epi16(codes, ObRaBitQFastScanConstants::NIBBLE_BITS), nibble_mask);
    const __m512i low_lut = _mm512_loadu_si512(low_byte_luts + offset);
    const __m512i high_lut = _mm512_loadu_si512(high_byte_luts + offset);
    low_byte_rows_0_15 = _mm512_add_epi32(low_byte_rows_0_15,
        sum_fast_scan_groups(_mm512_shuffle_epi8(low_lut, rows_0_15)));
    low_byte_rows_16_31 = _mm512_add_epi32(low_byte_rows_16_31,
        sum_fast_scan_groups(_mm512_shuffle_epi8(low_lut, rows_16_31)));
    high_byte_rows_0_15 = _mm512_add_epi32(high_byte_rows_0_15,
        sum_fast_scan_groups(_mm512_shuffle_epi8(high_lut, rows_0_15)));
    high_byte_rows_16_31 = _mm512_add_epi32(high_byte_rows_16_31,
        sum_fast_scan_groups(_mm512_shuffle_epi8(high_lut, rows_16_31)));
  }
  _mm512_storeu_si512(low_byte_sums, low_byte_rows_0_15);
  _mm512_storeu_si512(low_byte_sums + 16, low_byte_rows_16_31);
  _mm512_storeu_si512(high_byte_sums, high_byte_rows_0_15);
  _mm512_storeu_si512(high_byte_sums + 16, high_byte_rows_16_31);
  if (byte_index < code_bytes) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    add_fast_scan_tail(packed_codes + offset, low_byte_luts + offset,
        high_byte_luts + offset, low_byte_sums, high_byte_sums);
  }
}

__attribute__((target("avx512f,avx512bw,avx512dq,avx512vl,avx512vbmi,avx512vnni")))
static void accumulate_lut_pairs_vnni(const uint8_t *packed_codes,
                                      const uint8_t *low_byte_luts,
                                      const uint8_t *high_byte_luts,
                                      const int64_t code_bytes,
                                      uint32_t *low_byte_sums,
                                      uint32_t *high_byte_sums)
{
  // Gather the four dimension groups for each row into four adjacent bytes.
  // This transposes dimension groups, not row IDs: rows remain 0,1,...,15.
  alignas(64) static const uint8_t GROUP_TRANSPOSE[64] = {
      0,16,32,48, 1,17,33,49, 2,18,34,50, 3,19,35,51,
      4,20,36,52, 5,21,37,53, 6,22,38,54, 7,23,39,55,
      8,24,40,56, 9,25,41,57, 10,26,42,58, 11,27,43,59,
      12,28,44,60, 13,29,45,61, 14,30,46,62, 15,31,47,63};
  const __m512i transpose = _mm512_load_si512(GROUP_TRANSPOSE);
  // Four 16-entry LUTs occupy offsets 0,16,32,48 in the loaded LUT register.
  const __m512i lut_offsets = _mm512_set1_epi32(0x30201000);
  const __m512i nibble_mask = _mm512_set1_epi8(ObRaBitQFastScanConstants::NIBBLE_MASK);
  const __m512i ones = _mm512_set1_epi8(1);
  __m512i low_byte_rows_0_15 = _mm512_setzero_si512();
  __m512i low_byte_rows_16_31 = _mm512_setzero_si512();
  __m512i high_byte_rows_0_15 = _mm512_setzero_si512();
  __m512i high_byte_rows_16_31 = _mm512_setzero_si512();
  int64_t byte_index = 0;
  for (; code_bytes - byte_index >= 2; byte_index += 2) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    const __m512i codes = _mm512_permutexvar_epi8(
        transpose, _mm512_loadu_si512(packed_codes + offset));
    const __m512i rows_0_15 = _mm512_add_epi8(
        _mm512_and_si512(codes, nibble_mask), lut_offsets);
    const __m512i rows_16_31 = _mm512_add_epi8(_mm512_and_si512(
        _mm512_srli_epi16(codes, ObRaBitQFastScanConstants::NIBBLE_BITS), nibble_mask), lut_offsets);
    const __m512i low_lut = _mm512_loadu_si512(low_byte_luts + offset);
    const __m512i high_lut = _mm512_loadu_si512(high_byte_luts + offset);
    // dpbusd sums four uint8 LUT values (times signed +1) into each uint32 row.
    low_byte_rows_0_15 = _mm512_dpbusd_epi32(low_byte_rows_0_15,
        _mm512_permutexvar_epi8(rows_0_15, low_lut), ones);
    low_byte_rows_16_31 = _mm512_dpbusd_epi32(low_byte_rows_16_31,
        _mm512_permutexvar_epi8(rows_16_31, low_lut), ones);
    high_byte_rows_0_15 = _mm512_dpbusd_epi32(high_byte_rows_0_15,
        _mm512_permutexvar_epi8(rows_0_15, high_lut), ones);
    high_byte_rows_16_31 = _mm512_dpbusd_epi32(high_byte_rows_16_31,
        _mm512_permutexvar_epi8(rows_16_31, high_lut), ones);
  }
  _mm512_storeu_si512(low_byte_sums, low_byte_rows_0_15);
  _mm512_storeu_si512(low_byte_sums + 16, low_byte_rows_16_31);
  _mm512_storeu_si512(high_byte_sums, high_byte_rows_0_15);
  _mm512_storeu_si512(high_byte_sums + 16, high_byte_rows_16_31);
  if (byte_index < code_bytes) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    add_fast_scan_tail(packed_codes + offset, low_byte_luts + offset,
        high_byte_luts + offset, low_byte_sums, high_byte_sums);
  }
}

static int64_t recover_metrics(
    const uint32_t *low_byte_sums,
    const uint32_t *high_byte_sums,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *metric_values)
{
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float lut_offset = computer.get_lut_offset();
  const float lut_scale = computer.get_lut_scale();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  const bool is_query_zero = is_ip_or_cosine
      && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON;
  const __m512 zero = _mm512_setzero_ps();
  const __m512 one = _mm512_set1_ps(1.0F);
  const __m512 negative_one = _mm512_set1_ps(-1.0F);
  const __m512 two = _mm512_set1_ps(2.0F);
  const __m512 half = _mm512_set1_ps(0.5F);
  const __m512 epsilon = _mm512_set1_ps(ObRaBitQFastScanConstants::ZERO_EPSILON);
  const __m512i abs_mask = _mm512_set1_epi32(INT32_MAX);
  const __m512 offset = _mm512_set1_ps(lut_offset);
  const __m512 scale = _mm512_set1_ps(lut_scale);
  const __m512 query_residual = _mm512_set1_ps(query_residual_norm);
  const __m512 query_residual_squared = _mm512_mul_ps(query_residual, query_residual);
  const __m512 query_raw = _mm512_set1_ps(query_raw_norm);
  const __m512 query_raw_squared = _mm512_mul_ps(query_raw, query_raw);
  int64_t i = 0;
  for (; vector_count - i >= 16; i += 16) {
    const __m512 residual = _mm512_loadu_ps(residual_norms + i);
    const __m512 raw = _mm512_loadu_ps(raw_norms + i);
    const __mmask16 invalid_mask =
        _mm512_cmp_ps_mask(residual, zero, _CMP_LT_OQ)
        | _mm512_cmp_ps_mask(raw, zero, _CMP_LT_OQ);
    if (OB_UNLIKELY(0 != invalid_mask)) {
      const int64_t invalid_lane =
          __builtin_ctz(static_cast<unsigned int>(invalid_mask));
      specific::avx2::recover_metrics(
          low_byte_sums + i, high_byte_sums + i, residual_norms + i, corrections + i,
          raw_norms + i, invalid_lane, computer, metric_values + i);
      return i + invalid_lane;
    }

    __m512 metric_value = one;
    if (!is_query_zero) {
      const __m512i quantized_sum = _mm512_add_epi32(
          _mm512_loadu_si512(low_byte_sums + i),
          _mm512_slli_epi32(_mm512_loadu_si512(high_byte_sums + i), CHAR_BIT));
      const __m512 normalized_dot = _mm512_add_ps(
          offset, _mm512_mul_ps(scale, _mm512_cvtepi32_ps(quantized_sum)));
      const __m512 correction = _mm512_loadu_ps(corrections + i);
      const __m512 abs_correction = _mm512_castsi512_ps(_mm512_and_si512(
          _mm512_castps_si512(correction), abs_mask));
      const __mmask16 small_correction =
          _mm512_cmp_ps_mask(abs_correction, epsilon, _CMP_LT_OQ);
      const __mmask16 positive_correction =
          _mm512_cmp_ps_mask(correction, zero, _CMP_GT_OQ);
      const __m512 correction_sign =
          _mm512_mask_mov_ps(negative_one, positive_correction, one);
      const __m512 safe_correction =
          _mm512_mask_mov_ps(correction, small_correction, correction_sign);
      const __m512 corrected_dot = _mm512_div_ps(normalized_dot, safe_correction);
      const __m512 estimated_l2_squared = _mm512_sub_ps(
          _mm512_add_ps(_mm512_mul_ps(residual, residual), query_residual_squared),
          _mm512_mul_ps(_mm512_mul_ps(
              two, _mm512_mul_ps(residual, query_residual)), corrected_dot));
      if (!is_ip_or_cosine) {
        metric_value = estimated_l2_squared;
      } else {
        const __m512 raw_dot_twice = _mm512_sub_ps(
            _mm512_add_ps(query_raw_squared, _mm512_mul_ps(raw, raw)),
            estimated_l2_squared);
        const __m512 abs_raw = _mm512_castsi512_ps(_mm512_and_si512(
            _mm512_castps_si512(raw), abs_mask));
        const __mmask16 small_raw =
            _mm512_cmp_ps_mask(abs_raw, epsilon, _CMP_LT_OQ);
        if (share::VIDA_IP == metric) {
          metric_value = _mm512_sub_ps(one, _mm512_mul_ps(half, raw_dot_twice));
        } else {
          const __m512 safe_raw = _mm512_mask_mov_ps(raw, small_raw, one);
          metric_value = _mm512_sub_ps(one, _mm512_div_ps(
              _mm512_mul_ps(half, raw_dot_twice),
              _mm512_mul_ps(query_raw, safe_raw)));
        }
        metric_value = _mm512_mask_mov_ps(metric_value, small_raw, one);
      }
    }
    _mm512_storeu_ps(metric_values + i, metric_value);
  }
  int64_t invalid_index = -1;
  if (i < vector_count) {
    const int64_t tail_invalid = specific::avx2::recover_metrics(
        low_byte_sums + i, high_byte_sums + i, residual_norms + i, corrections + i,
        raw_norms + i, vector_count - i, computer, metric_values + i);
    invalid_index = tail_invalid < 0 ? -1 : i + tail_invalid;
  }
  return invalid_index;
}

static void compute_lower_bounds(
    const float *metric_values,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer,
    float *lower_bounds)
{
  const int64_t dim = computer.get_dim();
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const float query_residual_norm = computer.get_query_residual_norm();
  const float query_raw_norm = computer.get_query_raw_norm();
  const float dim_error_scale = computer.get_dim_error_scale();
  const float lut_dot_error = computer.get_lut_dot_error();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  int64_t i = 0;
  if (dim > 1
      && !(is_ip_or_cosine
           && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON)) {
    const __m512 zero = _mm512_setzero_ps();
    const __m512 one = _mm512_set1_ps(1.0F);
    const __m512 two = _mm512_set1_ps(2.0F);
    const __m512 epsilon = _mm512_set1_ps(ObRaBitQFastScanConstants::ZERO_EPSILON);
    const __m512 negative_max = _mm512_set1_ps(-FLT_MAX);
    const __m512i abs_mask = _mm512_set1_epi32(INT32_MAX);
    const __m512 query_residual = _mm512_set1_ps(query_residual_norm);
    const __m512 query_raw = _mm512_set1_ps(query_raw_norm);
    const __m512 error_scale = _mm512_set1_ps(dim_error_scale);
    const __m512 lut_error = _mm512_set1_ps(lut_dot_error);
    for (; vector_count - i >= 16; i += 16) {
      const __m512 metrics = _mm512_loadu_ps(metric_values + i);
      const __m512 residuals = _mm512_loadu_ps(residual_norms + i);
      const __m512 correction = _mm512_loadu_ps(corrections + i);
      const __m512 raw = _mm512_loadu_ps(raw_norms + i);
      const __m512 abs_correction = _mm512_castsi512_ps(_mm512_and_si512(
          _mm512_castps_si512(correction), abs_mask));
      const __mmask16 small_correction =
          _mm512_cmp_ps_mask(abs_correction, epsilon, _CMP_LT_OQ);
      const __m512 safe_abs_correction =
          _mm512_mask_mov_ps(abs_correction, small_correction, one);
      const __m512 correction_squared =
          _mm512_mul_ps(safe_abs_correction, safe_abs_correction);
      const __m512 ratio = _mm512_div_ps(
          _mm512_max_ps(zero, _mm512_sub_ps(one, correction_squared)),
          correction_squared);
      const __m512 normalized_dot_error = _mm512_add_ps(
          _mm512_mul_ps(error_scale, _mm512_sqrt_ps(ratio)),
          _mm512_div_ps(lut_error, safe_abs_correction));
      __m512 error = _mm512_mul_ps(
          _mm512_mul_ps(query_residual, residuals), normalized_dot_error);
      if (share::VIDA_L2 == metric) {
        error = _mm512_mul_ps(error, two);
      } else if (share::VIDA_COS == metric) {
        const __m512 abs_raw = _mm512_castsi512_ps(_mm512_and_si512(
            _mm512_castps_si512(raw), abs_mask));
        const __mmask16 small_raw =
            _mm512_cmp_ps_mask(abs_raw, epsilon, _CMP_LT_OQ);
        const __m512 safe_raw = _mm512_mask_mov_ps(raw, small_raw, one);
        error = _mm512_div_ps(error, _mm512_mul_ps(query_raw, safe_raw));
      }
      __m512 lower = _mm512_sub_ps(metrics, error);
      lower = _mm512_mask_mov_ps(lower, small_correction, negative_max);
      if (is_ip_or_cosine) {
        const __m512 abs_raw = _mm512_castsi512_ps(_mm512_and_si512(
            _mm512_castps_si512(raw), abs_mask));
        const __mmask16 small_raw =
            _mm512_cmp_ps_mask(abs_raw, epsilon, _CMP_LT_OQ);
        lower = _mm512_mask_mov_ps(lower, small_raw, metrics);
      }
      _mm512_storeu_ps(lower_bounds + i, lower);
    }
  }
  if (i < vector_count) {
    specific::avx2::compute_lower_bounds(
        metric_values + i, residual_norms + i, corrections + i, raw_norms + i,
        vector_count - i, computer, lower_bounds + i);
  }
}

static const ObRaBitQFastScanSimdOps RABITQ_FAST_SCAN_OPS = {
    build_luts, quantize_luts,
    accumulate_lut_pairs, recover_metrics,
    compute_lower_bounds};

static const ObRaBitQFastScanSimdOps RABITQ_FAST_SCAN_VNNI_OPS = {
    build_luts, quantize_luts,
    accumulate_lut_pairs_vnni, recover_metrics,
    compute_lower_bounds};

)

#endif // OB_USE_MULTITARGET_CODE

#if OB_ARM_USE_MULTITARGET_CODE
namespace specific
{
namespace neon
{

static const uint32_t SIGN_BITS[8] = {1, 2, 4, 8, 16, 32, 64, 128};

static float dot_packed_codes(const float *query, const uint8_t *packed_codes,
                              const int64_t dim, const float inv_sqrt_dim)
{
  if (0 == dim) { return 0.0F; }
  const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
  const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
  const float32x4_t positives = vdupq_n_f32(positive);
  const float32x4_t negatives = vdupq_n_f32(negative);
  const uint32x4_t low_bits = vld1q_u32(SIGN_BITS);
  const uint32x4_t high_bits = vld1q_u32(SIGN_BITS + 4);
  float32x4_t low_sum = vdupq_n_f32(0.0F);
  float32x4_t high_sum = vdupq_n_f32(0.0F);
  int64_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    const uint32x4_t bits = vdupq_n_u32(packed_codes[i / 8]);
    const float32x4_t low_signs = vbslq_f32(vtstq_u32(bits, low_bits), positives, negatives);
    const float32x4_t high_signs = vbslq_f32(vtstq_u32(bits, high_bits), positives, negatives);
    low_sum = vaddq_f32(low_sum, vmulq_f32(vld1q_f32(query + i), low_signs));
    high_sum = vaddq_f32(high_sum, vmulq_f32(vld1q_f32(query + i + 4), high_signs));
  }
  return vaddvq_f32(vaddq_f32(low_sum, high_sum))
      + specific::normal::dot_packed_codes(query + i, packed_codes + i / 8, dim - i, inv_sqrt_dim);
}

static const ObRaBitQSimdOps RABITQ_OPS = {
    specific::normal::pack_sign_codes, dot_packed_codes, nullptr};

static void apply_signs(const int64_t dim, const uint8_t *signs, float *values)
{
  const uint32x4_t low_bits = vld1q_u32(SIGN_BITS);
  const uint32x4_t high_bits = vld1q_u32(SIGN_BITS + 4);
  const uint32x4_t sign_bit = vdupq_n_u32(0x80000000U);
  int64_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    const uint32x4_t bits = vdupq_n_u32(signs[i / 8]);
    const uint32x4_t low_mask = vandq_u32(vtstq_u32(bits, low_bits), sign_bit);
    const uint32x4_t high_mask = vandq_u32(vtstq_u32(bits, high_bits), sign_bit);
    vst1q_f32(values + i, vreinterpretq_f32_u32(
        veorq_u32(vreinterpretq_u32_f32(vld1q_f32(values + i)), low_mask)));
    vst1q_f32(values + i + 4, vreinterpretq_f32_u32(
        veorq_u32(vreinterpretq_u32_f32(vld1q_f32(values + i + 4)), high_mask)));
  }
  specific::normal::apply_signs(dim - i, signs + i / 8, values + i);
}

static void fht(const int64_t length, float *values)
{
  int64_t half = 1;
  while (half < length) {
    const int64_t block = half * 2;
    for (int64_t base = 0; base < length; base += block) {
      int64_t i = 0;
      for (; i + 4 <= half; i += 4) {
        const float32x4_t lhs = vld1q_f32(values + base + i);
        const float32x4_t rhs = vld1q_f32(values + base + half + i);
        vst1q_f32(values + base + i, vaddq_f32(lhs, rhs));
        vst1q_f32(values + base + half + i, vsubq_f32(lhs, rhs));
      }
      for (; i < half; ++i) {
        const float lhs = values[base + i];
        const float rhs = values[base + half + i];
        values[base + i] = lhs + rhs;
        values[base + half + i] = lhs - rhs;
      }
    }
    if (half == length / 2) { break; }
    half <<= 1;
  }
}

static void scale(const int64_t length, const float factor, float *values)
{
  const float32x4_t factors = vdupq_n_f32(factor);
  int64_t i = 0;
  for (; i + 4 <= length; i += 4) {
    vst1q_f32(values + i, vmulq_f32(vld1q_f32(values + i), factors));
  }
  specific::normal::scale(length - i, factor, values + i);
}

static void kac(const int64_t length, float *values)
{
  const int64_t pair_count = length / 2;
  const int64_t offset = length % 2 + pair_count;
  int64_t i = 0;
  for (; i + 4 <= pair_count; i += 4) {
    const float32x4_t lhs = vld1q_f32(values + i);
    const float32x4_t rhs = vld1q_f32(values + offset + i);
    vst1q_f32(values + i, vaddq_f32(lhs, rhs));
    vst1q_f32(values + offset + i, vsubq_f32(lhs, rhs));
  }
  for (; i < pair_count; ++i) {
    const float lhs = values[i];
    const float rhs = values[offset + i];
    values[i] = lhs + rhs;
    values[offset + i] = lhs - rhs;
  }
  if (length % 2 != 0) { values[pair_count] *= specific::normal::SQRT_TWO; }
}

static const ObFhtKacSimdOps FHT_KAC_OPS = {apply_signs, fht, scale, kac};

static void build_nibble_lut(const float *residual, const float positive,
    const float negative, float *lut, float32x4_t &minimum, float32x4_t &maximum)
{
  for (uint32_t first = 0; first < 16; first += 4) {
    const uint32_t indices[4] = {first, first + 1, first + 2, first + 3};
    const uint32x4_t masks = vld1q_u32(indices);
    float32x4_t sums = vdupq_n_f32(0.0F);
    for (uint32_t bit = 0; bit < 4; ++bit) {
      const float32x4_t signs = vbslq_f32(
          vtstq_u32(masks, vdupq_n_u32(1U << bit)),
          vdupq_n_f32(positive), vdupq_n_f32(negative));
      const float32x4_t product = vmulq_f32(vdupq_n_f32(residual[bit]), signs);
      sums = bit == 0 ? product : vaddq_f32(sums, product);
    }
    vst1q_f32(lut + first, sums);
    minimum = vminq_f32(minimum, sums);
    maximum = vmaxq_f32(maximum, sums);
  }
}

static void build_luts(const float *normalized_residual, const int64_t dim,
    const float positive, const float negative, float *luts, float &lut_min, float &lut_max)
{
  const int64_t code_bytes = (dim + CHAR_BIT - 1) / CHAR_BIT;
  float32x4_t minimum = vdupq_n_f32(FLT_MAX);
  float32x4_t maximum = vdupq_n_f32(-FLT_MAX);
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const int64_t offset = byte_index * CHAR_BIT;
    float tail[CHAR_BIT] = {};
    const float *residual = normalized_residual + offset;
    if (dim - offset < CHAR_BIT) {
      MEMCPY(tail, residual, (dim - offset) * sizeof(float));
      residual = tail;
    }
    float *lut = luts + byte_index * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    build_nibble_lut(residual, positive, negative, lut, minimum, maximum);
    build_nibble_lut(residual + 4, positive, negative, lut + 16, minimum, maximum);
  }
  lut_min = vminvq_f32(minimum);
  lut_max = vmaxvq_f32(maximum);
}

static void quantize_luts(const float *float_luts, const int64_t lut_entry_count,
    const float lut_min, const float lut_scale, uint8_t *low_byte_luts,
    uint8_t *high_byte_luts)
{
  if (lut_scale <= 0.0F) {
    MEMSET(low_byte_luts, 0, lut_entry_count);
    MEMSET(high_byte_luts, 0, lut_entry_count);
    return;
  }
  const float32x4_t minima = vdupq_n_f32(lut_min);
  const float32x4_t scales = vdupq_n_f32(lut_scale);
  const float32x4_t zero = vdupq_n_f32(0.0F);
  const float32x4_t limit = vdupq_n_f32(UINT16_MAX);
  int64_t i = 0;
  for (; i + 4 <= lut_entry_count; i += 4) {
    const float32x4_t scaled = vdivq_f32(vsubq_f32(vld1q_f32(float_luts + i), minima), scales);
    const uint32x4_t quantized = vcvtaq_u32_f32(vminq_f32(limit, vmaxq_f32(zero, scaled)));
    uint32_t values[4];
    vst1q_u32(values, quantized);
    for (int64_t lane = 0; lane < 4; ++lane) {
      low_byte_luts[i + lane] = static_cast<uint8_t>(values[lane] & UINT8_MAX);
      high_byte_luts[i + lane] = static_cast<uint8_t>(values[lane] >> CHAR_BIT);
    }
  }
  specific::normal::quantize_luts(float_luts + i, lut_entry_count - i,
      lut_min, lut_scale, low_byte_luts + i, high_byte_luts + i);
}

// Each LUT contains two 16-entry nibble tables. Codes are stored in two
// 16-row groups; a row's sum combines the corresponding byte from each table.
static void add_lut_values(const uint8x16_t first, const uint8x16_t second,
    uint32x4_t *sums)
{
  const uint16x8_t low = vaddl_u8(vget_low_u8(first), vget_low_u8(second));
  const uint16x8_t high = vaddl_u8(vget_high_u8(first), vget_high_u8(second));
  sums[0] = vaddw_u16(sums[0], vget_low_u16(low));
  sums[1] = vaddw_u16(sums[1], vget_high_u16(low));
  sums[2] = vaddw_u16(sums[2], vget_low_u16(high));
  sums[3] = vaddw_u16(sums[3], vget_high_u16(high));
}

static void accumulate_lut_pairs(const uint8_t *packed_codes,
    const uint8_t *low_byte_luts, const uint8_t *high_byte_luts,
    const int64_t code_bytes, uint32_t *low_byte_sums, uint32_t *high_byte_sums)
{
  uint32x4_t low_sums[8];
  uint32x4_t high_sums[8];
  for (int group = 0; group < 8; ++group) {
    low_sums[group] = vdupq_n_u32(0);
    high_sums[group] = vdupq_n_u32(0);
  }
  const uint8x16_t nibble_mask = vdupq_n_u8(ObRaBitQFastScanConstants::NIBBLE_MASK);
  for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
    const int64_t offset = byte_index * ObRaBitQFastScanConstants::BATCH_SIZE;
    const uint8x16_t codes0 = vld1q_u8(packed_codes + offset);
    const uint8x16_t codes1 = vld1q_u8(packed_codes + offset + 16);
    const uint8x16_t first_lut = vld1q_u8(low_byte_luts + offset);
    const uint8x16_t second_lut = vld1q_u8(low_byte_luts + offset + 16);
    const uint8x16_t first_high_lut = vld1q_u8(high_byte_luts + offset);
    const uint8x16_t second_high_lut = vld1q_u8(high_byte_luts + offset + 16);
    const uint8x16_t rows0 = vandq_u8(codes0, nibble_mask);
    const uint8x16_t rows1 = vandq_u8(vshrq_n_u8(codes0, 4), nibble_mask);
    const uint8x16_t rows2 = vandq_u8(codes1, nibble_mask);
    const uint8x16_t rows3 = vandq_u8(vshrq_n_u8(codes1, 4), nibble_mask);
    add_lut_values(vqtbl1q_u8(first_lut, rows0), vqtbl1q_u8(second_lut, rows2), low_sums);
    add_lut_values(vqtbl1q_u8(first_lut, rows1), vqtbl1q_u8(second_lut, rows3), low_sums + 4);
    add_lut_values(vqtbl1q_u8(first_high_lut, rows0), vqtbl1q_u8(second_high_lut, rows2), high_sums);
    add_lut_values(vqtbl1q_u8(first_high_lut, rows1), vqtbl1q_u8(second_high_lut, rows3), high_sums + 4);
  }
  for (int group = 0; group < 8; ++group) {
    vst1q_u32(low_byte_sums + group * 4, low_sums[group]);
    vst1q_u32(high_byte_sums + group * 4, high_sums[group]);
  }
}

static int64_t recover_metrics(const uint32_t *low_byte_sums,
    const uint32_t *high_byte_sums, const float *residual_norms,
    const float *corrections, const float *raw_norms, const int64_t vector_count,
    const ObRaBitQFastScanComputer &computer, float *metric_values)
{
  const share::ObVectorIndexDistAlgorithm metric = computer.get_metric();
  const bool is_ip_or_cosine = share::VIDA_IP == metric || share::VIDA_COS == metric;
  const float query_raw_norm = computer.get_query_raw_norm();
  const bool is_query_zero = is_ip_or_cosine
      && std::abs(query_raw_norm) < ObRaBitQFastScanConstants::ZERO_EPSILON;
  const float32x4_t zero = vdupq_n_f32(0.0F);
  const float32x4_t one = vdupq_n_f32(1.0F);
  const float32x4_t negative_one = vdupq_n_f32(-1.0F);
  const float32x4_t half = vdupq_n_f32(0.5F);
  const float32x4_t two = vdupq_n_f32(2.0F);
  const float32x4_t epsilon = vdupq_n_f32(ObRaBitQFastScanConstants::ZERO_EPSILON);
  const float32x4_t offset = vdupq_n_f32(computer.get_lut_offset());
  const float32x4_t scale = vdupq_n_f32(computer.get_lut_scale());
  const float32x4_t query_residual = vdupq_n_f32(computer.get_query_residual_norm());
  const float32x4_t query_residual_squared = vmulq_f32(query_residual, query_residual);
  const float32x4_t query_raw = vdupq_n_f32(query_raw_norm);
  const float32x4_t query_raw_squared = vmulq_f32(query_raw, query_raw);
  int64_t i = 0;
  for (; i + 4 <= vector_count; i += 4) {
    const float32x4_t residual = vld1q_f32(residual_norms + i);
    const float32x4_t raw = vld1q_f32(raw_norms + i);
    const uint32x4_t invalid = vorrq_u32(vcltq_f32(residual, zero), vcltq_f32(raw, zero));
    uint32_t invalid_lanes[4];
    vst1q_u32(invalid_lanes, invalid);
    for (int lane = 0; lane < 4; ++lane) {
      if (invalid_lanes[lane]) {
        specific::normal::recover_metrics(low_byte_sums + i, high_byte_sums + i,
            residual_norms + i, corrections + i, raw_norms + i, lane, computer,
            metric_values + i);
        return i + lane;
      }
    }

    float32x4_t result = one;
    if (!is_query_zero) {
      const uint32x4_t quantized = vaddq_u32(vld1q_u32(low_byte_sums + i),
          vshlq_n_u32(vld1q_u32(high_byte_sums + i), CHAR_BIT));
      const float32x4_t normalized_dot = vaddq_f32(offset,
          vmulq_f32(scale, vcvtq_f32_u32(quantized)));
      const float32x4_t correction = vld1q_f32(corrections + i);
      const uint32x4_t small_correction = vcltq_f32(vabsq_f32(correction), epsilon);
      const float32x4_t correction_sign = vbslq_f32(
          vcgtq_f32(correction, zero), one, negative_one);
      const float32x4_t safe_correction = vbslq_f32(
          small_correction, correction_sign, correction);
      const float32x4_t corrected_dot = vdivq_f32(normalized_dot, safe_correction);
      const float32x4_t estimated_l2_squared = vsubq_f32(
          vaddq_f32(vmulq_f32(residual, residual), query_residual_squared),
          vmulq_f32(vmulq_f32(two, vmulq_f32(residual, query_residual)), corrected_dot));
      if (!is_ip_or_cosine) {
        result = estimated_l2_squared;
      } else {
        const float32x4_t raw_dot_twice = vsubq_f32(
            vaddq_f32(query_raw_squared, vmulq_f32(raw, raw)), estimated_l2_squared);
        const uint32x4_t small_raw = vcltq_f32(vabsq_f32(raw), epsilon);
        if (share::VIDA_IP == metric) {
          result = vsubq_f32(one, vmulq_f32(half, raw_dot_twice));
        } else {
          const float32x4_t safe_raw = vbslq_f32(small_raw, one, raw);
          result = vsubq_f32(one, vdivq_f32(vmulq_f32(half, raw_dot_twice),
              vmulq_f32(query_raw, safe_raw)));
        }
        result = vbslq_f32(small_raw, one, result);
      }
    }
    vst1q_f32(metric_values + i, result);
  }
  if (i < vector_count) {
    const int64_t tail_invalid = specific::normal::recover_metrics(
        low_byte_sums + i, high_byte_sums + i, residual_norms + i,
        corrections + i, raw_norms + i, vector_count - i, computer,
        metric_values + i);
    return tail_invalid < 0 ? -1 : i + tail_invalid;
  }
  return -1;
}

static const ObRaBitQFastScanSimdOps RABITQ_FAST_SCAN_OPS = {
    build_luts, quantize_luts, accumulate_lut_pairs,
    // Keep scalar error arithmetic consistent with compute_bounds_batch().
    recover_metrics, specific::normal::compute_lower_bounds};

} // namespace neon
} // namespace specific
#endif // OB_ARM_USE_MULTITARGET_CODE

const ObRaBitQSimdOps &get_rabitq_simd_ops()
{
#if OB_USE_MULTITARGET_CODE
  const ObRaBitQSimdOps *ops =
      get_rabitq_simd_ops(common::ObTargetArch::AVX512);
  if (nullptr == ops) {
    ops = get_rabitq_simd_ops(common::ObTargetArch::AVX2);
  }
  if (nullptr != ops) {
    return *ops;
  }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
  if (const ObRaBitQSimdOps *ops = get_rabitq_simd_ops(common::ObTargetArch::NEON)) {
    return *ops;
  }
#endif
  return specific::normal::RABITQ_OPS;
}

const ObRaBitQSimdOps *get_rabitq_simd_ops(const common::ObTargetArch arch)
{
  const ObRaBitQSimdOps *ops = nullptr;
  switch (arch) {
    case common::ObTargetArch::Default:
      ops = &specific::normal::RABITQ_OPS;
      break;
#if OB_USE_MULTITARGET_CODE
    case common::ObTargetArch::AVX2:
      if (common::is_arch_supported(arch)) {
        ops = &specific::avx2::RABITQ_OPS;
      }
      break;
    case common::ObTargetArch::AVX512:
      if (common::is_arch_supported(common::ObTargetArch::AVX512) &&
          common::is_arch_supported(common::ObTargetArch::AVX2)) {
        ops = &specific::avx512::RABITQ_OPS;
      }
      break;
#endif
#if OB_ARM_USE_MULTITARGET_CODE
    case common::ObTargetArch::NEON:
      if (common::is_arch_supported(arch)) { ops = &specific::neon::RABITQ_OPS; }
      break;
#endif
    default:
      break;
  }
  return ops;
}

const ObFhtKacSimdOps &get_fht_kac_simd_ops()
{
#if OB_USE_MULTITARGET_CODE
  const ObFhtKacSimdOps *ops =
      get_fht_kac_simd_ops(common::ObTargetArch::AVX512);
  if (nullptr == ops) {
    ops = get_fht_kac_simd_ops(common::ObTargetArch::AVX2);
  }
  if (nullptr != ops) {
    return *ops;
  }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
  if (const ObFhtKacSimdOps *ops = get_fht_kac_simd_ops(common::ObTargetArch::NEON)) {
    return *ops;
  }
#endif
  return specific::normal::FHT_KAC_OPS;
}

const ObFhtKacSimdOps *get_fht_kac_simd_ops(
    const common::ObTargetArch arch)
{
  const ObFhtKacSimdOps *ops = nullptr;
  switch (arch) {
    case common::ObTargetArch::Default:
      ops = &specific::normal::FHT_KAC_OPS;
      break;
#if OB_USE_MULTITARGET_CODE
    case common::ObTargetArch::AVX2:
      if (common::is_arch_supported(arch)) {
        ops = &specific::avx2::FHT_KAC_OPS;
      }
      break;
    case common::ObTargetArch::AVX512:
      if (common::is_arch_supported(common::ObTargetArch::AVX512) &&
          common::is_arch_supported(common::ObTargetArch::AVX2)) {
        ops = &specific::avx512::FHT_KAC_OPS;
      }
      break;
#endif
#if OB_ARM_USE_MULTITARGET_CODE
    case common::ObTargetArch::NEON:
      if (common::is_arch_supported(arch)) { ops = &specific::neon::FHT_KAC_OPS; }
      break;
#endif
    default:
      break;
  }
  return ops;
}

const ObRaBitQFastScanSimdOps &get_rabitq_fast_scan_simd_ops()
{
#if OB_USE_MULTITARGET_CODE
  const ObRaBitQFastScanSimdOps *ops =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX512);
  if (nullptr != ops) {
    static const bool has_vbmi_vnni =
        __builtin_cpu_supports("avx512vbmi") && __builtin_cpu_supports("avx512vnni");
    return has_vbmi_vnni ? specific::avx512::RABITQ_FAST_SCAN_VNNI_OPS : *ops;
  }
  if (nullptr == ops) {
    ops = get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX2);
  }
  if (nullptr != ops) {
    return *ops;
  }
#endif
#if OB_ARM_USE_MULTITARGET_CODE
  if (const ObRaBitQFastScanSimdOps *ops = get_rabitq_fast_scan_simd_ops(common::ObTargetArch::NEON)) {
    return *ops;
  }
#endif
  return specific::normal::RABITQ_FAST_SCAN_OPS;
}

const ObRaBitQFastScanSimdOps *get_rabitq_fast_scan_simd_ops(
    const common::ObTargetArch arch)
{
  const ObRaBitQFastScanSimdOps *ops = nullptr;
  switch (arch) {
    case common::ObTargetArch::Default:
      ops = &specific::normal::RABITQ_FAST_SCAN_OPS;
      break;
#if OB_USE_MULTITARGET_CODE
    case common::ObTargetArch::AVX2:
      if (common::is_arch_supported(arch)) {
        ops = &specific::avx2::RABITQ_FAST_SCAN_OPS;
      }
      break;
    case common::ObTargetArch::AVX512:
      if (common::is_arch_supported(common::ObTargetArch::AVX512)
          && common::is_arch_supported(common::ObTargetArch::AVX2)) {
        ops = &specific::avx512::RABITQ_FAST_SCAN_OPS;
      }
      break;
#endif
#if OB_ARM_USE_MULTITARGET_CODE
    case common::ObTargetArch::NEON:
      if (common::is_arch_supported(arch)) { ops = &specific::neon::RABITQ_FAST_SCAN_OPS; }
      break;
#endif
    default:
      break;
  }
  return ops;
}

} // namespace quantization
} // namespace oceanbase
