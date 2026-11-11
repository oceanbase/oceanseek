/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_simd.h"

#include <cmath>
#include <string.h>

#include "lib/alloc/alloc_assist.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace quantization
{
namespace
{

constexpr double SIMD_QUANTIZATION_EPS = 1e-5;

// ExtraBits 按每 64 维一块打包，块大小为 extra_bits * 8 字节；不足 64 维的尾块补 0。
// n = extra_bits 时，块内 lane 0～63 的布局为：
//
//   1: 8B，block[lane / 8].bit[lane % 8] = code[lane]
//   2: 16B，byte[b] 依次放入 code[b]、code[16+b]、code[32+b]、code[48+b] 各 2 bit
//   3: 前 16B 为 2-bit 布局，后 8B 为最高 1-bit plane
//   4: 32B，byte[b] 低/高 4 bit 分别为 code[b]、code[32+b]
//   5: 前 32B 使用 4-bit 布局，最后 8B 为最高 1-bit plane
//   6: 48B，lane 0～47 各占 1 byte 低 6 bit；lane 48～63 拆成 2 bit 写入前三组 byte 高 2 bit
//   7: 前 48B 为 6-bit 布局，后 8B 为最高 1-bit plane
//   8: 64B，每个 lane 直接存一个 uint8
static int64_t extra_block_size(const int8_t extra_bits)
{
  return static_cast<int64_t>(extra_bits) * 8;
}

static uint8_t encode_extra_code(const int32_t magnitude,
                                 const int32_t max_magnitude,
                                 const bool negative)
{
  const uint8_t sign_mask = negative
                                ? static_cast<uint8_t>(max_magnitude)
                                : 0;
  return static_cast<uint8_t>(magnitude) ^ sign_mask;
}

// 打包 2/4/6-bit ExtraBits。
static void pack_even_block(const uint8_t *raw,
                            const int8_t extra_bits,
                            uint8_t *block)
{
  if (2 == extra_bits) {
    for (int64_t b = 0; b < 16; ++b) {
      block[b] = static_cast<uint8_t>(
          (raw[b] & 0x03U)
          | ((raw[16 + b] & 0x03U) << 2)
          | ((raw[32 + b] & 0x03U) << 4)
          | ((raw[48 + b] & 0x03U) << 6));
    }
  } else if (4 == extra_bits) {
    for (int64_t b = 0; b < 32; ++b) {
      block[b] = static_cast<uint8_t>(
          (raw[b] & 0x0FU) | ((raw[32 + b] & 0x0FU) << 4));
    }
  } else if (6 == extra_bits) {
    for (int64_t b = 0; b < 16; ++b) {
      block[b] = static_cast<uint8_t>(
          (raw[b] & 0x3FU) | ((raw[48 + b] & 0x03U) << 6));
      block[16 + b] = static_cast<uint8_t>(
          (raw[16 + b] & 0x3FU) | (((raw[48 + b] >> 2) & 0x03U) << 6));
      block[32 + b] = static_cast<uint8_t>(
          (raw[32 + b] & 0x3FU) | (((raw[48 + b] >> 4) & 0x03U) << 6));
    }
  }
}

// 将 64 个 extra code 打成一块 ExtraBits。偶数位宽直接按上表打包；
// 奇数位宽先按 n-1 的偶数布局写入低位，再追加 8B 最高 bit plane。
static void pack_extra_block(const uint8_t *raw,
                             const int8_t extra_bits,
                             uint8_t *block)
{
  if (1 == extra_bits) {
    for (int64_t i = 0; i < 64; ++i) {
      block[i / 8] |= static_cast<uint8_t>((raw[i] & 0x01U) << (i % 8));
    }
  } else if (2 == extra_bits || 4 == extra_bits || 6 == extra_bits) {
    pack_even_block(raw, extra_bits, block);
  } else if (5 == extra_bits) {
    pack_even_block(raw, 4, block);
    for (int64_t i = 0; i < 64; ++i) {
      const uint8_t top = static_cast<uint8_t>((raw[i] >> 4) & 0x01U);
      block[32 + i % 8] |= static_cast<uint8_t>(top << (i / 8));
    }
  } else if (3 == extra_bits || 7 == extra_bits) {
    const int8_t low_bits = extra_bits - 1;
    pack_even_block(raw, low_bits, block);
    const int64_t top_offset = static_cast<int64_t>(low_bits) * 8;
    for (int64_t i = 0; i < 64; ++i) {
      const uint8_t top = static_cast<uint8_t>((raw[i] >> low_bits) & 0x01U);
      block[top_offset + i % 8] |= static_cast<uint8_t>(top << (i / 8));
    }
  } else if (8 == extra_bits) {
    MEMCPY(block, raw, 64);
  }
}

// Scalar fallback: decode one complete 64-dimension block in natural order.
// The search SIMD paths below unpack the same layout directly into registers.
template <int8_t EXTRA_BITS>
static void unpack_extra_block(const uint8_t *block, uint8_t *raw)
{
  static_assert(EXTRA_BITS >= 1 && EXTRA_BITS <= 8, "invalid extra bits");
  if constexpr (1 == EXTRA_BITS) {
    for (int64_t i = 0; i < 64; ++i) {
      raw[i] = static_cast<uint8_t>(
          (block[i / 8] >> (i % 8)) & 0x01U);
    }
  } else if constexpr (2 == EXTRA_BITS || 3 == EXTRA_BITS) {
    for (int64_t run = 0; run < 4; ++run) {
      for (int64_t b = 0; b < 16; ++b) {
        uint8_t code = static_cast<uint8_t>(
            (block[b] >> (2 * run)) & 0x03U);
        if constexpr (3 == EXTRA_BITS) {
          const int64_t lane = 16 * run + b;
          code |= static_cast<uint8_t>(
              ((block[16 + lane % 8] >> (lane / 8)) & 0x01U) << 2);
        }
        raw[16 * run + b] = code;
      }
    }
  } else if constexpr (4 == EXTRA_BITS) {
    for (int64_t b = 0; b < 32; ++b) {
      raw[b] = block[b] & 0x0FU;
      raw[32 + b] = block[b] >> 4;
    }
  } else if constexpr (5 == EXTRA_BITS) {
    for (int64_t lane = 0; lane < 64; ++lane) {
      const uint8_t low = static_cast<uint8_t>(
          (block[lane % 32] >> (4 * (lane / 32))) & 0x0FU);
      const uint8_t top = static_cast<uint8_t>(
          ((block[32 + lane % 8] >> (lane / 8)) & 0x01U) << 4);
      raw[lane] = low | top;
    }
  } else if constexpr (6 == EXTRA_BITS || 7 == EXTRA_BITS) {
    for (int64_t run = 0; run < 3; ++run) {
      for (int64_t b = 0; b < 16; ++b) {
        raw[16 * run + b] = block[16 * run + b] & 0x3FU;
      }
    }
    for (int64_t b = 0; b < 16; ++b) {
      raw[48 + b] = static_cast<uint8_t>(
          (block[b] >> 6)
          | ((block[16 + b] >> 6) << 2)
          | ((block[32 + b] >> 6) << 4));
    }
    if constexpr (7 == EXTRA_BITS) {
      for (int64_t lane = 0; lane < 64; ++lane) {
        raw[lane] |= static_cast<uint8_t>(
            ((block[48 + lane % 8] >> (lane / 8)) & 0x01U) << 6);
      }
    }
  } else {
    MEMCPY(raw, block, 64);
  }
}

static void pack_codes_impl(const float *normalized_residual,
                            const int64_t dim,
                            const int8_t extra_bits,
                            const double best_t,
                            uint8_t *packed_msb,
                            uint8_t *packed_extra,
                            float &correction)
{
  const int32_t max_magnitude = (1 << extra_bits) - 1;
  double correction_sum = 0.0;
  const int64_t block_size = extra_block_size(extra_bits);
  const int64_t block_count = dim / 64 + (dim % 64 == 0 ? 0 : 1);
  for (int64_t block_index = 0; block_index < block_count; ++block_index) {
    uint8_t raw[64] = {};
    const int64_t begin = block_index * 64;
    const int64_t end = begin + 64 < dim ? begin + 64 : dim;
    for (int64_t i = begin; i < end; ++i) {
      const float value = normalized_residual[i];
      const bool negative = value < 0.0F;
      const double absolute = std::abs(static_cast<double>(value));
      int32_t magnitude = static_cast<int32_t>(
          best_t * absolute + SIMD_QUANTIZATION_EPS);
      if (magnitude > max_magnitude) {
        magnitude = max_magnitude;
      }
      if (!negative) {
        packed_msb[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
      }
      raw[i - begin] = encode_extra_code(
          magnitude, max_magnitude, negative);
      correction_sum += absolute * (static_cast<double>(magnitude) + 0.5);
    }
    pack_extra_block(raw,
                     extra_bits,
                     packed_extra + block_index * block_size);
  }
  correction = static_cast<float>(correction_sum);
}

} // namespace

namespace specific
{
namespace normal
{

static float absolute_and_max(const float *residual, float *absolute, const int64_t dim)
{
  float maximum = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    absolute[i] = std::abs(residual[i]);
    if (absolute[i] > maximum) {
      maximum = absolute[i];
    }
  }
  return maximum;
}

static void pack_codes(const float *normalized_residual,
                       const int64_t dim,
                       const int8_t extra_bits,
                       const double best_t,
                       uint8_t *packed_msb,
                       uint8_t *packed_extra,
                       float &correction)
{
  pack_codes_impl(normalized_residual,
                  dim,
                  extra_bits,
                  best_t,
                  packed_msb,
                  packed_extra,
                  correction);
}

static float dot_msb_codes(const float *query,
                           const uint8_t *packed_msb,
                           const int64_t dim)
{
  float result = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    if (0 != (packed_msb[i / 8] & static_cast<uint8_t>(1U << (i % 8)))) {
      result += query[i];
    }
  }
  return result;
}

template <int8_t EXTRA_BITS>
static float dot_extra_codes(const float *query,
                             const uint8_t *packed_extra,
                             const int64_t dim)
{
  static_assert(EXTRA_BITS >= 1 && EXTRA_BITS <= 8, "invalid extra bits");
  float result = 0.0F;
  constexpr int64_t block_size = static_cast<int64_t>(EXTRA_BITS) * 8;
  const int64_t block_count = (dim + 63) / 64;
  for (int64_t block_index = 0; block_index < block_count; ++block_index) {
    uint8_t raw[64] = {};
    unpack_extra_block<EXTRA_BITS>(
        packed_extra + block_index * block_size, raw);
    const int64_t begin = block_index * 64;
    const int64_t end = begin + 64 < dim ? begin + 64 : dim;
    for (int64_t i = begin; i < end; ++i) {
      result += query[i] * static_cast<float>(raw[i - begin]);
    }
  }
  return result;
}

const ObExtendRaBitQSimdOps EXTEND_RABITQ_OPS = {
    absolute_and_max,
    pack_codes,
    dot_msb_codes,
    {dot_extra_codes<1>,
     dot_extra_codes<2>,
     dot_extra_codes<3>,
     dot_extra_codes<4>,
     dot_extra_codes<5>,
     dot_extra_codes<6>,
     dot_extra_codes<7>,
     dot_extra_codes<8>}};

} // namespace normal
} // namespace specific

#if OB_USE_MULTITARGET_CODE

OB_DECLARE_AVX2_SPECIFIC_CODE(

static float absolute_and_max(const float *residual, float *absolute, const int64_t dim)
{
  __m256 maxima = _mm256_setzero_ps();
  int64_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    const __m256 values = _mm256_loadu_ps(residual + i);
    const __m256 magnitudes = _mm256_andnot_ps(_mm256_set1_ps(-0.0F), values);
    _mm256_storeu_ps(absolute + i, magnitudes);
    // Keep the previous maximum for NaN inputs, matching the scalar comparison.
    maxima = _mm256_max_ps(magnitudes, maxima);
  }
  float lanes[8];
  _mm256_storeu_ps(lanes, maxima);
  float maximum = 0.0F;
  for (int64_t lane = 0; lane < 8; ++lane) {
    if (lanes[lane] > maximum) {
      maximum = lanes[lane];
    }
  }
  const float tail_maximum = specific::normal::absolute_and_max(
      residual + i, absolute + i, dim - i);
  return tail_maximum > maximum ? tail_maximum : maximum;
}

static void pack_extra_block_simd(const uint8_t *raw,
                                  const int8_t extra_bits,
                                  uint8_t *block)
{
  if (1 == extra_bits) {
    for (int64_t i = 0; i < 64; i += 32) {
      const __m256i values = _mm256_loadu_si256(
          reinterpret_cast<const __m256i *>(raw + i));
      const uint32_t bits = static_cast<uint32_t>(
          _mm256_movemask_epi8(_mm256_slli_epi16(values, 7)));
      MEMCPY(block + i / 8, &bits, sizeof(bits));
    }
  } else if (2 == extra_bits) {
    const __m128i low_mask = _mm_set1_epi8(0x03);
    const __m128i c0 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw));
    const __m128i c1 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 16));
    const __m128i c2 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 32));
    const __m128i c3 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 48));
    const __m128i packed = _mm_or_si128(
        _mm_or_si128(_mm_and_si128(c0, low_mask),
                     _mm_slli_epi16(_mm_and_si128(c1, low_mask), 2)),
        _mm_or_si128(_mm_slli_epi16(_mm_and_si128(c2, low_mask), 4),
                     _mm_slli_epi16(_mm_and_si128(c3, low_mask), 6)));
    _mm_storeu_si128(reinterpret_cast<__m128i *>(block), packed);
  } else if (4 == extra_bits || 5 == extra_bits) {
    const __m128i low_mask = _mm_set1_epi8(0x0f);
    const __m128i c0 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw));
    const __m128i c1 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 16));
    const __m128i c2 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 32));
    const __m128i c3 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 48));
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(block),
        _mm_or_si128(_mm_and_si128(c0, low_mask),
                     _mm_slli_epi16(_mm_and_si128(c2, low_mask), 4)));
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(block + 16),
        _mm_or_si128(_mm_and_si128(c1, low_mask),
                     _mm_slli_epi16(_mm_and_si128(c3, low_mask), 4)));
    if (5 == extra_bits) {
      for (int64_t i = 0; i < 64; ++i) {
        const uint8_t top = static_cast<uint8_t>((raw[i] >> 4) & 0x01U);
        block[32 + i % 8] |= static_cast<uint8_t>(top << (i / 8));
      }
    }
  } else if (6 == extra_bits) {
    const __m128i low_mask = _mm_set1_epi8(0x3f);
    const __m128i pair_mask = _mm_set1_epi8(0x03);
    const __m128i c0 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw));
    const __m128i c1 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 16));
    const __m128i c2 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 32));
    const __m128i c3 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(raw + 48));
    const __m128i high0 = _mm_slli_epi16(
        _mm_and_si128(c3, pair_mask), 6);
    const __m128i high1 = _mm_slli_epi16(
        _mm_and_si128(_mm_srli_epi16(c3, 2), pair_mask), 6);
    const __m128i high2 = _mm_slli_epi16(
        _mm_and_si128(_mm_srli_epi16(c3, 4), pair_mask), 6);
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(block),
        _mm_or_si128(_mm_and_si128(c0, low_mask), high0));
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(block + 16),
        _mm_or_si128(_mm_and_si128(c1, low_mask), high1));
    _mm_storeu_si128(
        reinterpret_cast<__m128i *>(block + 32),
        _mm_or_si128(_mm_and_si128(c2, low_mask), high2));
  } else if (3 == extra_bits || 7 == extra_bits) {
    const int8_t low_bits = extra_bits - 1;
    pack_extra_block_simd(raw, low_bits, block);
    const int64_t top_offset = static_cast<int64_t>(low_bits) * 8;
    for (int64_t i = 0; i < 64; ++i) {
      const uint8_t top = static_cast<uint8_t>((raw[i] >> low_bits) & 0x01U);
      block[top_offset + i % 8] |= static_cast<uint8_t>(top << (i / 8));
    }
  } else if (8 == extra_bits) {
    _mm256_storeu_si256(
        reinterpret_cast<__m256i *>(block),
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(raw)));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i *>(block + 32),
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(raw + 32)));
  }
}

static uint64_t load_u64(const uint8_t *data)
{
  uint64_t value = 0;
  MEMCPY(&value, data, sizeof(value));
  return value;
}

static uint64_t shift_plane(const uint64_t plane,
                            const int64_t from_bit,
                            const int64_t to_bit)
{
  return from_bit >= to_bit
             ? plane >> (from_bit - to_bit)
             : plane << (to_bit - from_bit);
}

static __m128i top_plane_run(const uint64_t plane,
                             const int64_t run,
                             const int64_t top_bit)
{
  const uint64_t low = shift_plane(plane, 2 * run, top_bit);
  const uint64_t high = shift_plane(plane, 2 * run + 1, top_bit);
  return _mm_and_si128(
      _mm_set_epi64x(static_cast<int64_t>(high), static_cast<int64_t>(low)),
      _mm_set1_epi8(static_cast<char>(1U << top_bit)));
}

// Unpack one width-specific 64-dimension block into four natural-order
// 16-byte runs. Callers widen these byte lanes and fuse the dot product.
template <int8_t EXTRA_BITS>
static void unpack_extra_block_simd(const uint8_t *block, __m128i *runs)
{
  static_assert(EXTRA_BITS >= 1 && EXTRA_BITS <= 8, "invalid extra bits");
  if constexpr (1 == EXTRA_BITS) {
    constexpr uint64_t BYTE_SPLAT = 0x0101010101010101ULL;
    const __m128i bit_select =
        _mm_set1_epi64x(static_cast<int64_t>(0x8040201008040201ULL));
    const __m128i one = _mm_set1_epi8(1);
    for (int64_t run = 0; run < 4; ++run) {
      const uint64_t low = static_cast<uint64_t>(block[2 * run]) * BYTE_SPLAT;
      const uint64_t high =
          static_cast<uint64_t>(block[2 * run + 1]) * BYTE_SPLAT;
      const __m128i bytes = _mm_set_epi64x(
          static_cast<int64_t>(high), static_cast<int64_t>(low));
      const __m128i selected = _mm_cmpeq_epi8(
          _mm_and_si128(bytes, bit_select), bit_select);
      runs[run] = _mm_and_si128(selected, one);
    }
  } else if constexpr (2 == EXTRA_BITS || 3 == EXTRA_BITS) {
    const __m128i packed = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(block));
    const __m128i mask = _mm_set1_epi8(0x03);
    runs[0] = _mm_and_si128(packed, mask);
    runs[1] = _mm_and_si128(_mm_srli_epi16(packed, 2), mask);
    runs[2] = _mm_and_si128(_mm_srli_epi16(packed, 4), mask);
    runs[3] = _mm_and_si128(_mm_srli_epi16(packed, 6), mask);
    if constexpr (3 == EXTRA_BITS) {
      const uint64_t plane = load_u64(block + 16);
      for (int64_t run = 0; run < 4; ++run) {
        runs[run] = _mm_or_si128(runs[run], top_plane_run(plane, run, 2));
      }
    }
  } else if constexpr (4 == EXTRA_BITS || 5 == EXTRA_BITS) {
    const __m256i packed = _mm256_loadu_si256(
        reinterpret_cast<const __m256i *>(block));
    const __m256i mask = _mm256_set1_epi8(0x0f);
    const __m256i lo = _mm256_and_si256(packed, mask);
    const __m256i hi = _mm256_and_si256(
        _mm256_srli_epi16(packed, 4), mask);
    runs[0] = _mm256_castsi256_si128(lo);
    runs[1] = _mm256_extracti128_si256(lo, 1);
    runs[2] = _mm256_castsi256_si128(hi);
    runs[3] = _mm256_extracti128_si256(hi, 1);
    if constexpr (5 == EXTRA_BITS) {
      const uint64_t plane = load_u64(block + 32);
      for (int64_t run = 0; run < 4; ++run) {
        runs[run] = _mm_or_si128(runs[run], top_plane_run(plane, run, 4));
      }
    }
  } else if constexpr (6 == EXTRA_BITS || 7 == EXTRA_BITS) {
    const __m128i packed0 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(block));
    const __m128i packed1 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(block + 16));
    const __m128i packed2 = _mm_loadu_si128(
        reinterpret_cast<const __m128i *>(block + 32));
    const __m128i mask6 = _mm_set1_epi8(0x3f);
    const __m128i mask2 = _mm_set1_epi8(static_cast<char>(0xc0));
    runs[0] = _mm_and_si128(packed0, mask6);
    runs[1] = _mm_and_si128(packed1, mask6);
    runs[2] = _mm_and_si128(packed2, mask6);
    runs[3] = _mm_or_si128(
        _mm_or_si128(
            _mm_srli_epi16(_mm_and_si128(packed0, mask2), 6),
            _mm_srli_epi16(_mm_and_si128(packed1, mask2), 4)),
        _mm_srli_epi16(_mm_and_si128(packed2, mask2), 2));
    if constexpr (7 == EXTRA_BITS) {
      const uint64_t plane = load_u64(block + 48);
      for (int64_t run = 0; run < 4; ++run) {
        runs[run] = _mm_or_si128(runs[run], top_plane_run(plane, run, 6));
      }
    }
  } else {
    for (int64_t run = 0; run < 4; ++run) {
      runs[run] = _mm_loadu_si128(
          reinterpret_cast<const __m128i *>(block + run * 16));
    }
  }
}

static void fma_16_codes(const __m128i codes,
                         const float *query,
                         __m256 *accumulators)
{
  const __m256 low = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(codes));
  accumulators[0] = _mm256_fmadd_ps(
      low, _mm256_loadu_ps(query), accumulators[0]);
  const __m128i high_codes = _mm_srli_si128(codes, 8);
  const __m256 high = _mm256_cvtepi32_ps(
      _mm256_cvtepu8_epi32(high_codes));
  accumulators[1] = _mm256_fmadd_ps(
      high, _mm256_loadu_ps(query + 8), accumulators[1]);
}

static void pack_codes(const float *normalized_residual,
                       const int64_t dim,
                       const int8_t extra_bits,
                       const double best_t,
                       uint8_t *packed_msb,
                       uint8_t *packed_extra,
                       float &correction)
{
  const int32_t max_magnitude = (1 << extra_bits) - 1;
  const __m256d scale = _mm256_set1_pd(best_t);
  const __m256d eps = _mm256_set1_pd(SIMD_QUANTIZATION_EPS);
  const __m128 sign = _mm_set1_ps(-0.0F);
  const __m128i maximum = _mm_set1_epi32(max_magnitude);
  double correction_sum = 0.0;
  const int64_t block_size = extra_block_size(extra_bits);
  const int64_t block_count = dim / 64 + (dim % 64 == 0 ? 0 : 1);
  for (int64_t block_index = 0; block_index < block_count; ++block_index) {
    uint8_t raw[64] = {};
    const int64_t begin = block_index * 64;
    const int64_t end = begin + 64 < dim ? begin + 64 : dim;
    int64_t i = begin;
    for (; end - i >= 4; i += 4) {
      const __m128 values = _mm_loadu_ps(normalized_residual + i);
      const __m256d absolutes = _mm256_cvtps_pd(_mm_andnot_ps(sign, values));
      const __m128i magnitudes = _mm_min_epi32(
          _mm256_cvttpd_epi32(
              _mm256_add_pd(_mm256_mul_pd(absolutes, scale), eps)),
          maximum);
      int32_t lanes[4];
      _mm_storeu_si128(reinterpret_cast<__m128i *>(lanes), magnitudes);
      for (int64_t lane = 0; lane < 4; ++lane) {
        const float value = normalized_residual[i + lane];
        const bool negative = value < 0.0F;
        const int32_t magnitude = lanes[lane];
        if (!negative) {
          packed_msb[(i + lane) / 8] |=
              static_cast<uint8_t>(1U << ((i + lane) % 8));
        }
        raw[i + lane - begin] = encode_extra_code(
            magnitude, max_magnitude, negative);
        correction_sum += std::abs(static_cast<double>(value))
                          * (static_cast<double>(magnitude) + 0.5);
      }
    }
    for (; i < end; ++i) {
      const float value = normalized_residual[i];
      const bool negative = value < 0.0F;
      const double absolute = std::abs(static_cast<double>(value));
      int32_t magnitude =
          static_cast<int32_t>(best_t * absolute + SIMD_QUANTIZATION_EPS);
      if (magnitude > max_magnitude) {
        magnitude = max_magnitude;
      }
      if (!negative) {
        packed_msb[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
      }
      raw[i - begin] = encode_extra_code(
          magnitude, max_magnitude, negative);
      correction_sum += absolute * (static_cast<double>(magnitude) + 0.5);
    }
    pack_extra_block_simd(
        raw, extra_bits, packed_extra + block_index * block_size);
  }
  correction = static_cast<float>(correction_sum);
}

static float horizontal_sum(const __m256 values)
{
  const __m128 halves =
      _mm_add_ps(_mm256_castps256_ps128(values),
                 _mm256_extractf128_ps(values, 1));
  const __m128 pairs = _mm_hadd_ps(halves, halves);
  return _mm_cvtss_f32(_mm_hadd_ps(pairs, pairs));
}

static float dot_msb_codes(const float *query,
                           const uint8_t *packed_msb,
                           const int64_t dim)
{
  const __m256i lane_masks =
      _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08,
                        0x10, 0x20, 0x40, 0x80);
  const __m256i zero = _mm256_setzero_si256();
  __m256 accumulator = _mm256_setzero_ps();
  int64_t i = 0;
  for (; dim - i >= 8; i += 8) {
    const __m256i packed =
        _mm256_set1_epi32(static_cast<int32_t>(packed_msb[i / 8]));
    const __m256i enabled =
        _mm256_cmpgt_epi32(_mm256_and_si256(packed, lane_masks), zero);
    const __m256 selected =
        _mm256_and_ps(_mm256_loadu_ps(query + i), _mm256_castsi256_ps(enabled));
    accumulator = _mm256_add_ps(accumulator, selected);
  }
  return horizontal_sum(accumulator)
         + specific::normal::dot_msb_codes(
               query + i, packed_msb + i / 8, dim - i);
}

template <int8_t EXTRA_BITS>
static float dot_extra_codes(const float *query,
                             const uint8_t *packed_extra,
                             const int64_t dim)
{
  static_assert(EXTRA_BITS >= 1 && EXTRA_BITS <= 8, "invalid extra bits");
  __m256 dot_accumulators[2] = {
      _mm256_setzero_ps(), _mm256_setzero_ps()};
  constexpr int64_t block_size = static_cast<int64_t>(EXTRA_BITS) * 8;
  const int64_t simd_dim = dim / 64 * 64;
  int64_t i = 0;
  for (; i < simd_dim; i += 64) {
    __m128i runs[4];
    unpack_extra_block_simd<EXTRA_BITS>(
        packed_extra + i / 64 * block_size, runs);
    for (int64_t run = 0; run < 4; ++run) {
      const float *run_query = query + i + run * 16;
      fma_16_codes(runs[run], run_query, dot_accumulators);
    }
  }
  const float tail_dot = specific::normal::dot_extra_codes<EXTRA_BITS>(
      query + i,
      packed_extra + i / 64 * block_size,
      dim - i);
  return horizontal_sum(_mm256_add_ps(
             dot_accumulators[0], dot_accumulators[1]))
         + tail_dot;
}

static const ObExtendRaBitQSimdOps EXTEND_RABITQ_OPS = {
    absolute_and_max,
    pack_codes,
    dot_msb_codes,
    {dot_extra_codes<1>,
     dot_extra_codes<2>,
     dot_extra_codes<3>,
     dot_extra_codes<4>,
     dot_extra_codes<5>,
     dot_extra_codes<6>,
     dot_extra_codes<7>,
     dot_extra_codes<8>}};

)

OB_DECLARE_AVX512_SPECIFIC_CODE(

static float absolute_and_max(const float *residual, float *absolute, const int64_t dim)
{
  __m512 maxima = _mm512_setzero_ps();
  int64_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    const __m512 values = _mm512_loadu_ps(residual + i);
    const __m512 magnitudes = _mm512_castsi512_ps(_mm512_and_si512(
        _mm512_castps_si512(values), _mm512_set1_epi32(0x7FFFFFFF)));
    _mm512_storeu_ps(absolute + i, magnitudes);
    // Keep the previous maximum for NaN inputs, matching the scalar comparison.
    maxima = _mm512_max_ps(magnitudes, maxima);
  }
  float lanes[16];
  _mm512_storeu_ps(lanes, maxima);
  float maximum = 0.0F;
  for (int64_t lane = 0; lane < 16; ++lane) {
    if (lanes[lane] > maximum) {
      maximum = lanes[lane];
    }
  }
  const float tail_maximum = specific::normal::absolute_and_max(
      residual + i, absolute + i, dim - i);
  return tail_maximum > maximum ? tail_maximum : maximum;
}

static void pack_codes(const float *normalized_residual,
                       const int64_t dim,
                       const int8_t extra_bits,
                       const double best_t,
                       uint8_t *packed_msb,
                       uint8_t *packed_extra,
                       float &correction)
{
  const int32_t max_magnitude = (1 << extra_bits) - 1;
  const __m512d scale = _mm512_set1_pd(best_t);
  const __m512d eps = _mm512_set1_pd(SIMD_QUANTIZATION_EPS);
  const __m256 sign = _mm256_set1_ps(-0.0F);
  const __m256i maximum = _mm256_set1_epi32(max_magnitude);
  double correction_sum = 0.0;
  const int64_t block_size = extra_block_size(extra_bits);
  const int64_t block_count = dim / 64 + (dim % 64 == 0 ? 0 : 1);
  for (int64_t block_index = 0; block_index < block_count; ++block_index) {
    uint8_t raw[64] = {};
    const int64_t begin = block_index * 64;
    const int64_t end = begin + 64 < dim ? begin + 64 : dim;
    int64_t i = begin;
    for (; end - i >= 8; i += 8) {
      const __m256 values = _mm256_loadu_ps(normalized_residual + i);
      const __m512d absolutes =
          _mm512_cvtps_pd(_mm256_andnot_ps(sign, values));
      const __m256i magnitudes = _mm256_min_epi32(
          _mm512_cvttpd_epi32(
              _mm512_add_pd(_mm512_mul_pd(absolutes, scale), eps)),
          maximum);
      // Preserve value < 0 semantics, including -0.0F, while packing eight sign bits.
      const __m256 negative_lanes =
          _mm256_cmp_ps(values, _mm256_setzero_ps(), _CMP_LT_OQ);
      packed_msb[i / 8] |= static_cast<uint8_t>(~_mm256_movemask_ps(negative_lanes));
      const __m256i codes = _mm256_xor_si256(magnitudes,
          _mm256_and_si256(_mm256_castps_si256(negative_lanes), maximum));
      const __m128i codes16 = _mm_packus_epi32(
          _mm256_castsi256_si128(codes), _mm256_extracti128_si256(codes, 1));
      const __m128i codes8 = _mm_packus_epi16(codes16, _mm_setzero_si128());
      _mm_storel_epi64(reinterpret_cast<__m128i *>(raw + (i - begin)), codes8);
      int32_t lanes[8];
      _mm256_storeu_si256(reinterpret_cast<__m256i *>(lanes), magnitudes);
      for (int64_t lane = 0; lane < 8; ++lane) {
        const float value = normalized_residual[i + lane];
        const int32_t magnitude = lanes[lane];
        correction_sum += std::abs(static_cast<double>(value))
                          * (static_cast<double>(magnitude) + 0.5);
      }
    }
    for (; i < end; ++i) {
      const float value = normalized_residual[i];
      const bool negative = value < 0.0F;
      const double absolute = std::abs(static_cast<double>(value));
      int32_t magnitude =
          static_cast<int32_t>(best_t * absolute + SIMD_QUANTIZATION_EPS);
      if (magnitude > max_magnitude) {
        magnitude = max_magnitude;
      }
      if (!negative) {
        packed_msb[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
      }
      raw[i - begin] = encode_extra_code(
          magnitude, max_magnitude, negative);
      correction_sum += absolute * (static_cast<double>(magnitude) + 0.5);
    }
    specific::avx2::pack_extra_block_simd(
        raw, extra_bits, packed_extra + block_index * block_size);
  }
  correction = static_cast<float>(correction_sum);
}

static float dot_msb_codes(const float *query,
                           const uint8_t *packed_msb,
                           const int64_t dim)
{
  __m512 accumulator = _mm512_setzero_ps();
  int64_t i = 0;
  for (; dim - i >= 16; i += 16) {
    const uint16_t bits =
        static_cast<uint16_t>(packed_msb[i / 8])
        | static_cast<uint16_t>(
              static_cast<uint16_t>(packed_msb[i / 8 + 1]) << 8);
    const __m512 query_values = _mm512_loadu_ps(query + i);
    accumulator = _mm512_add_ps(
        accumulator,
        _mm512_maskz_mov_ps(static_cast<__mmask16>(bits), query_values));
  }
  return _mm512_reduce_add_ps(accumulator)
         + specific::avx2::dot_msb_codes(
               query + i, packed_msb + i / 8, dim - i);
}

template <int8_t EXTRA_BITS>
static float dot_extra_codes(const float *query,
                             const uint8_t *packed_extra,
                             const int64_t dim)
{
  static_assert(EXTRA_BITS >= 1 && EXTRA_BITS <= 8, "invalid extra bits");
  __m512 dot_accumulators[2] = {
      _mm512_setzero_ps(), _mm512_setzero_ps()};
  constexpr int64_t block_size = static_cast<int64_t>(EXTRA_BITS) * 8;
  const int64_t simd_dim = dim / 64 * 64;
  int64_t i = 0;
  for (; i < simd_dim; i += 64) {
    __m128i code_lanes[4];
    specific::avx2::unpack_extra_block_simd<EXTRA_BITS>(
        packed_extra + i / 64 * block_size, code_lanes);
    for (int64_t run = 0; run < 4; ++run) {
      const __m512 codes = _mm512_cvtepi32_ps(
          _mm512_cvtepu8_epi32(code_lanes[run]));
      const __m512 query_values = _mm512_loadu_ps(query + i + run * 16);
      dot_accumulators[(i / 64 + run) % 2] = _mm512_fmadd_ps(
          query_values, codes, dot_accumulators[(i / 64 + run) % 2]);
    }
  }
  const float tail_dot = specific::normal::dot_extra_codes<EXTRA_BITS>(
      query + i,
      packed_extra + i / 64 * block_size,
      dim - i);
  return _mm512_reduce_add_ps(
             _mm512_add_ps(dot_accumulators[0], dot_accumulators[1]))
         + tail_dot;
}

static const ObExtendRaBitQSimdOps EXTEND_RABITQ_OPS = {
    absolute_and_max,
    pack_codes,
    dot_msb_codes,
    {dot_extra_codes<1>,
     dot_extra_codes<2>,
     dot_extra_codes<3>,
     dot_extra_codes<4>,
     dot_extra_codes<5>,
     dot_extra_codes<6>,
     dot_extra_codes<7>,
     dot_extra_codes<8>}};

)

#endif // OB_USE_MULTITARGET_CODE

const ObExtendRaBitQSimdOps &get_extend_rabitq_simd_ops()
{
#if OB_USE_MULTITARGET_CODE
  const ObExtendRaBitQSimdOps *ops =
      get_extend_rabitq_simd_ops(common::ObTargetArch::AVX512);
  if (nullptr == ops) {
    ops = get_extend_rabitq_simd_ops(common::ObTargetArch::AVX2);
  }
  if (nullptr != ops) {
    return *ops;
  }
#endif
  return specific::normal::EXTEND_RABITQ_OPS;
}

const ObExtendRaBitQSimdOps *get_extend_rabitq_simd_ops(
    const common::ObTargetArch arch)
{
  const ObExtendRaBitQSimdOps *ops = nullptr;
  switch (arch) {
    case common::ObTargetArch::Default:
      ops = &specific::normal::EXTEND_RABITQ_OPS;
      break;
#if OB_USE_MULTITARGET_CODE
    case common::ObTargetArch::AVX2:
      if (common::is_arch_supported(arch)) {
        ops = &specific::avx2::EXTEND_RABITQ_OPS;
      }
      break;
    case common::ObTargetArch::AVX512:
      if (common::is_arch_supported(common::ObTargetArch::AVX512)
          && common::is_arch_supported(common::ObTargetArch::AVX2)) {
        ops = &specific::avx512::EXTEND_RABITQ_OPS;
      }
      break;
#endif
    default:
      break;
  }
  return ops;
}

} // namespace quantization
} // namespace oceanbase
