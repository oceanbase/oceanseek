/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <random>
#include <stdint.h>
#include <string.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common/ob_target_specific.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_quantizer.h"
#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_simd.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"

namespace oceanbase
{
namespace quantization
{
namespace
{

class AlignedBuffer
{
public:
  explicit AlignedBuffer(const int64_t size)
      : storage_((size + static_cast<int64_t>(sizeof(uint64_t)) - 1)
                 / static_cast<int64_t>(sizeof(uint64_t)), 0),
        size_(size)
  {}

  uint8_t *data() { return reinterpret_cast<uint8_t *>(storage_.data()); }
  const uint8_t *data() const
  {
    return reinterpret_cast<const uint8_t *>(storage_.data());
  }
  int64_t size() const { return size_; }

private:
  std::vector<uint64_t> storage_;
  int64_t size_;
};

struct TestLayout
{
  int64_t msb_size_;
  int64_t padded_dim_;
  int64_t extra_offset_;
  int64_t extra_size_;
  int64_t residual_norm_offset_;
  int64_t raw_norm_offset_;
  int64_t rescale_offset_;
  int64_t size_;
};

int64_t align_up(const int64_t value, const int64_t alignment)
{
  return (value + alignment - 1) / alignment * alignment;
}

TestLayout test_layout(const int64_t dim, const int8_t bits)
{
  TestLayout layout = {};
  layout.msb_size_ = (dim + 7) / 8;
  layout.padded_dim_ = align_up(dim, 64);
  layout.extra_offset_ = layout.msb_size_;
  layout.extra_size_ = layout.padded_dim_ * (bits - 1) / 8;
  layout.residual_norm_offset_ = align_up(
      layout.extra_offset_ + layout.extra_size_, sizeof(float));
  layout.raw_norm_offset_ = layout.residual_norm_offset_ + sizeof(float);
  layout.rescale_offset_ = layout.raw_norm_offset_ + sizeof(float);
  layout.size_ = layout.rescale_offset_ + sizeof(float);
  return layout;
}

float load_float(const uint8_t *data)
{
  float value = 0.0F;
  memcpy(&value, data, sizeof(value));
  return value;
}

void store_float(uint8_t *data, const float value)
{
  memcpy(data, &value, sizeof(value));
}

uint8_t decode_extra(const uint8_t *packed,
                     const int8_t extra_bits,
                     const int64_t index)
{
  const int64_t block_size = static_cast<int64_t>(extra_bits) * 8;
  const uint8_t *block = packed + index / 64 * block_size;
  const int64_t lane = index % 64;
  uint8_t code = 0;
  if (1 == extra_bits) {
    code = static_cast<uint8_t>((block[lane / 8] >> (lane % 8)) & 1U);
  } else if (2 == extra_bits || 3 == extra_bits) {
    const int64_t byte_index = lane % 16;
    code = static_cast<uint8_t>(
        (block[byte_index] >> (2 * (lane / 16))) & 0x03U);
    if (3 == extra_bits) {
      code |= static_cast<uint8_t>(
          ((block[16 + lane % 8] >> (lane / 8)) & 1U) << 2);
    }
  } else if (4 == extra_bits) {
    code = static_cast<uint8_t>(
        (block[lane % 32] >> (4 * (lane / 32))) & 0x0FU);
  } else if (5 == extra_bits) {
    code = static_cast<uint8_t>(
        (block[lane % 32] >> (4 * (lane / 32))) & 0x0FU);
    code |= static_cast<uint8_t>(
        ((block[32 + lane % 8] >> (lane / 8)) & 1U) << 4);
  } else if (6 == extra_bits || 7 == extra_bits) {
    if (lane < 48) {
      code = static_cast<uint8_t>(block[lane] & 0x3FU);
    } else {
      const int64_t b = lane - 48;
      code = static_cast<uint8_t>(
          ((block[b] >> 6) & 0x03U)
          | (((block[16 + b] >> 6) & 0x03U) << 2)
          | (((block[32 + b] >> 6) & 0x03U) << 4));
    }
    if (7 == extra_bits) {
      code |= static_cast<uint8_t>(
          ((block[48 + lane % 8] >> (lane / 8)) & 1U) << 6);
    }
  } else if (8 == extra_bits) {
    code = block[lane];
  }
  return code;
}

std::vector<float> normalized_residual(const std::vector<float> &vector,
                                       const std::vector<float> &centroid,
                                       float &norm)
{
  std::vector<float> residual(vector.size());
  float norm2 = 0.0F;
  for (int64_t i = 0; i < static_cast<int64_t>(vector.size()); ++i) {
    residual[i] = vector[i] - centroid[i];
    norm2 += residual[i] * residual[i];
  }
  norm = std::sqrt(norm2);
  if (norm > 0.0F) {
    for (float &value : residual) {
      value /= norm;
    }
  }
  return residual;
}

float prototype_best_t(const std::vector<float> &normalized,
                       const int8_t extra_bits)
{
  constexpr float EPS = 1e-5F;
  constexpr float TIGHT_START[9] = {
      0.0F, 0.15F, 0.20F, 0.52F, 0.59F, 0.71F, 0.75F, 0.77F, 0.81F};
  const int32_t maximum = (1 << extra_bits) - 1;
  float max_value = -1.0F;
  for (const float value : normalized) {
    max_value = std::max(max_value, std::abs(value));
  }
  const float t_end = static_cast<float>(maximum + 10) / max_value;
  const float t_start = t_end * TIGHT_START[extra_bits];
  std::vector<int32_t> current(normalized.size());
  float squared_denominator = normalized.size() * 0.25F;
  float numerator = 0.0F;
  using Event = std::pair<float, int64_t>;
  std::priority_queue<Event, std::vector<Event>, std::greater<Event>> events;
  for (int64_t i = 0; i < static_cast<int64_t>(normalized.size()); ++i) {
    const float absolute = std::abs(normalized[i]);
    current[i] = static_cast<int32_t>(
        std::floor(t_start * absolute + EPS));
    current[i] = std::max(0, std::min(current[i], maximum));
    squared_denominator += current[i] * current[i] + current[i];
    numerator += (current[i] + 0.5) * absolute;
    if (absolute > 0.0 && current[i] < maximum) {
      const float next_t = static_cast<float>(current[i] + 1) / absolute;
      if (next_t < t_end) {
        events.emplace(next_t, i);
      }
    }
  }
  float max_ip = numerator / std::sqrt(squared_denominator);
  float best_t = t_start;
  while (!events.empty()) {
    const float current_t = events.top().first;
    const int64_t index = events.top().second;
    events.pop();
    ++current[index];
    squared_denominator += 2 * current[index];
    numerator += std::abs(normalized[index]);
    const float current_ip = numerator / std::sqrt(squared_denominator);
    if (current_ip > max_ip) {
      max_ip = current_ip;
      best_t = current_t;
    }
    if (current[index] < maximum) {
      const float absolute = std::abs(normalized[index]);
      const float next_t = static_cast<float>(current[index] + 1) / absolute;
      if (next_t < t_end) {
        events.emplace(next_t, index);
      }
    }
  }
  return best_t;
}

int32_t prototype_magnitude(const float value,
                            const int8_t extra_bits,
                            const float best_t)
{
  const int32_t maximum = (1 << extra_bits) - 1;
  int32_t magnitude = static_cast<int32_t>(
      best_t * std::abs(value) + 1e-5F);
  return std::min(magnitude, maximum);
}

float tolerance(const float expected)
{
  return 2e-5F * std::max(1.0F, std::abs(expected));
}

float accumulation_tolerance(const float expected,
                             const double absolute_sum)
{
  return tolerance(expected)
         + static_cast<float>(4.0 * std::numeric_limits<float>::epsilon()
                              * absolute_sum);
}

float vector_norm(const std::vector<float> &vector)
{
  float squared_norm = 0.0F;
  for (const float value : vector) {
    squared_norm += value * value;
  }
  return std::sqrt(squared_norm);
}

float scalar_metric_from_code(
    const share::ObVectorIndexDistAlgorithm metric,
    const int8_t bits,
    const std::vector<float> &base,
    const std::vector<float> &query,
    const std::vector<float> &centroid,
    const uint8_t *base_code)
{
  const int64_t dim = static_cast<int64_t>(base.size());
  const int8_t extra_bits = bits - 1;
  const TestLayout layout = test_layout(dim, bits);
  float query_residual_norm = 0.0F;
  const std::vector<float> normalized_query =
      normalized_residual(query, centroid, query_residual_norm);
  float base_residual_norm = 0.0F;
  normalized_residual(base, centroid, base_residual_norm);

  float ip_msb = 0.0F;
  float ip_extra = 0.0F;
  float sumq = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    const float query_value = normalized_query[i];
    if (0 != (base_code[i / 8]
              & static_cast<uint8_t>(1U << (i % 8)))) {
      ip_msb += query_value;
    }
    ip_extra += query_value
                * decode_extra(base_code + layout.extra_offset_,
                               extra_bits,
                               i);
    sumq += query_value;
  }

  const float factor = static_cast<float>(1U << extra_bits);
  const float nbits_rescale =
      load_float(base_code + layout.rescale_offset_);
  const float normalized_dot =
      (factor * ip_msb + ip_extra - (factor - 0.5F) * sumq)
      * nbits_rescale;
  const float query_raw_norm = vector_norm(query);
  const float base_raw_norm = vector_norm(base);
  const float estimated_l2_squared =
      base_residual_norm * base_residual_norm
      + query_residual_norm * query_residual_norm
      - 2.0F * base_residual_norm * query_residual_norm * normalized_dot;
  float result = estimated_l2_squared;
  if (share::VIDA_IP == metric) {
    result = 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm
                     - estimated_l2_squared) * 0.5F;
  } else if (share::VIDA_COS == metric) {
    result = 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm
                     - estimated_l2_squared)
                        * 0.5F / (query_raw_norm * base_raw_norm);
  }
  return result;
}

} // namespace

TEST(TestExtendRaBitQQuantizer, LayoutSizingAndBitsValidation)
{
  const int64_t dimensions[] = {1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 768};
  for (int8_t bits = 2; bits <= 9; ++bits) {
    for (const int64_t dim : dimensions) {
      const ObQuantizationParams params = {dim, bits, {}, calc_inv_sqrt_dim(dim)};
      const TestLayout layout = test_layout(dim, bits);
      int64_t code_size = 0;
      int64_t query_size = 0;
      int64_t scratch_size = 0;
      EXPECT_EQ(OB_SUCCESS,
                ObExtendRaBitQQuantizer::calc_code_size(params, code_size));
      EXPECT_EQ(layout.size_, code_size);
      EXPECT_EQ(OB_SUCCESS,
                ObExtendRaBitQQuantizer::calc_query_code_size(params, query_size));
      EXPECT_EQ((dim + 3) * static_cast<int64_t>(sizeof(float)), query_size);
      EXPECT_EQ(OB_SUCCESS,
                ObExtendRaBitQQuantizer::calc_encode_scratch_size(
                    params, scratch_size));
      const int64_t normalized_bytes = dim * sizeof(float);
      const int64_t absolute_offset = normalized_bytes;
      const int64_t current_magnitude_offset =
          absolute_offset + normalized_bytes;
      const int64_t event_offset = align_up(
          current_magnitude_offset + dim * sizeof(uint8_t),
          sizeof(uint64_t));
      const int64_t event_capacity =
          dim * ((1LL << (bits - 1)) - 1);
      const int64_t radix_scratch_offset =
          event_offset + event_capacity * sizeof(uint64_t);
      const int64_t expected_scratch_size =
          radix_scratch_offset + event_capacity * sizeof(uint64_t)
          + (event_capacity <= 1024 ? 4 * 256 : 2048 + 2048 + 1024) * sizeof(int64_t);
      EXPECT_EQ(expected_scratch_size, scratch_size);
    }
  }

  for (const int8_t bits : {static_cast<int8_t>(1), static_cast<int8_t>(10)}) {
    const ObQuantizationParams params = {64, bits, {}, calc_inv_sqrt_dim(64)};
    int64_t size = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_code_size(params, size));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_query_code_size(params, size));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_encode_scratch_size(params, size));
  }

  for (const int64_t dim : {0LL, -1LL}) {
    const ObQuantizationParams params = {dim, 4, {}, 0.0F};
    int64_t size = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_code_size(params, size));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_query_code_size(params, size));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObExtendRaBitQQuantizer::calc_encode_scratch_size(params, size));
  }

}

TEST(TestExtendRaBitQQuantizer, PrepareQueryCachesNormalizedResidualSum)
{
  const int64_t dim = 65;
  const int8_t bits = 4;
  const ObQuantizationParams params = {dim, bits, {}, 0.0F};
  const ObQuantizerMetricOps *ops =
      ObExtendRaBitQQuantizer::get_ops().metric_ops[share::VIDA_L2];
  ASSERT_NE(nullptr, ops);

  std::vector<float> query(dim);
  std::vector<float> centroid(dim);
  for (int64_t i = 0; i < dim; ++i) {
    query[i] = std::sin(static_cast<float>(i) * 0.17F)
               + static_cast<float>(i % 5) * 0.03F;
    centroid[i] = static_cast<float>((i * 7) % 11 - 5) * 0.02F;
  }

  int64_t query_size = 0;
  ASSERT_EQ(OB_SUCCESS, ops->calc_query_code_size(params, query_size));
  AlignedBuffer query_code(query_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               query.data(),
                               centroid.data(),
                               {query_code.data(), query_code.size()}));

  float residual_norm = 0.0F;
  const std::vector<float> normalized_query =
      normalized_residual(query, centroid, residual_norm);
  float expected_sumq = 0.0F;
  for (const float value : normalized_query) {
    expected_sumq += value;
  }
  const int64_t sumq_offset = (dim + 2) * sizeof(float);
  EXPECT_FLOAT_EQ(expected_sumq,
                  load_float(query_code.data() + sumq_offset));
}

TEST(TestExtendRaBitQQuantizer, EncodingMatchesPrototypeAndConfiguredLayout)
{
  const int64_t dim = 65;
  std::vector<float> vector(dim);
  std::vector<float> centroid(dim);
  for (int64_t i = 0; i < dim; ++i) {
    vector[i] = static_cast<float>((i * 17) % 29 - 14) * 0.13F;
    centroid[i] = static_cast<float>((i * 7) % 11 - 5) * 0.03F;
  }
  vector[5] = centroid[5];
  for (int8_t bits = 2; bits <= 9; ++bits) {
    const int8_t extra_bits = bits - 1;
    const int32_t maximum = (1 << extra_bits) - 1;
    const ObQuantizationParams params = {dim, bits, {}, calc_inv_sqrt_dim(dim)};
    const ObQuantizerMetricOps *ops =
        ObExtendRaBitQQuantizer::get_ops().metric_ops[share::VIDA_L2];
    ASSERT_NE(nullptr, ops);
    int64_t code_size = 0;
    int64_t scratch_size = 0;
    ASSERT_EQ(OB_SUCCESS, ops->calc_code_size(params, code_size));
    ASSERT_EQ(OB_SUCCESS,
              ops->calc_encode_scratch_size(params, scratch_size));
    AlignedBuffer code(code_size);
    AlignedBuffer scratch(scratch_size);
    ASSERT_EQ(OB_SUCCESS,
              ops->encode(params,
                          vector.data(),
                          centroid.data(),
                          {scratch.data(), scratch.size()},
                          {code.data(), code.size()}));

    float residual_norm = 0.0F;
    const std::vector<float> normalized =
        normalized_residual(vector, centroid, residual_norm);
    const float best_t = prototype_best_t(normalized, extra_bits);
    const TestLayout layout = test_layout(dim, bits);
    double correction = 0.0;
    for (int64_t i = 0; i < dim; ++i) {
      const int32_t magnitude =
          prototype_magnitude(normalized[i], extra_bits, best_t);
      const bool msb = 0 != (code.data()[i / 8]
                             & static_cast<uint8_t>(1U << (i % 8)));
      EXPECT_EQ(!(normalized[i] < 0.0F), msb)
          << "bits=" << static_cast<int>(bits) << " dim=" << i;
      const uint8_t expected_extra = normalized[i] < 0.0F
                                         ? maximum - magnitude
                                         : magnitude;
      EXPECT_EQ(expected_extra,
                decode_extra(code.data() + layout.extra_offset_,
                             extra_bits,
                             i))
          << "bits=" << static_cast<int>(bits) << " dim=" << i;
      correction += std::abs(static_cast<double>(normalized[i]))
                    * (magnitude + 0.5);
    }
    for (int64_t i = dim; i < layout.padded_dim_; ++i) {
      EXPECT_EQ(0,
                decode_extra(code.data() + layout.extra_offset_,
                             extra_bits,
                             i));
    }
    EXPECT_NEAR(residual_norm,
                load_float(code.data() + layout.residual_norm_offset_),
                1e-6F);
    EXPECT_NEAR(vector_norm(vector),
                load_float(code.data() + layout.raw_norm_offset_),
                1e-6F);
    EXPECT_NEAR(static_cast<float>(1.0 / correction),
                load_float(code.data() + layout.rescale_offset_),
                1e-6F);
  }
}

TEST(TestExtendRaBitQQuantizer, PackingDecodesEveryLaneAcrossSupportedTargets)
{
  common::init_arches();
  const int64_t dim = 129;
  std::vector<float> normalized(dim);
  float norm2 = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    normalized[i] = 17 == i
                        ? 0.0F
                        : static_cast<float>((i * 23) % 47 - 23) * 0.01F;
    norm2 += normalized[i] * normalized[i];
  }
  const float norm = std::sqrt(norm2);
  ASSERT_GT(norm, 0.0F);
  for (float &value : normalized) {
    value /= norm;
  }

  const ObExtendRaBitQSimdOps *generic =
      get_extend_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  const common::ObTargetArch arches[] = {
      common::ObTargetArch::Default,
      common::ObTargetArch::AVX2,
      common::ObTargetArch::AVX512};
  const ObExtendRaBitQSimdOps *target_ops[] = {
      generic,
      get_extend_rabitq_simd_ops(arches[1]),
      get_extend_rabitq_simd_ops(arches[2])};
  for (int8_t extra_bits = 1; extra_bits <= 8; ++extra_bits) {
    const int32_t maximum = (1 << extra_bits) - 1;
    const int64_t msb_size = (dim + 7) / 8;
    const int64_t extra_size = align_up(dim, 64) * extra_bits / 8;
    const double best_t = 0.85 * maximum
                          * std::sqrt(static_cast<double>(dim));
    std::vector<uint8_t> packed_msb(msb_size, 0);
    std::vector<uint8_t> packed_extra(extra_size, 0);
    float correction = 0.0F;
    generic->pack_codes(normalized.data(),
                        dim,
                        extra_bits,
                        best_t,
                        packed_msb.data(),
                        packed_extra.data(),
                        correction);

    std::vector<float> one_hot_query(dim, 0.0F);
    for (int64_t lane = 0; lane < dim; ++lane) {
      SCOPED_TRACE(::testing::Message()
                   << "extra_bits=" << static_cast<int>(extra_bits)
                   << ", lane=" << lane);
      one_hot_query[lane] = 1.0F;
      const bool negative = normalized[lane] < 0.0F;
      const int32_t magnitude = std::min(
          static_cast<int32_t>(
              best_t * std::abs(static_cast<double>(normalized[lane]))
              + 1e-5),
          maximum);
      const uint8_t expected_extra = static_cast<uint8_t>(magnitude)
                                     ^ static_cast<uint8_t>(
                                         negative ? maximum : 0);
      for (int64_t target_index = 0; target_index < 3; ++target_index) {
        const ObExtendRaBitQSimdOps *ops = target_ops[target_index];
        if (nullptr != ops) {
          SCOPED_TRACE(::testing::Message()
                       << "arch="
                       << static_cast<uint32_t>(arches[target_index]));
          EXPECT_FLOAT_EQ(negative ? 0.0F : 1.0F,
                          ops->dot_msb_codes(one_hot_query.data(),
                                             packed_msb.data(),
                                             dim));
          EXPECT_FLOAT_EQ(static_cast<float>(expected_extra),
                          ops->dot_extra_codes[extra_bits - 1](
                              one_hot_query.data(), packed_extra.data(), dim));
        }
      }
      one_hot_query[lane] = 0.0F;
    }
  }
}

TEST(TestExtendRaBitQQuantizer, ComputeAndComputeXBitsReturnSameMetric)
{
  const int64_t dim = 65;
  std::vector<float> base(dim);
  std::vector<float> query(dim);
  std::vector<float> centroid(dim);
  for (int64_t i = 0; i < dim; ++i) {
    base[i] = std::sin(static_cast<float>(i) * 0.17F);
    query[i] = std::cos(static_cast<float>(i) * 0.11F);
    centroid[i] = static_cast<float>((i % 5) - 2) * 0.02F;
  }
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int8_t bits = 2; bits <= 9; ++bits) {
    const ObQuantizationParams params = {dim, bits, {}, 0.0F};
    for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
      common::ObArenaAllocator allocator("ExtendRQ");
      ObQuantizationEncoder encoder;
      ObQuantizationComputer computer;
      ASSERT_EQ(OB_SUCCESS,
                encoder.init(allocator,
                             ObQuantizationType::EXTEND_RABITQ,
                             metric,
                             params));
      AlignedBuffer code(encoder.code_size());
      ASSERT_EQ(OB_SUCCESS,
                encoder.encode(base.data(),
                               centroid.data(),
                               {code.data(), code.size()}));
      ASSERT_EQ(OB_SUCCESS,
                computer.init(allocator,
                              ObQuantizationType::EXTEND_RABITQ,
                              metric,
                              params,
                              query.data(),
                              centroid.data()));
      float query_residual_norm = 0.0F;
      const std::vector<float> normalized_query =
          normalized_residual(query, centroid, query_residual_norm);
      float ip_msb = 0.0F;
      for (int64_t i = 0; i < dim; ++i) {
        if (0 != (code.data()[i / 8]
                  & static_cast<uint8_t>(1U << (i % 8)))) {
          ip_msb += normalized_query[i];
        }
      }
      float computed = 0.0F;
      float computed_xbits = 0.0F;
      ASSERT_EQ(OB_SUCCESS, computer.compute({code.data(), code.size()}, computed));
      ASSERT_EQ(OB_SUCCESS,
                computer.compute_xbits({code.data(), code.size()},
                                       ip_msb,
                                       computed_xbits));
      EXPECT_NEAR(computed, computed_xbits, tolerance(computed));

      float changed_xbits = 0.0F;
      ASSERT_EQ(OB_SUCCESS,
                computer.compute_xbits({code.data(), code.size()},
                                       ip_msb + 0.25F,
                                       changed_xbits));
      EXPECT_GT(std::abs(changed_xbits - computed_xbits), 1e-5F)
          << "bits=" << static_cast<int>(bits)
          << " metric=" << static_cast<int>(metric);
    }
  }
}

TEST(TestExtendRaBitQQuantizer, RejectsInvalidBuffers)
{
  const int64_t dim = 8;
  const int8_t bits = 4;
  const ObQuantizationParams params = {dim, bits, {}, 0.0F};
  const ObQuantizerMetricOps *ops =
      ObExtendRaBitQQuantizer::get_ops().metric_ops[share::VIDA_L2];
  ASSERT_NE(nullptr, ops);
  int64_t code_size = 0;
  int64_t query_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS, ops->calc_code_size(params, code_size));
  ASSERT_EQ(OB_SUCCESS, ops->calc_query_code_size(params, query_size));
  ASSERT_EQ(OB_SUCCESS,
            ops->calc_encode_scratch_size(params, scratch_size));
  AlignedBuffer code(code_size);
  AlignedBuffer query_code(query_size);
  AlignedBuffer scratch(scratch_size);
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F,
                             5.0F, -6.0F, 7.0F, -8.0F};
  const float query[dim] = {-0.5F, 1.0F, -1.5F, 2.0F,
                            -2.5F, 3.0F, -3.5F, 4.0F};
  const float centroid[dim] = {};

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->encode(params, nullptr, centroid,
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size()}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->encode(params, vector, nullptr,
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size()}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->encode(params, vector, centroid, {nullptr, scratch.size()},
                        {code.data(), code.size()}));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->encode(params, vector, centroid,
                        {scratch.data(), scratch.size() - 1},
                        {code.data(), code.size()}));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->encode(params, vector, centroid,
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size() - 1}));

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(params, nullptr, centroid,
                               {query_code.data(), query_code.size()}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(params, query, nullptr,
                               {query_code.data(), query_code.size()}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(params, query, centroid,
                               {nullptr, query_code.size()}));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->prepare_query(params, query, centroid,
                               {query_code.data(), query_code.size() - 1}));

  ASSERT_EQ(OB_SUCCESS,
            ops->encode(params, vector, centroid,
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size()}));
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params, query, centroid,
                               {query_code.data(), query_code.size()}));
  float metric_value = 123.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->compute(params, {nullptr, query_code.size()},
                         {code.data(), code.size()}, metric_value));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->compute(params, {query_code.data(), query_code.size()},
                         {nullptr, code.size()}, metric_value));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->compute(params,
                         {query_code.data(), query_code.size() - 1},
                         {code.data(), code.size()}, metric_value));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->compute(params, {query_code.data(), query_code.size()},
                         {code.data(), code.size() - 1}, metric_value));

  std::vector<uint8_t> unaligned_query(query_size + 1, 0);
  memcpy(unaligned_query.data() + 1, query_code.data(), query_size);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->compute(params, {unaligned_query.data() + 1, query_size},
                         {code.data(), code.size()}, metric_value));
}

TEST(TestExtendRaBitQQuantizer, RejectsInvalidEncodedMetadata)
{
  const int64_t dim = 8;
  const int8_t bits = 4;
  const ObQuantizationParams params = {dim, bits, {}, 0.0F};
  const ObQuantizerMetricOps *ops =
      ObExtendRaBitQQuantizer::get_ops().metric_ops[share::VIDA_L2];
  ASSERT_NE(nullptr, ops);
  int64_t code_size = 0;
  int64_t query_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS, ops->calc_code_size(params, code_size));
  ASSERT_EQ(OB_SUCCESS, ops->calc_query_code_size(params, query_size));
  ASSERT_EQ(OB_SUCCESS,
            ops->calc_encode_scratch_size(params, scratch_size));
  AlignedBuffer code(code_size);
  AlignedBuffer query_code(query_size);
  AlignedBuffer scratch(scratch_size);
  const float vector[dim] = {1.0F, 2.0F, 3.0F, 4.0F,
                             5.0F, 6.0F, 7.0F, 8.0F};
  const float query[dim] = {8.0F, 7.0F, 6.0F, 5.0F,
                            4.0F, 3.0F, 2.0F, 1.0F};
  const float centroid[dim] = {};
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(params, vector, centroid,
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size()}));
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params, query, centroid,
                               {query_code.data(), query_code.size()}));

  const TestLayout layout = test_layout(dim, bits);
  const int64_t query_residual_offset = dim * sizeof(float);
  const int64_t query_raw_offset = query_residual_offset + sizeof(float);
  const float invalid_norms[] = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(),
      -1.0F};
  for (const float invalid : invalid_norms) {
    for (const int64_t offset : {query_residual_offset, query_raw_offset}) {
      AlignedBuffer bad_query(query_size);
      memcpy(bad_query.data(), query_code.data(), query_size);
      store_float(bad_query.data() + offset, invalid);
      float metric_value = 123.0F;
      EXPECT_EQ(OB_ERR_UNEXPECTED,
                ops->compute(params, {bad_query.data(), bad_query.size()},
                             {code.data(), code.size()}, metric_value));
      EXPECT_FLOAT_EQ(123.0F, metric_value);
    }
    for (const int64_t offset : {layout.residual_norm_offset_,
                                 layout.raw_norm_offset_}) {
      AlignedBuffer bad_code(code_size);
      memcpy(bad_code.data(), code.data(), code_size);
      store_float(bad_code.data() + offset, invalid);
      float metric_value = 123.0F;
      EXPECT_EQ(OB_ERR_UNEXPECTED,
                ops->compute(params,
                             {query_code.data(), query_code.size()},
                             {bad_code.data(), bad_code.size()}, metric_value));
      EXPECT_FLOAT_EQ(123.0F, metric_value);
    }
  }

  const float invalid_rescales[] = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(), 0.0F, -1.0F};
  for (const float invalid : invalid_rescales) {
    AlignedBuffer bad_code(code_size);
    memcpy(bad_code.data(), code.data(), code_size);
    store_float(bad_code.data() + layout.rescale_offset_, invalid);
    float metric_value = 123.0F;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
              ops->compute(params, {query_code.data(), query_code.size()},
                           {bad_code.data(), bad_code.size()}, metric_value));
    EXPECT_FLOAT_EQ(123.0F, metric_value);
  }
}

TEST(TestExtendRaBitQQuantizer, ComputeMatchesIndependentScalarFormula)
{
  const int64_t dim = 65;
  std::vector<float> base(dim);
  std::vector<float> query(dim);
  std::vector<float> centroid(dim);
  for (int64_t i = 0; i < dim; ++i) {
    base[i] = std::sin(static_cast<float>(i) * 0.23F) + 0.01F * (i % 3);
    query[i] = std::cos(static_cast<float>(i) * 0.07F) - 0.02F * (i % 7);
    centroid[i] = static_cast<float>((i * 5) % 13 - 6) * 0.015F;
  }
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int8_t bits = 2; bits <= 9; ++bits) {
    const ObQuantizationParams params = {dim, bits, {}, 0.0F};
    for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
      common::ObArenaAllocator allocator("ExtendScalar");
      ObQuantizationEncoder encoder;
      ObQuantizationComputer computer;
      ASSERT_EQ(OB_SUCCESS,
                encoder.init(allocator,
                             ObQuantizationType::EXTEND_RABITQ,
                             metric,
                             params));
      AlignedBuffer code(encoder.code_size());
      ASSERT_EQ(OB_SUCCESS,
                encoder.encode(base.data(),
                               centroid.data(),
                               {code.data(), code.size()}));
      ASSERT_EQ(OB_SUCCESS,
                computer.init(allocator,
                              ObQuantizationType::EXTEND_RABITQ,
                              metric,
                              params,
                              query.data(),
                              centroid.data()));

      float computed = 0.0F;
      ASSERT_EQ(OB_SUCCESS,
                computer.compute({code.data(), code.size()}, computed));
      const float expected = scalar_metric_from_code(metric,
                                                     bits,
                                                     base,
                                                     query,
                                                     centroid,
                                                     code.data());
      EXPECT_NEAR(expected, computed, tolerance(expected))
          << "bits=" << static_cast<int>(bits)
          << " metric=" << static_cast<int>(metric);
    }
  }
}

TEST(TestExtendRaBitQQuantizer, ZeroResidualIsDeterministic)
{
  const int64_t dim = 17;
  const int8_t bits = 4;
  std::vector<float> vector(dim, 0.25F);
  const ObQuantizationParams params = {dim, bits, {}, calc_inv_sqrt_dim(dim)};
  const ObQuantizerMetricOps *ops =
      ObExtendRaBitQQuantizer::get_ops().metric_ops[share::VIDA_L2];
  int64_t code_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS, ops->calc_code_size(params, code_size));
  ASSERT_EQ(OB_SUCCESS,
            ops->calc_encode_scratch_size(params, scratch_size));
  AlignedBuffer code(code_size);
  AlignedBuffer scratch(scratch_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(params,
                        vector.data(),
                        vector.data(),
                        {scratch.data(), scratch.size()},
                        {code.data(), code.size()}));
  const TestLayout layout = test_layout(dim, bits);
  for (int64_t i = 0; i < layout.residual_norm_offset_; ++i) {
    EXPECT_EQ(0, code.data()[i]);
  }
  EXPECT_FLOAT_EQ(0.0F,
                  load_float(code.data() + layout.residual_norm_offset_));
  EXPECT_NEAR(vector_norm(vector),
              load_float(code.data() + layout.raw_norm_offset_),
              1e-6F);
  EXPECT_FLOAT_EQ(1.0F, load_float(code.data() + layout.rescale_offset_));

  std::vector<float> query(dim);
  for (int64_t i = 0; i < dim; ++i) {
    query[i] = 0.1F * static_cast<float>(i + 1);
  }
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
    common::ObArenaAllocator allocator("ExtendZeroRes");
    ObQuantizationComputer computer;
    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator,
                            ObQuantizationType::EXTEND_RABITQ,
                            metric,
                            params,
                            query.data(),
                            vector.data()));
    float computed = 0.0F;
    ASSERT_EQ(OB_SUCCESS, computer.compute({code.data(), code.size()}, computed));
    const float expected = scalar_metric_from_code(metric,
                                                   bits,
                                                   vector,
                                                   query,
                                                   vector,
                                                   code.data());
    EXPECT_NEAR(expected, computed, tolerance(expected))
        << "metric=" << static_cast<int>(metric);
  }
}

TEST(TestExtendRaBitQQuantizer, ZeroRawNormReturnsMaximumIpAndCosine)
{
  const int64_t dim = 17;
  const int8_t bits = 4;
  const std::vector<float> zeros(dim, 0.0F);
  const std::vector<float> nonzero(dim, 0.25F);
  const ObQuantizationParams params = {dim, bits, {}, 0.0F};
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_IP, share::VIDA_COS};
  const std::vector<float> *bases[] = {&zeros, &nonzero};
  const std::vector<float> *queries[] = {&nonzero, &zeros};
  for (int64_t case_index = 0; case_index < 2; ++case_index) {
    for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
      common::ObArenaAllocator allocator("ExtendZeroNorm");
      ObQuantizationEncoder encoder;
      ObQuantizationComputer computer;
      ASSERT_EQ(OB_SUCCESS,
                encoder.init(allocator,
                             ObQuantizationType::EXTEND_RABITQ,
                             metric,
                             params));
      AlignedBuffer code(encoder.code_size());
      ASSERT_EQ(OB_SUCCESS,
                encoder.encode(bases[case_index]->data(),
                               zeros.data(),
                               {code.data(), code.size()}));
      ASSERT_EQ(OB_SUCCESS,
                computer.init(allocator,
                              ObQuantizationType::EXTEND_RABITQ,
                              metric,
                              params,
                              queries[case_index]->data(),
                              zeros.data()));
      float computed = 0.0F;
      ASSERT_EQ(OB_SUCCESS,
                computer.compute({code.data(), code.size()}, computed));
      EXPECT_FLOAT_EQ(1.0F, computed)
          << "metric=" << static_cast<int>(metric)
          << " case=" << case_index;
    }
  }
}

TEST(TestExtendRaBitQQuantizer, SimdAbsoluteAndMaxMatchesGeneric)
{
  common::init_arches();
  const ObExtendRaBitQSimdOps *generic =
      get_extend_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  const common::ObTargetArch arches[] = {
      common::ObTargetArch::Default,
      common::ObTargetArch::AVX2, common::ObTargetArch::AVX512};
  const int64_t dimensions[] = {1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 768, 769};
  constexpr float CANARY = -1234.0F;
  for (const int64_t dim : dimensions) {
    // Offset aligned storage by one float to exercise unaligned loads/stores.
    std::vector<uint64_t> input_storage((dim + 3) / 2);
    std::vector<uint64_t> output_storage((dim + 3) / 2);
    float *input = reinterpret_cast<float *>(input_storage.data()) + 1;
    float *output = reinterpret_cast<float *>(output_storage.data()) + 1;
    std::vector<float> expected(dim);
    for (int64_t peak = -1; peak < dim; ++peak) {
      for (int64_t i = 0; i < dim; ++i) {
        input[i] = peak < 0 ? -0.0F : (i % 2 == 0 ? -0.25F : 0.5F);
      }
      if (peak >= 0) {
        input[peak] = -1.0F;
      }
      const float expected_max = generic->absolute_and_max(input, expected.data(), dim);
      for (const common::ObTargetArch arch : arches) {
        const ObExtendRaBitQSimdOps *ops = get_extend_rabitq_simd_ops(arch);
        if (nullptr != ops) {
          std::fill(output - 1, output + dim + 1, CANARY);
          EXPECT_EQ(expected_max, ops->absolute_and_max(input, output, dim));
          EXPECT_EQ(0, memcmp(expected.data(), output, dim * sizeof(float)));
          EXPECT_EQ(CANARY, output[-1]);
          EXPECT_EQ(CANARY, output[dim]);
        }
      }
      std::fill(output, output + dim, CANARY);
      EXPECT_EQ(expected_max,
          get_extend_rabitq_simd_ops().absolute_and_max(input, output, dim));
      EXPECT_EQ(0, memcmp(expected.data(), output, dim * sizeof(float)));
    }
  }
}

TEST(TestExtendRaBitQQuantizer, SimdPackingAndDotMatchGeneric)
{
  common::init_arches();
  const int64_t dimensions[] = {
      1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33,
      63, 64, 65, 127, 128, 129, 255, 256, 257, 767, 768, 769};
  constexpr int64_t RANDOM_ROUNDS = 8;
  std::mt19937 generator(20260828);
  std::uniform_real_distribution<float> residual_distribution(-1.0F, 1.0F);
  std::uniform_real_distribution<float> query_distribution(-2.0F, 2.0F);
  std::uniform_real_distribution<double> scale_distribution(0.35, 1.85);
  const ObExtendRaBitQSimdOps *generic =
      get_extend_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  const common::ObTargetArch arches[] = {
      common::ObTargetArch::AVX2, common::ObTargetArch::AVX512};
  const ObExtendRaBitQSimdOps *simd_ops[] = {
      get_extend_rabitq_simd_ops(arches[0]),
      get_extend_rabitq_simd_ops(arches[1])};
  for (const int64_t dim : dimensions) {
    for (int64_t round = 0; round < RANDOM_ROUNDS; ++round) {
      std::vector<float> normalized(dim);
      std::vector<float> query(dim);
      float norm2 = 0.0F;
      float query_norm2 = 0.0F;
      for (int64_t i = 0; i < dim; ++i) {
        normalized[i] = residual_distribution(generator);
        query[i] = query_distribution(generator);
        norm2 += normalized[i] * normalized[i];
        query_norm2 += query[i] * query[i];
      }
      const float norm = std::sqrt(norm2);
      const float query_norm = std::sqrt(query_norm2);
      ASSERT_GT(norm, 0.0F);
      ASSERT_GT(query_norm, 0.0F);
      for (int64_t i = 0; i < dim; ++i) {
        normalized[i] /= norm;
        query[i] /= query_norm;
      }
      for (int8_t extra_bits = 1; extra_bits <= 8; ++extra_bits) {
        SCOPED_TRACE(::testing::Message()
                     << "dim=" << dim
                     << ", round=" << round
                     << ", extra_bits=" << static_cast<int>(extra_bits));
        const int64_t msb_size = (dim + 7) / 8;
        const int64_t extra_size = align_up(dim, 64) * extra_bits / 8;
        const double best_t = scale_distribution(generator)
                              * ((1U << extra_bits) - 1)
                              * std::sqrt(static_cast<double>(dim));
        std::vector<uint8_t> expected_msb(msb_size, 0);
        std::vector<uint8_t> expected_extra(extra_size, 0);
        float expected_correction = 0.0F;
        generic->pack_codes(normalized.data(),
                            dim,
                            extra_bits,
                            best_t,
                            expected_msb.data(),
                            expected_extra.data(),
                            expected_correction);
        const float expected_msb_dot = generic->dot_msb_codes(
            query.data(), expected_msb.data(), dim);
        const float expected_extra_dot =
            generic->dot_extra_codes[extra_bits - 1](
                query.data(), expected_extra.data(), dim);
        double absolute_msb_sum = 0.0;
        double absolute_extra_sum = 0.0;
        for (int64_t i = 0; i < dim; ++i) {
          const double absolute_query =
              std::abs(static_cast<double>(query[i]));
          if (0 != (expected_msb[i / 8]
                    & static_cast<uint8_t>(1U << (i % 8)))) {
            absolute_msb_sum += absolute_query;
          }
          absolute_extra_sum +=
              absolute_query
              * decode_extra(expected_extra.data(), extra_bits, i);
        }
        for (int64_t arch_index = 0; arch_index < 2; ++arch_index) {
          const ObExtendRaBitQSimdOps *simd = simd_ops[arch_index];
          if (nullptr != simd) {
            SCOPED_TRACE(::testing::Message()
                         << "arch=" << static_cast<uint32_t>(arches[arch_index]));
            std::vector<uint8_t> actual_msb(msb_size, 0);
            std::vector<uint8_t> actual_extra(extra_size, 0);
            float actual_correction = 0.0F;
            simd->pack_codes(normalized.data(),
                             dim,
                             extra_bits,
                             best_t,
                             actual_msb.data(),
                             actual_extra.data(),
                             actual_correction);
            EXPECT_EQ(expected_msb, actual_msb);
            EXPECT_EQ(expected_extra, actual_extra);
            EXPECT_FLOAT_EQ(expected_correction, actual_correction);
            EXPECT_NEAR(expected_msb_dot,
                        simd->dot_msb_codes(
                            query.data(), actual_msb.data(), dim),
                        accumulation_tolerance(expected_msb_dot,
                                               absolute_msb_sum));
            EXPECT_NEAR(expected_extra_dot,
                        simd->dot_extra_codes[extra_bits - 1](
                            query.data(), actual_extra.data(), dim),
                        accumulation_tolerance(expected_extra_dot,
                                               absolute_extra_sum));
          }
        }
      }
    }
  }
}

TEST(TestExtendRaBitQQuantizer, SimdPackingPreservesSignedZeroAndTail)
{
  common::init_arches();
  const ObExtendRaBitQSimdOps *generic =
      get_extend_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  const float values[] = {0.0F, -0.0F, 0.125F, -0.125F,
                          0.5F, -0.5F, 1.0F, -1.0F};
  for (const int64_t dim : {7, 8, 9, 63, 64, 65, 129}) {
    std::vector<float> input(dim);
    for (int64_t i = 0; i < dim; ++i) {
      input[i] = values[i % 8];
    }
    for (int8_t extra_bits = 1; extra_bits <= 8; ++extra_bits) {
      const int64_t msb_size = (dim + 7) / 8;
      const int64_t extra_size = align_up(dim, 64) * extra_bits / 8;
      const double best_t = (1U << extra_bits) - 0.75;
      std::vector<uint8_t> expected(msb_size + extra_size, 0);
      float expected_correction = 0.0F;
      generic->pack_codes(input.data(), dim, extra_bits, best_t,
                          expected.data(), expected.data() + msb_size,
                          expected_correction);
      for (const common::ObTargetArch arch :
           {common::ObTargetArch::AVX2, common::ObTargetArch::AVX512}) {
        const ObExtendRaBitQSimdOps *simd = get_extend_rabitq_simd_ops(arch);
        if (nullptr != simd) {
          SCOPED_TRACE(::testing::Message() << "dim=" << dim
                       << ", extra_bits=" << static_cast<int>(extra_bits)
                       << ", arch=" << static_cast<uint32_t>(arch));
          std::vector<uint8_t> actual(expected.size(), 0);
          float actual_correction = 0.0F;
          simd->pack_codes(input.data(), dim, extra_bits, best_t,
                           actual.data(), actual.data() + msb_size,
                           actual_correction);
          EXPECT_EQ(expected, actual);
          EXPECT_EQ(0, memcmp(&expected_correction, &actual_correction,
                              sizeof(float)));
          // Both +0.0F and -0.0F follow the nonnegative sign-code path.
          EXPECT_EQ(3U, actual[0] & 3U);
        }
      }
    }
  }
}

TEST(TestExtendRaBitQQuantizer, SimdHandlesUnalignedAddresses)
{
  common::init_arches();
  const int64_t dim = 65;
  std::vector<float> normalized_storage(dim + 1);
  std::vector<float> query_storage(dim + 1);
  float *normalized = normalized_storage.data() + 1;
  float *query = query_storage.data() + 1;
  ASSERT_EQ(0U, reinterpret_cast<uintptr_t>(normalized) % alignof(float));
  ASSERT_EQ(0U, reinterpret_cast<uintptr_t>(query) % alignof(float));
  ASSERT_NE(0U, reinterpret_cast<uintptr_t>(normalized) % 32);
  ASSERT_NE(0U, reinterpret_cast<uintptr_t>(query) % 32);
  float norm2 = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    normalized[i] = static_cast<float>((i * 13) % 31 - 15) * 0.02F;
    query[i] = static_cast<float>((i * 19) % 37 - 18) * 0.01F;
    norm2 += normalized[i] * normalized[i];
  }
  const float norm = std::sqrt(norm2);
  for (int64_t i = 0; i < dim; ++i) {
    normalized[i] /= norm;
  }

  const ObExtendRaBitQSimdOps *generic =
      get_extend_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  const common::ObTargetArch arches[] = {
      common::ObTargetArch::AVX2, common::ObTargetArch::AVX512};
  const ObExtendRaBitQSimdOps *simd_ops[] = {
      get_extend_rabitq_simd_ops(arches[0]),
      get_extend_rabitq_simd_ops(arches[1])};
  for (int8_t extra_bits = 1; extra_bits <= 8; ++extra_bits) {
    SCOPED_TRACE(::testing::Message()
                 << "extra_bits=" << static_cast<int>(extra_bits));
    const int64_t msb_size = (dim + 7) / 8;
    const int64_t extra_size = align_up(dim, 64) * extra_bits / 8;
    const double best_t = 0.9 * ((1U << extra_bits) - 1)
                          * std::sqrt(static_cast<double>(dim));
    std::vector<uint8_t> expected_msb(msb_size, 0);
    std::vector<uint8_t> expected_extra(extra_size, 0);
    float expected_correction = 0.0F;
    generic->pack_codes(normalized,
                        dim,
                        extra_bits,
                        best_t,
                        expected_msb.data(),
                        expected_extra.data(),
                        expected_correction);
    const float expected_msb_dot = generic->dot_msb_codes(
        query, expected_msb.data(), dim);
    const float expected_extra_dot =
        generic->dot_extra_codes[extra_bits - 1](
            query, expected_extra.data(), dim);

    for (int64_t arch_index = 0; arch_index < 2; ++arch_index) {
      const ObExtendRaBitQSimdOps *simd = simd_ops[arch_index];
      if (nullptr != simd) {
        SCOPED_TRACE(::testing::Message()
                     << "arch=" << static_cast<uint32_t>(arches[arch_index]));
        constexpr uint8_t CANARY = 0xA5;
        std::vector<uint8_t> actual_msb_storage(msb_size + 2, CANARY);
        std::vector<uint8_t> actual_extra_storage(extra_size + 2, CANARY);
        uint8_t *actual_msb = actual_msb_storage.data() + 1;
        uint8_t *actual_extra = actual_extra_storage.data() + 1;
        std::fill(actual_msb, actual_msb + msb_size, 0);
        std::fill(actual_extra, actual_extra + extra_size, 0);
        ASSERT_NE(0U, reinterpret_cast<uintptr_t>(actual_msb) % 32);
        ASSERT_NE(0U, reinterpret_cast<uintptr_t>(actual_extra) % 32);
        float actual_correction = 0.0F;
        simd->pack_codes(normalized,
                         dim,
                         extra_bits,
                         best_t,
                         actual_msb,
                         actual_extra,
                         actual_correction);
        EXPECT_EQ(0, memcmp(expected_msb.data(), actual_msb, msb_size));
        EXPECT_EQ(0, memcmp(expected_extra.data(), actual_extra, extra_size));
        EXPECT_FLOAT_EQ(expected_correction, actual_correction);
        EXPECT_NEAR(expected_msb_dot,
                    simd->dot_msb_codes(query, actual_msb, dim),
                    tolerance(expected_msb_dot));
        EXPECT_NEAR(expected_extra_dot,
                    simd->dot_extra_codes[extra_bits - 1](
                        query, actual_extra, dim),
                    tolerance(expected_extra_dot));
        EXPECT_EQ(CANARY, actual_msb_storage.front());
        EXPECT_EQ(CANARY, actual_msb_storage.back());
        EXPECT_EQ(CANARY, actual_extra_storage.front());
        EXPECT_EQ(CANARY, actual_extra_storage.back());
      }
    }
  }
}

TEST(TestExtendRaBitQQuantizer, XBitsRejectsNonFiniteMsb)
{
  const int64_t dim = 8;
  const ObQuantizationParams params = {dim, 2, {}, 0.0F};
  const float query[dim] = {1.0F};
  const float base[dim] = {2.0F};
  const float centroid[dim] = {};
  common::ObArenaAllocator allocator("ExtendBadIP");
  ObQuantizationEncoder encoder;
  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::EXTEND_RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedBuffer code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(base, centroid, {code.data(), code.size()}));
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::EXTEND_RABITQ,
                          share::VIDA_L2,
                          params,
                          query,
                          centroid));
  float metric_value = 0.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            computer.compute_xbits(
                {code.data(), code.size()},
                std::numeric_limits<float>::quiet_NaN(),
                metric_value));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            computer.compute_xbits(
                {code.data(), code.size()},
                std::numeric_limits<float>::infinity(),
                metric_value));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            computer.compute_xbits({nullptr, 0}, 0.0F, metric_value));
}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
