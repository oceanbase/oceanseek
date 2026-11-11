/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <algorithm>
#include <cmath>
#include <memory>
#include <limits>
#include <float.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <vector>

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_fast_scan.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

namespace
{

int64_t code_bytes(const int64_t dim)
{
  return dim / 8 + (0 == dim % 8 ? 0 : 1);
}

void pack_codes_reference(const uint8_t *codes,
                          const int64_t code_stride,
                          const int64_t code_count,
                          const int64_t dim,
                          std::vector<uint8_t> &packed)
{
  const int64_t bytes = code_bytes(dim);
  const int64_t block_count = code_count / 32 + (0 == code_count % 32 ? 0 : 1);
  const uint8_t tail_mask = 0 == dim % 8
                                ? UINT8_MAX
                                : static_cast<uint8_t>((1U << (dim % 8)) - 1U);
  packed.assign(block_count * bytes * 32, 0);
  for (int64_t block_index = 0; block_index < block_count; ++block_index) {
    for (int64_t byte_index = 0; byte_index < bytes; ++byte_index) {
      uint8_t source_bytes[32] = {};
      for (int64_t lane = 0; lane < 32; ++lane) {
        const int64_t code_index = block_index * 32 + lane;
        if (code_index < code_count) {
          source_bytes[lane] = codes[code_index * code_stride + byte_index];
          if (byte_index == bytes - 1) {
            source_bytes[lane] &= tail_mask;
          }
        }
      }
      uint8_t *column = packed.data() + (block_index * bytes + byte_index) * 32;
      for (int64_t output_lane = 0; output_lane < 16; ++output_lane) {
        const int64_t low_lane = output_lane;
        const int64_t high_lane = low_lane + 16;
        column[output_lane] = static_cast<uint8_t>(
            (source_bytes[low_lane] & 0x0FU)
            | ((source_bytes[high_lane] & 0x0FU) << 4));
        column[output_lane + 16] = static_cast<uint8_t>(
            (source_bytes[low_lane] >> 4) | (source_bytes[high_lane] & 0xF0U));
      }
    }
  }
}

void unpack_codes(const uint8_t *packed,
                  const int64_t code_count,
                  const int64_t dim,
                  std::vector<uint8_t> &codes)
{
  const int64_t bytes = code_bytes(dim);
  codes.assign(code_count * bytes, 0);
  for (int64_t code_index = 0; code_index < code_count; ++code_index) {
    const int64_t block_index = code_index / ObRaBitQFastScanEncoder::BATCH_SIZE;
    const int64_t lane = code_index % ObRaBitQFastScanEncoder::BATCH_SIZE;
    const int64_t packed_lane = lane % 16;
    const bool is_high_nibble = lane >= 16;
    const int64_t block_offset =
        block_index * bytes * ObRaBitQFastScanEncoder::BATCH_SIZE;
    for (int64_t byte_index = 0; byte_index < bytes; ++byte_index) {
      const uint8_t low_pair = packed[block_offset + byte_index * 32 + packed_lane];
      const uint8_t high_pair = packed[block_offset + byte_index * 32
                                       + packed_lane + 16];
      codes[code_index * bytes + byte_index] = is_high_nibble
          ? static_cast<uint8_t>((low_pair >> 4) | (high_pair & 0xF0U))
          : static_cast<uint8_t>((low_pair & 0x0FU) | (high_pair << 4));
    }
  }
}

int64_t metadata_offset(const int64_t dim)
{
  const int64_t bytes = code_bytes(dim);
  return bytes + (sizeof(float) - bytes % sizeof(float)) % sizeof(float);
}

void load_metadata(const uint8_t *code,
                   const int64_t offset,
                   float &residual_norm,
                   float &correction,
                   float &raw_norm)
{
  MEMCPY(&residual_norm, code + offset, sizeof(float));
  MEMCPY(&correction, code + offset + sizeof(float), sizeof(float));
  MEMCPY(&raw_norm, code + offset + 2 * sizeof(float), sizeof(float));
}

float fast_scan_metric_tolerance(const float expected)
{
  return 1e-3F * std::max(1.0F, std::abs(expected));
}

void expect_metric_near_or_equal(const float expected, const float actual)
{
  if (expected == actual) {
    EXPECT_FLOAT_EQ(expected, actual);
  } else {
    EXPECT_NEAR(expected, actual, fast_scan_metric_tolerance(expected));
  }
}

void init_arches_quietly()
{
  fflush(stdout);
  const int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    freopen("/dev/null", "w", stdout);
    common::init_arches();
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    common::init_arches();
  }
}

} // namespace

TEST(TestRaBitQFastScanEncoder, MatchesReferenceLaneLayout)
{
  const int64_t dim = 8;
  const int64_t code_count = 32;
  uint8_t codes[code_count];
  for (int64_t i = 0; i < code_count; ++i) {
    codes[i] = static_cast<uint8_t>(i);
  }
  const uint8_t expected[32] = {
      0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
      0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
      0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10,
      0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10};
  uint8_t packed[sizeof(expected)] = {};

  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQFastScanEncoder::pack_codes(
                codes, code_count, dim, {packed, sizeof(packed)}));
  EXPECT_EQ(0, memcmp(expected, packed, sizeof(expected)));
}

TEST(TestRaBitQFastScanEncoder, RoundTripsDimensionsCountsAndStride)
{
  const int64_t dimensions[] = {1, 7, 8, 9, 63, 64, 65};
  const int64_t code_counts[] = {0, 1, 15, 16, 31, 32, 33, 65};
  for (int64_t dim : dimensions) {
    const int64_t bytes = code_bytes(dim);
    const int64_t stride = bytes + 5;
    const uint8_t tail_mask = 0 == dim % 8
                                  ? UINT8_MAX
                                  : static_cast<uint8_t>((1U << (dim % 8)) - 1U);
    for (int64_t vector_count : code_counts) {
      SCOPED_TRACE(::testing::Message() << "dim=" << dim << ", vector_count=" << vector_count);
      std::vector<uint8_t> source(vector_count * stride, 0xA5);
      std::vector<uint8_t> expected(vector_count * bytes, 0);
      for (int64_t code_index = 0; code_index < vector_count; ++code_index) {
        for (int64_t byte_index = 0; byte_index < bytes; ++byte_index) {
          const uint8_t value = static_cast<uint8_t>(
              code_index * 73 + byte_index * 29 + dim * 11);
          source[code_index * stride + byte_index] = value;
          expected[code_index * bytes + byte_index] = value;
        }
        expected[(code_index + 1) * bytes - 1] &= tail_mask;
      }

      int64_t packed_size = -1;
      ASSERT_EQ(OB_SUCCESS,
                ObRaBitQFastScanEncoder::calc_packed_size(dim, vector_count, packed_size));
      const int64_t expected_block_count = vector_count / 32 + (0 == vector_count % 32 ? 0 : 1);
      EXPECT_EQ(expected_block_count * bytes * 32, packed_size);
      std::vector<uint8_t> guarded_packed(packed_size + 2, 0xCC);
      uint8_t *packed = guarded_packed.data() + 1;
      ASSERT_EQ(OB_SUCCESS,
                ObRaBitQFastScanEncoder::pack_codes(
                    vector_count > 0 ? source.data() : nullptr,
                    stride,
                    vector_count,
                    dim,
                    {packed, packed_size}));
      EXPECT_EQ(0xCC, guarded_packed.front());
      EXPECT_EQ(0xCC, guarded_packed.back());
      std::vector<uint8_t> reference_packed;
      pack_codes_reference(source.data(), stride, vector_count, dim, reference_packed);
      EXPECT_EQ(reference_packed,
                std::vector<uint8_t>(packed, packed + packed_size));

      std::vector<uint8_t> actual;
      unpack_codes(packed, vector_count, dim, actual);
      EXPECT_EQ(expected, actual);
    }
  }
}

TEST(TestRaBitQFastScanEncoder, ZeroPadsTailBlock)
{
  const int64_t dim = 8;
  const int64_t code_count = 33;
  std::vector<uint8_t> codes(code_count, 0xFF);
  int64_t packed_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQFastScanEncoder::calc_packed_size(dim, code_count, packed_size));
  std::vector<uint8_t> packed(packed_size, 0xA5);
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQFastScanEncoder::pack_codes(
                codes.data(), code_count, dim, {packed.data(), packed_size}));

  const uint8_t *tail_block = packed.data() + 32;
  EXPECT_EQ(0x0F, tail_block[0]);
  EXPECT_EQ(0x0F, tail_block[16]);
  for (int64_t i = 1; i < 16; ++i) {
    EXPECT_EQ(0, tail_block[i]);
    EXPECT_EQ(0, tail_block[i + 16]);
  }
}

TEST(TestRaBitQRowBatch, EmptyInvalidAndUnalignedPayloads)
{
  ObArenaAllocator allocator("RaBQRowBatch");
  ObQuantizationComputer computer;
  float out[2], lower[2];
  EXPECT_EQ(OB_NOT_INIT, computer.compute_batch(nullptr, 0, nullptr));
  const ObQuantizationParams params = {9, 1, {}, calc_inv_sqrt_dim(9)};
  float query[9] = {1, 2, 3}, centroid[9] = {};
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(allocator, ObQuantizationType::RABITQ, share::VIDA_COS, params));
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::RABITQ,
      share::VIDA_COS, params, query, centroid));
  std::vector<uint8_t> bytes(encoder.code_size() + 1);
  ObQuantizationBuffer code = {bytes.data() + 1, encoder.code_size()};
  ASSERT_EQ(OB_SUCCESS, encoder.encode(query, centroid, {bytes.data(), encoder.code_size()}));
  MEMMOVE(bytes.data() + 1, bytes.data(), encoder.code_size());
  EXPECT_EQ(OB_SUCCESS, computer.compute_batch(nullptr, 0, nullptr));
  EXPECT_EQ(OB_SUCCESS, computer.compute_lower_bounds_batch(nullptr, 0, nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_lower_bounds_batch(nullptr, 1, lower));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_lower_bounds_batch(&code, 1, nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(&code, -1, out));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(nullptr, 1, out));
  ObQuantizationBuffer codes[2] = {code, {code.data_, code.size_ - 1}};
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, computer.compute_batch(codes, 2, out));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, computer.compute_lower_bounds_batch(codes, 2, lower));
  codes[1] = {nullptr, code.size_};
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(codes, 2, out));
  codes[1] = code;
  ASSERT_EQ(OB_SUCCESS, computer.compute_batch(codes, 2, out));
  ASSERT_EQ(OB_SUCCESS, computer.compute_lower_bounds_batch(codes, 2, lower));
  float expected, lo, hi;
  ASSERT_EQ(OB_SUCCESS, computer.compute_bounds(code, expected, lo, hi));
  EXPECT_FLOAT_EQ(expected, out[1]);
  EXPECT_FLOAT_EQ(lo, lower[1]);
  computer.reset();
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, query, nullptr));
  codes[0] = {reinterpret_cast<uint8_t *>(query), sizeof(query)};
  ASSERT_EQ(OB_SUCCESS, computer.compute_batch(codes, 1, out));
  EXPECT_FLOAT_EQ(0, out[0]);
  EXPECT_EQ(OB_NOT_SUPPORTED, computer.compute_lower_bounds_batch(codes, 1, lower));
}

TEST(TestRaBitQRowBatch, ReprepareQueryAndCachedNorm)
{
  ObArenaAllocator allocator("RaBQRefresh");
  const int dim = 65;
  ObQuantizationParams params = {dim, 1, {}, calc_inv_sqrt_dim(dim)};
  float query[dim] = {}, centroid[dim] = {}, base[dim] = {};
  for (const auto metric : {share::VIDA_L2, share::VIDA_IP, share::VIDA_COS}) {
    ObQuantizationEncoder encoder;
    ObQuantizationComputer reusable;
    ASSERT_EQ(OB_NOT_INIT, reusable.reprepare_query(query, centroid));
    ASSERT_EQ(OB_SUCCESS, encoder.init(allocator, ObQuantizationType::RABITQ, metric, params));
    std::vector<uint8_t> bytes(encoder.code_size());
    ObQuantizationBuffer code{bytes.data(), encoder.code_size()};
    for (int q = 0; q < 3; ++q) {
      float norm2 = 0;
      for (int i = 0; i < dim; ++i) { query[i] = q * ((i % 7) - 3) * 0.03F; norm2 += query[i] * query[i]; }
      const float norm = std::sqrt(norm2);
      for (int c = 0; c < 4; ++c) {
        for (int i = 0; i < dim; ++i) {
          centroid[i] = c * ((i % 5) - 2) * 0.02F;
          base[i] = ((i * 7) % 11 - 5) * 0.1F;
        }
        ASSERT_EQ(OB_SUCCESS, encoder.encode(base, centroid, code));
        if (!reusable.is_inited()) {
          ASSERT_EQ(OB_SUCCESS, reusable.init(allocator, ObQuantizationType::RABITQ,
              metric, params, query, centroid, &norm));
        } else {
          ASSERT_EQ(OB_SUCCESS, reusable.reprepare_query(query, centroid, &norm));
        }
        ObQuantizationComputer fresh;
        ASSERT_EQ(OB_SUCCESS, fresh.init(allocator, ObQuantizationType::RABITQ, metric, params, query, centroid));
        float value, lower, upper, got;
        ASSERT_EQ(OB_SUCCESS, fresh.compute_bounds(code, value, lower, upper));
        ASSERT_EQ(OB_SUCCESS, reusable.compute_lower_bounds_batch(&code, 1, &got));
        EXPECT_FLOAT_EQ(lower, got);
      }
    }
    const float invalid_norm = -1.0F;
    EXPECT_EQ(OB_INVALID_ARGUMENT, reusable.reprepare_query(query, centroid, &invalid_norm));
    EXPECT_FALSE(reusable.is_inited());
    float got;
    EXPECT_EQ(OB_NOT_INIT, reusable.compute_lower_bounds_batch(&code, 1, &got));
    ASSERT_EQ(OB_SUCCESS, reusable.init(allocator, ObQuantizationType::RABITQ, metric, params, query, centroid));
    EXPECT_EQ(OB_INVALID_ARGUMENT, reusable.reprepare_query(nullptr, centroid));
    EXPECT_FALSE(reusable.is_inited());
  }
}

TEST(TestRaBitQRowBatch, LowerBoundsKeepDegenerateAndNonFiniteBehavior)
{
  ObArenaAllocator allocator("RaBQBoundEdge");
  const int dim = 9;
  const ObQuantizationParams params = {dim, 1, {}, calc_inv_sqrt_dim(dim)};
  float query[dim] = {1.0F}, centroid[dim] = {}, base[dim] = {0.3F, 0.2F};
  for (const auto metric : {share::VIDA_L2, share::VIDA_IP, share::VIDA_COS}) {
    ObQuantizationEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(allocator, ObQuantizationType::RABITQ, metric, params));
    std::vector<uint8_t> bytes(encoder.code_size());
    ObQuantizationBuffer code{bytes.data(), encoder.code_size()};
    for (float q : {0.0F, 1.0F, std::numeric_limits<float>::infinity(), std::numeric_limits<float>::quiet_NaN()}) {
      query[0] = q;
      ObQuantizationComputer computer;
      ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::RABITQ, metric, params, query, centroid));
      for (float correction : {0.0F, 1e-8F, 0.8F, std::numeric_limits<float>::infinity(), std::numeric_limits<float>::quiet_NaN()}) {
        ASSERT_EQ(OB_SUCCESS, encoder.encode(base, centroid, code));
        MEMCPY(code.data_ + code.size_ - 2 * sizeof(float), &correction, sizeof(float));
        float value, lower, upper, got;
        ASSERT_EQ(OB_SUCCESS, computer.compute_bounds(code, value, lower, upper));
        ASSERT_EQ(OB_SUCCESS, computer.compute_lower_bounds_batch(&code, 1, &got));
        if (std::isnan(lower)) { EXPECT_TRUE(std::isnan(got)); }
        else { EXPECT_FLOAT_EQ(lower, got); }
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, MatchesScalarMetricsAndBounds)
{
  const int64_t dimensions[] = {1, 7, 9, 65, 768};
  const int64_t code_counts[] = {1, 17, 32};
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (const int64_t dim : dimensions) {
    std::vector<float> query(dim);
    std::vector<float> centroid(dim);
    for (int64_t j = 0; j < dim; ++j) {
      query[j] = static_cast<float>((j * 17 + 3) % 23 - 11) * 0.19F;
      centroid[j] = static_cast<float>((j * 7 + 5) % 13 - 6) * 0.11F;
    }
    const ObQuantizationParams params = {dim, 1, {}, calc_inv_sqrt_dim(dim)};
    for (const int64_t vector_count : code_counts) {
      for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
        SCOPED_TRACE(::testing::Message()
            << "dim=" << dim << ", vector_count=" << vector_count << ", metric=" << metric);
        ObArenaAllocator allocator("RaBQFastCalc");
        ObQuantizationEncoder encoder;
        ObQuantizationComputer scalar_computer;
        ObRaBitQFastScanComputer fast_computer;
        ASSERT_EQ(OB_SUCCESS, encoder.init(
            allocator, ObQuantizationType::RABITQ, metric, params));
        ASSERT_EQ(OB_SUCCESS, scalar_computer.init(
            allocator, ObQuantizationType::RABITQ, metric,
            params, query.data(), centroid.data()));
        ASSERT_EQ(OB_SUCCESS, fast_computer.init(
            allocator, metric, params, query.data(), centroid.data()));

        const int64_t base_size = encoder.code_size();
        const int64_t meta_offset = metadata_offset(dim);
        std::vector<uint8_t> base_codes(vector_count * base_size);
        std::vector<float> vector(dim);
        float residual_norms[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        float corrections[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        float raw_norms[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        for (int64_t i = 0; i < vector_count; ++i) {
          for (int64_t j = 0; j < dim; ++j) {
            vector[j] = 0 == i
                ? centroid[j]
                : static_cast<float>((i * 29 + j * 31 + 7) % 37 - 18) * 0.07F;
          }
          uint8_t *base_code = base_codes.data() + i * base_size;
          ASSERT_EQ(OB_SUCCESS, encoder.encode(
              vector.data(), centroid.data(), {base_code, base_size}));
          load_metadata(base_code, meta_offset,
              residual_norms[i], corrections[i], raw_norms[i]);
        }

        int64_t packed_size = 0;
        ASSERT_EQ(OB_SUCCESS,
            ObRaBitQFastScanEncoder::calc_packed_size(dim, vector_count, packed_size));
        std::vector<uint8_t> packed_codes(packed_size);
        ASSERT_EQ(OB_SUCCESS, ObRaBitQFastScanEncoder::pack_codes(
            base_codes.data(), base_size, vector_count, dim,
            {packed_codes.data(), packed_size}));

        float metrics_out[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        float lower_bounds[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        float upper_bounds[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        float lower_bounds_only[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
        ASSERT_EQ(OB_SUCCESS, fast_computer.compute_bounds_batch(
            {packed_codes.data(), packed_size}, residual_norms, corrections,
            raw_norms, vector_count, metrics_out, lower_bounds, upper_bounds));
        ASSERT_EQ(OB_SUCCESS, fast_computer.compute_lower_bounds_batch(
            {packed_codes.data(), packed_size}, residual_norms, corrections,
            raw_norms, vector_count, lower_bounds_only));
        ObQuantizationBuffer row_codes[32];
        float row_metrics[32], row_lower[32];
        for (int64_t i = 0; i < vector_count; ++i) {
          row_codes[i] = {base_codes.data() + i * base_size, base_size};
        }
        ASSERT_EQ(OB_SUCCESS, scalar_computer.compute_batch(row_codes, vector_count, row_metrics));
        ASSERT_EQ(OB_SUCCESS, scalar_computer.compute_lower_bounds_batch(row_codes, vector_count, row_lower));
        for (int64_t i = 0; i < vector_count; ++i) {
          EXPECT_FLOAT_EQ(lower_bounds[i], lower_bounds_only[i]);
          float scalar_metric = 0.0F;
          float scalar_lower = 0.0F;
          float scalar_upper = 0.0F;
          ASSERT_EQ(OB_SUCCESS, scalar_computer.compute_bounds(
              {base_codes.data() + i * base_size, base_size},
              scalar_metric, scalar_lower, scalar_upper));
          EXPECT_FLOAT_EQ(scalar_metric, row_metrics[i]);
          EXPECT_FLOAT_EQ(scalar_lower, row_lower[i]);
          expect_metric_near_or_equal(scalar_metric, metrics_out[i]);
          if (scalar_lower == scalar_upper
              || -FLT_MAX == scalar_lower || FLT_MAX == scalar_upper) {
            expect_metric_near_or_equal(scalar_lower, lower_bounds[i]);
            expect_metric_near_or_equal(scalar_upper, upper_bounds[i]);
          } else {
            const float tolerance = fast_scan_metric_tolerance(scalar_metric);
            EXPECT_LE(lower_bounds[i], scalar_lower + tolerance);
            EXPECT_GE(upper_bounds[i], scalar_upper - tolerance);
          }
        }
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, PackedAccumulationHandlesLargeSumsTailsAndRowOrder)
{
  const int64_t batch_size = ObRaBitQFastScanConstants::BATCH_SIZE;
  const ObRaBitQFastScanSimdOps *kernels[] = {
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::Default),
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX2),
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX512),
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::NEON),
      &get_rabitq_fast_scan_simd_ops()};
  // Start above the uint16 limit; also cover empty input, short tails, both
  // sides of former uint16 limits, and large inputs with odd tails.
  const int64_t byte_counts[] = {
      512, 0, 1, 2, 3, 4, 31, 32, 33, 96, 127, 128, 129,
      255, 256, 257, 258, 259, 511, 513, 2048, 2049};
  uint32_t random_state = 0x5387231U;
  const auto next_byte = [&random_state]() -> uint8_t {
    random_state ^= random_state << 13;
    random_state ^= random_state >> 17;
    random_state ^= random_state << 5;
    return static_cast<uint8_t>(random_state);
  };
  for (const int64_t bytes : byte_counts) {
    for (int pattern = 0; pattern < 3; ++pattern) {
      SCOPED_TRACE(::testing::Message() << "bytes=" << bytes << ", pattern=" << pattern);
      std::vector<uint8_t> rows(batch_size * bytes);
      for (auto &value : rows) { value = next_byte(); }
      std::vector<uint8_t> reference_packed;
      pack_codes_reference(rows.data(), bytes, batch_size, bytes * 8, reference_packed);
      // Offset every input by one byte: none of the kernels may require alignment.
      std::vector<uint8_t> packed_storage(batch_size * bytes + 1);
      std::vector<uint8_t> low_storage(batch_size * bytes + 1);
      std::vector<uint8_t> high_storage(batch_size * bytes + 1);
      uint8_t *packed = packed_storage.data() + 1;
      uint8_t *low_luts = low_storage.data() + 1;
      uint8_t *high_luts = high_storage.data() + 1;
      std::copy(reference_packed.begin(), reference_packed.end(), packed);
      for (int64_t i = 0; i < batch_size * bytes; ++i) {
        // All 255 stresses carries/overflow; complementary planes and random
        // entries detect swapped rows, nibble groups, and LUT byte planes.
        low_luts[i] = 0 == pattern ? UINT8_MAX
            : (1 == pattern ? (i % 3 == 0 ? 0 : UINT8_MAX) : next_byte());
        high_luts[i] = 0 == pattern ? UINT8_MAX
            : (1 == pattern ? UINT8_MAX - low_luts[i] : next_byte());
      }
      uint32_t expected_low[batch_size] = {};
      uint32_t expected_high[batch_size] = {};
      for (int64_t row = 0; row < batch_size; ++row) {
        for (int64_t byte = 0; byte < bytes; ++byte) {
          const uint8_t code = rows[row * bytes + byte];
          const int64_t first = byte * 32 + (code & 15);
          const int64_t second = byte * 32 + 16 + (code >> 4);
          expected_low[row] += low_luts[first] + low_luts[second];
          expected_high[row] += high_luts[first] + high_luts[second];
        }
      }
      for (const ObRaBitQFastScanSimdOps *ops : kernels) {
        if (nullptr != ops) {
          SCOPED_TRACE(::testing::Message() << "kernel=" << ops);
          const uint32_t sentinel = 0xdeadbeefU;
          std::vector<uint32_t> low(batch_size + 2, sentinel);
          std::vector<uint32_t> high(batch_size + 2, sentinel);
          ops->accumulate_lut_pairs(
              packed, low_luts, high_luts, bytes, low.data() + 1, high.data() + 1);
          for (int64_t row = 0; row < batch_size; ++row) {
            ASSERT_EQ(expected_low[row], low[row + 1]) << "row=" << row;
            ASSERT_EQ(expected_high[row], high[row + 1]) << "row=" << row;
          }
          EXPECT_EQ(sentinel, low.front());
          EXPECT_EQ(sentinel, low.back());
          EXPECT_EQ(sentinel, high.front());
          EXPECT_EQ(sentinel, high.back());
        }
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, PackedAccumulationTailStopsAtPageBoundary)
{
  const int64_t page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GE(page_size, 32);
  const size_t mapping_size = page_size * 6;
  void *mapping = mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(MAP_FAILED, mapping);
  const auto unmap = [mapping_size](void *address) { munmap(address, mapping_size); };
  const std::unique_ptr<void, decltype(unmap)> mapping_guard(mapping, unmap);
  uint8_t *base = static_cast<uint8_t *>(mapping);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(0, mprotect(base + (2 * i + 1) * page_size, page_size, PROT_NONE));
  }
  // Each input has exactly one readable column before an inaccessible page.
  uint8_t *packed = base + page_size - 32;
  uint8_t *low_luts = base + 3 * page_size - 32;
  uint8_t *high_luts = base + 5 * page_size - 32;
  MEMSET(packed, 0, 32);
  MEMSET(low_luts, UINT8_MAX, 32);
  MEMSET(high_luts, UINT8_MAX, 32);
  for (const ObRaBitQFastScanSimdOps *ops : {
       get_rabitq_fast_scan_simd_ops(common::ObTargetArch::Default),
       get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX2),
       get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX512),
       get_rabitq_fast_scan_simd_ops(common::ObTargetArch::NEON),
       &get_rabitq_fast_scan_simd_ops()}) {
    if (nullptr != ops) {
      SCOPED_TRACE(::testing::Message() << "kernel=" << ops);
      uint32_t low[32], high[32];
      ops->accumulate_lut_pairs(packed, low_luts, high_luts, 1, low, high);
      for (int row = 0; row < 32; ++row) {
        EXPECT_EQ(2 * UINT8_MAX, low[row]);
        EXPECT_EQ(2 * UINT8_MAX, high[row]);
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, SelectsVnniOnlyWhenSupported)
{
  const auto &best = get_rabitq_fast_scan_simd_ops();
#if OB_USE_MULTITARGET_CODE
  const auto *bw = get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX512);
  if (nullptr != bw) {
    if (__builtin_cpu_supports("avx512vbmi") && __builtin_cpu_supports("avx512vnni")) {
      EXPECT_NE(bw->accumulate_lut_pairs, best.accumulate_lut_pairs);
    } else {
      EXPECT_EQ(bw->accumulate_lut_pairs, best.accumulate_lut_pairs);
    }
  }
#endif
  EXPECT_NE(nullptr, best.accumulate_lut_pairs);
}

TEST(TestRaBitQFastScanComputer, SimdKernelsMatchGeneric)
{
  const ObRaBitQFastScanSimdOps *generic =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, generic);
  ASSERT_NE(nullptr, generic->build_luts);
  ASSERT_NE(nullptr, generic->quantize_luts);
  ASSERT_NE(nullptr, generic->accumulate_lut_pairs);
  ASSERT_NE(nullptr, generic->recover_metrics);
  ASSERT_NE(nullptr, generic->compute_lower_bounds);
  const common::ObTargetArch arches[] = {
      common::ObTargetArch::AVX2, common::ObTargetArch::AVX512,
      common::ObTargetArch::NEON};
  const int64_t dims[] = {1, 7, 8, 9, 63, 64, 65, 768};
  for (const int64_t dim : dims) {
    SCOPED_TRACE(::testing::Message() << "dim=" << dim);
    const int64_t byte_count = code_bytes(dim);
    std::vector<float> residual(dim);
    std::vector<float> expected(byte_count * 32);
    std::vector<float> actual(byte_count * 32);
    for (int64_t i = 0; i < dim; ++i) {
      residual[i] = static_cast<float>((i * 37 + dim * 11) % 101 - 50) / 101.0F;
    }
    const float positive = calc_inv_sqrt_dim(dim);
    const float negative = -positive;
    float expected_min = 0.0F;
    float expected_max = 0.0F;
    generic->build_luts(residual.data(), dim, positive, negative,
                        expected.data(), expected_min, expected_max);
    for (const common::ObTargetArch arch : arches) {
      const ObRaBitQFastScanSimdOps *ops = get_rabitq_fast_scan_simd_ops(arch);
#if OB_USE_MULTITARGET_CODE
      if (common::is_arch_supported(arch)) {
        ASSERT_NE(nullptr, ops);
      }
#endif
      if (nullptr != ops) {
        SCOPED_TRACE(::testing::Message() << "arch=" << static_cast<int>(arch));
        EXPECT_NE(generic->build_luts, ops->build_luts);
        float actual_min = 0.0F;
        float actual_max = 0.0F;
        ops->build_luts(residual.data(), dim, positive, negative,
                        actual.data(), actual_min, actual_max);
        EXPECT_NEAR(expected_min, actual_min, 1e-6F);
        EXPECT_NEAR(expected_max, actual_max, 1e-6F);
        for (int64_t i = 0; i < byte_count * 32; ++i) {
          EXPECT_NEAR(expected[i], actual[i], 1e-6F) << "lut_index=" << i;
        }
        const float lut_scale = expected_max > expected_min
            ? (expected_max - expected_min) / static_cast<float>(UINT16_MAX)
            : 0.0F;
        std::vector<uint8_t> expected_low(byte_count * 32);
        std::vector<uint8_t> expected_high(byte_count * 32);
        std::vector<uint8_t> actual_low(byte_count * 32);
        std::vector<uint8_t> actual_high(byte_count * 32);
        generic->quantize_luts(
            expected.data(), byte_count * 32, expected_min, lut_scale,
            expected_low.data(), expected_high.data());
        ops->quantize_luts(
            expected.data(), byte_count * 32, expected_min, lut_scale,
            actual_low.data(), actual_high.data());
        EXPECT_EQ(expected_low, actual_low);
        EXPECT_EQ(expected_high, actual_high);
      }
    }
  }
  const int64_t code_byte_counts[] = {
      1, 2, 3, 8, 31, 32, 33, 96, 128, 129, 256, 257};
  for (const int64_t code_byte_count : code_byte_counts) {
    SCOPED_TRACE(::testing::Message() << "code_bytes=" << code_byte_count);
    std::vector<uint8_t> packed_codes(code_byte_count * 32);
    std::vector<uint8_t> luts(code_byte_count * 32);
    std::vector<uint8_t> high_luts(code_byte_count * 32);
    for (int64_t i = 0; i < code_byte_count * 32; ++i) {
      packed_codes[i] = static_cast<uint8_t>((i * 73 + code_byte_count * 19) & UINT8_MAX);
      luts[i] = static_cast<uint8_t>((i * 47 + code_byte_count * 31) & UINT8_MAX);
      high_luts[i] = static_cast<uint8_t>((i * 29 + code_byte_count * 41) & UINT8_MAX);
    }
    uint32_t expected[ObRaBitQFastScanComputer::BATCH_SIZE];
    uint32_t expected_high[ObRaBitQFastScanComputer::BATCH_SIZE];
    generic->accumulate_lut_pairs(
        packed_codes.data(), luts.data(), high_luts.data(),
        code_byte_count, expected, expected_high);
    for (const common::ObTargetArch arch : arches) {
      const ObRaBitQFastScanSimdOps *ops = get_rabitq_fast_scan_simd_ops(arch);
#if OB_USE_MULTITARGET_CODE
      if (common::is_arch_supported(arch)) {
        ASSERT_NE(nullptr, ops);
      }
#endif
      if (nullptr != ops) {
        SCOPED_TRACE(::testing::Message() << "arch=" << static_cast<int>(arch));
        EXPECT_NE(generic->accumulate_lut_pairs, ops->accumulate_lut_pairs);
        uint32_t actual_low_pair[ObRaBitQFastScanComputer::BATCH_SIZE];
        uint32_t actual_high_pair[ObRaBitQFastScanComputer::BATCH_SIZE];
        ops->accumulate_lut_pairs(
            packed_codes.data(), luts.data(), high_luts.data(),
            code_byte_count, actual_low_pair, actual_high_pair);
        for (int64_t lane = 0;
             lane < ObRaBitQFastScanComputer::BATCH_SIZE;
             ++lane) {
          EXPECT_EQ(expected[lane], actual_low_pair[lane]) << "pair_lane=" << lane;
          EXPECT_EQ(expected_high[lane], actual_high_pair[lane]) << "pair_lane=" << lane;
        }
      }
    }
  }


  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  const int64_t simd_dim = 768;
  ObArenaAllocator simd_allocator("FastScanSimd");
  std::vector<float> simd_query(simd_dim, 0.0F);
  std::vector<float> simd_centroid(simd_dim, 0.0F);
  simd_query[0] = 2.5F;
  simd_centroid[0] = 1.25F;
  const ObQuantizationParams simd_params = {
      simd_dim, 1, {}, calc_inv_sqrt_dim(simd_dim)};
  const int64_t metric_counts[] = {7, 16, 23, 32};
  for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
    ObRaBitQFastScanComputer computer;
    ASSERT_EQ(OB_SUCCESS, computer.init(simd_allocator, metric, simd_params,
        simd_query.data(), simd_centroid.data()));
    for (const int64_t vector_count : metric_counts) {
      SCOPED_TRACE(::testing::Message()
          << "recover metric=" << metric << ", vector_count=" << vector_count);
      uint32_t low_byte_sums[ObRaBitQFastScanComputer::BATCH_SIZE];
      uint32_t high_byte_sums[ObRaBitQFastScanComputer::BATCH_SIZE];
      float residual_norms[ObRaBitQFastScanComputer::BATCH_SIZE];
      float corrections[ObRaBitQFastScanComputer::BATCH_SIZE];
      float raw_norms[ObRaBitQFastScanComputer::BATCH_SIZE];
      float expected[ObRaBitQFastScanComputer::BATCH_SIZE];
      float actual[ObRaBitQFastScanComputer::BATCH_SIZE];
      for (int64_t i = 0; i < ObRaBitQFastScanComputer::BATCH_SIZE; ++i) {
        low_byte_sums[i] = static_cast<uint32_t>(i * 97 + 13);
        high_byte_sums[i] = static_cast<uint32_t>(i * 31 + 7);
        residual_norms[i] = static_cast<float>(i + 1) * 0.07F;
        corrections[i] = 0 == i % 11
            ? 0.0F : static_cast<float>((i % 9) + 1) * (0 == i % 2 ? 0.09F : -0.09F);
        raw_norms[i] = 0 == i % 13 ? 0.0F : static_cast<float>(i + 3) * 0.05F;
      }
      ASSERT_EQ(-1, generic->recover_metrics(
          low_byte_sums, high_byte_sums, residual_norms, corrections, raw_norms,
          vector_count, computer, expected));
      for (const common::ObTargetArch arch : arches) {
        const ObRaBitQFastScanSimdOps *ops = get_rabitq_fast_scan_simd_ops(arch);
        if (nullptr != ops) {
          SCOPED_TRACE(::testing::Message() << "arch=" << static_cast<int>(arch));
          ASSERT_NE(nullptr, ops->recover_metrics);
          EXPECT_NE(generic->recover_metrics, ops->recover_metrics);
          ASSERT_EQ(-1, ops->recover_metrics(
              low_byte_sums, high_byte_sums, residual_norms, corrections, raw_norms,
              vector_count, computer, actual));
          for (int64_t i = 0; i < vector_count; ++i) {
            EXPECT_NEAR(expected[i], actual[i],
                1e-5F * std::max(1.0F, std::abs(expected[i]))) << "lane=" << i;
          }
          residual_norms[17] = -1.0F;
          EXPECT_EQ(17, ops->recover_metrics(
              low_byte_sums, high_byte_sums, residual_norms, corrections, raw_norms,
              ObRaBitQFastScanComputer::BATCH_SIZE, computer, actual));
          residual_norms[17] = 1.26F;
        }
      }
    }
  }

  const int64_t lower_bound_counts[] = {7, 16, 23, 32};
  for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
    ObRaBitQFastScanComputer computer;
    ASSERT_EQ(OB_SUCCESS, computer.init(simd_allocator, metric, simd_params,
        simd_query.data(), simd_centroid.data()));
    for (const int64_t vector_count : lower_bound_counts) {
      SCOPED_TRACE(::testing::Message()
          << "metric=" << metric << ", vector_count=" << vector_count);
      float metric_values[ObRaBitQFastScanComputer::BATCH_SIZE];
      float residual_norms[ObRaBitQFastScanComputer::BATCH_SIZE];
      float corrections[ObRaBitQFastScanComputer::BATCH_SIZE];
      float raw_norms[ObRaBitQFastScanComputer::BATCH_SIZE];
      float expected[ObRaBitQFastScanComputer::BATCH_SIZE];
      float actual[ObRaBitQFastScanComputer::BATCH_SIZE];
      for (int64_t i = 0; i < ObRaBitQFastScanComputer::BATCH_SIZE; ++i) {
        metric_values[i] = static_cast<float>(i - 11) * 0.13F;
        residual_norms[i] = static_cast<float>(i + 1) * 0.07F;
        corrections[i] = 0 == i % 11
            ? 0.0F : static_cast<float>((i % 9) + 1) * (0 == i % 2 ? 0.09F : -0.09F);
        raw_norms[i] = 0 == i % 13 ? 0.0F : static_cast<float>(i + 3) * 0.05F;
      }
      generic->compute_lower_bounds(metric_values, residual_norms, corrections,
          raw_norms, vector_count, computer, expected);
      for (const common::ObTargetArch arch : arches) {
        const ObRaBitQFastScanSimdOps *ops = get_rabitq_fast_scan_simd_ops(arch);
        if (nullptr != ops) {
          SCOPED_TRACE(::testing::Message() << "arch=" << static_cast<int>(arch));
          ASSERT_NE(nullptr, ops->compute_lower_bounds);
          if (common::ObTargetArch::NEON != arch) {
            EXPECT_NE(generic->compute_lower_bounds, ops->compute_lower_bounds);
          }
          ops->compute_lower_bounds(metric_values, residual_norms, corrections,
              raw_norms, vector_count, computer, actual);
          for (int64_t i = 0; i < vector_count; ++i) {
            if (-FLT_MAX == expected[i]) {
              EXPECT_FLOAT_EQ(expected[i], actual[i]) << "lane=" << i;
            } else {
              EXPECT_NEAR(expected[i], actual[i],
                  1e-5F * std::max(1.0F, std::abs(expected[i]))) << "lane=" << i;
            }
          }
        }
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, NeonQuantizeLutsMatchesScalarAtHalfBoundaries)
{
  const ObRaBitQFastScanSimdOps *neon =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::NEON);
  if (nullptr != neon) {
    const ObRaBitQFastScanSimdOps *scalar =
        get_rabitq_fast_scan_simd_ops(common::ObTargetArch::Default);
    ASSERT_NE(nullptr, scalar);
    const float values[] = {
        -1.0F, 0.0F,
        std::nextafter(0.5F, 0.0F), 0.5F, std::nextafter(0.5F, 1.0F),
        std::nextafter(1.5F, 1.0F), 1.5F, std::nextafter(1.5F, 2.0F),
        std::nextafter(65534.5F, 0.0F), 65534.5F,
        std::nextafter(65534.5F, 65535.0F), 65535.0F, 65536.0F};
    constexpr int64_t count = sizeof(values) / sizeof(values[0]);
    uint8_t scalar_low[count], scalar_high[count];
    uint8_t neon_low[count], neon_high[count];
    scalar->quantize_luts(values, count, 0.0F, 1.0F, scalar_low, scalar_high);
    neon->quantize_luts(values, count, 0.0F, 1.0F, neon_low, neon_high);
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(scalar_low[i], neon_low[i]) << "lut_index=" << i;
      EXPECT_EQ(scalar_high[i], neon_high[i]) << "lut_index=" << i;
    }
  }
}

TEST(TestRaBitQFastScanComputer, SimdResolverReturnsStableSelectedOps)
{
  const ObRaBitQFastScanSimdOps *avx512_ops =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX512);
  const ObRaBitQFastScanSimdOps *avx2_ops =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::AVX2);
  const ObRaBitQFastScanSimdOps *neon_ops =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::NEON);
  const ObRaBitQFastScanSimdOps *default_ops =
      get_rabitq_fast_scan_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, default_ops);
  const ObRaBitQFastScanSimdOps *expected_ops = nullptr != avx512_ops
      ? avx512_ops : (nullptr != avx2_ops ? avx2_ops
                                         : (nullptr != neon_ops ? neon_ops : default_ops));

  const ObRaBitQFastScanSimdOps *selected_ops = &get_rabitq_fast_scan_simd_ops();
  EXPECT_EQ(selected_ops, &get_rabitq_fast_scan_simd_ops());
  EXPECT_EQ(expected_ops->build_luts, selected_ops->build_luts);
  EXPECT_EQ(expected_ops->quantize_luts, selected_ops->quantize_luts);
  EXPECT_EQ(expected_ops->recover_metrics, selected_ops->recover_metrics);
  EXPECT_EQ(expected_ops->compute_lower_bounds, selected_ops->compute_lower_bounds);
  if (nullptr == avx512_ops) {
    EXPECT_EQ(expected_ops, selected_ops);
  }
  EXPECT_EQ(nullptr, get_rabitq_fast_scan_simd_ops(
      static_cast<common::ObTargetArch>(INT_MAX)));
}

TEST(TestRaBitQFastScanComputer, DegenerateQueriesMatchScalar)
{
  const int64_t dim = 9;
  const int64_t vector_count = 2;
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t query_case = 0; query_case < 2; ++query_case) {
    std::vector<float> query(dim, 0.0F);
    std::vector<float> centroid(dim, 0.0F);
    if (1 == query_case) {
      for (int64_t i = 0; i < dim; ++i) {
        query[i] = static_cast<float>(i - 4) * 0.25F;
        centroid[i] = query[i];
      }
    }
    const ObQuantizationParams params = {dim, 1, {}, calc_inv_sqrt_dim(dim)};
    for (const share::ObVectorIndexDistAlgorithm metric : metrics) {
      SCOPED_TRACE(::testing::Message()
          << "query_case=" << query_case << ", metric=" << metric);
      ObArenaAllocator allocator("RaBQFastDeg");
      ObQuantizationEncoder encoder;
      ObQuantizationComputer scalar_computer;
      ObRaBitQFastScanComputer fast_computer;
      ASSERT_EQ(OB_SUCCESS, encoder.init(
          allocator, ObQuantizationType::RABITQ, metric, params));
      ASSERT_EQ(OB_SUCCESS, scalar_computer.init(
          allocator, ObQuantizationType::RABITQ, metric,
          params, query.data(), centroid.data()));
      ASSERT_EQ(OB_SUCCESS, fast_computer.init(
          allocator, metric, params, query.data(), centroid.data()));

      const int64_t base_size = encoder.code_size();
      const int64_t meta_offset = metadata_offset(dim);
      std::vector<uint8_t> base_codes(vector_count * base_size);
      float residual_norms[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      float corrections[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      float raw_norms[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      std::vector<float> vector(dim);
      for (int64_t row = 0; row < vector_count; ++row) {
        for (int64_t i = 0; i < dim; ++i) {
          vector[i] = 0 == row
              ? centroid[i] : static_cast<float>(i - 3) * 0.17F;
        }
        uint8_t *base_code = base_codes.data() + row * base_size;
        ASSERT_EQ(OB_SUCCESS, encoder.encode(
            vector.data(), centroid.data(), {base_code, base_size}));
        load_metadata(base_code, meta_offset,
            residual_norms[row], corrections[row], raw_norms[row]);
      }

      int64_t packed_size = 0;
      ASSERT_EQ(OB_SUCCESS,
          ObRaBitQFastScanEncoder::calc_packed_size(dim, vector_count, packed_size));
      std::vector<uint8_t> packed_codes(packed_size);
      ASSERT_EQ(OB_SUCCESS, ObRaBitQFastScanEncoder::pack_codes(
          base_codes.data(), base_size, vector_count, dim,
          {packed_codes.data(), packed_size}));
      float actual_metrics[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      float actual_lower[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      float actual_upper[ObRaBitQFastScanComputer::BATCH_SIZE] = {};
      ASSERT_EQ(OB_SUCCESS, fast_computer.compute_bounds_batch(
          {packed_codes.data(), packed_size}, residual_norms, corrections,
          raw_norms, vector_count, actual_metrics, actual_lower, actual_upper));
      for (int64_t row = 0; row < vector_count; ++row) {
        float expected_metric = 0.0F;
        float expected_lower = 0.0F;
        float expected_upper = 0.0F;
        ASSERT_EQ(OB_SUCCESS, scalar_computer.compute_bounds(
            {base_codes.data() + row * base_size, base_size},
            expected_metric, expected_lower, expected_upper));
        expect_metric_near_or_equal(expected_metric, actual_metrics[row]);
        expect_metric_near_or_equal(expected_lower, actual_lower[row]);
        expect_metric_near_or_equal(expected_upper, actual_upper[row]);
      }
    }
  }
}

TEST(TestRaBitQFastScanComputer, ValidatesStateAndBatchArguments)
{
  ObArenaAllocator allocator("RaBQFastArgs");
  ObRaBitQFastScanComputer computer;
  const ObQuantizationParams params = {8, 1, {}, calc_inv_sqrt_dim(8)};
  const float query[8] = {};
  const float centroid[8] = {};
  uint8_t packed[32] = {};
  float residual_norms[32] = {};
  float corrections[32] = {};
  float raw_norms[32] = {};
  float values[32] = {};

  EXPECT_EQ(OB_NOT_INIT, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, values));
  ASSERT_EQ(OB_SUCCESS, computer.init(
      allocator, share::VIDA_L2, params, query, centroid));
  EXPECT_EQ(OB_INIT_TWICE, computer.init(
      allocator, share::VIDA_L2, params, query, centroid));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 0, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 33, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(
      {packed, sizeof(packed)}, nullptr, corrections, raw_norms, 1, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, nullptr));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, computer.compute_batch(
      {packed, sizeof(packed) - 1}, residual_norms, corrections, raw_norms, 1, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_bounds_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1,
      values, nullptr, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.compute_lower_bounds_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, nullptr));
  residual_norms[0] = -1.0F;
  EXPECT_EQ(OB_ERR_UNEXPECTED, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, values));
  residual_norms[0] = 0.0F;
  raw_norms[0] = -1.0F;
  EXPECT_EQ(OB_ERR_UNEXPECTED, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, values));

  computer.reset();
  EXPECT_EQ(OB_NOT_INIT, computer.compute_batch(
      {packed, sizeof(packed)}, residual_norms, corrections, raw_norms, 1, values));
  ASSERT_EQ(OB_SUCCESS, computer.init(
      allocator, share::VIDA_L2, params, query, centroid));
}

TEST(TestRaBitQFastScanComputer, ValidatesInitArguments)
{
  ObArenaAllocator allocator("RaBQFastInit");
  ObRaBitQFastScanComputer computer;
  const float values[8] = {};
  ObQuantizationParams params = {8, 1, {}, calc_inv_sqrt_dim(8)};

  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(
      allocator, share::VIDA_MAX, params, values, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(
      allocator, share::VIDA_L2, params, nullptr, values));
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(
      allocator, share::VIDA_L2, params, values, nullptr));
  params.bits_ = 2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(
      allocator, share::VIDA_L2, params, values, values));
  params.bits_ = 1;
  params.dim_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(
      allocator, share::VIDA_L2, params, values, values));
}

TEST(TestRaBitQFastScanEncoder, ValidatesArgumentsAndCapacity)
{
  uint8_t source[2] = {0x12, 0x34};
  uint8_t packed[64] = {};
  int64_t size = 123;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObRaBitQFastScanEncoder::calc_packed_size(0, 1, size));
  EXPECT_EQ(0, size);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObRaBitQFastScanEncoder::calc_packed_size(8, -1, size));
  EXPECT_EQ(0, size);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObRaBitQFastScanEncoder::pack_codes(
                nullptr, 1, 1, 8, {packed, sizeof(packed)}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObRaBitQFastScanEncoder::pack_codes(
                source, 0, 1, 8, {packed, sizeof(packed)}));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObRaBitQFastScanEncoder::pack_codes(
                source, 1, 8, {nullptr, 32}));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ObRaBitQFastScanEncoder::pack_codes(
                source, 1, 8, {packed, 31}));
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObRaBitQFastScanEncoder::pack_codes(
                source, INT64_MAX, 2, 8, {packed, sizeof(packed)}));
}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_rabitq_fast_scan_encoder.log", true, false);
  oceanbase::quantization::init_arches_quietly();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
