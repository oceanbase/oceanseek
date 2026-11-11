/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <algorithm>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <vector>

#include <gtest/gtest.h>

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "common/ob_target_specific.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

namespace
{

class RecordingAllocator : public ObIAllocator
{
public:
  explicit RecordingAllocator(const bool fail = false)
      : fail_(fail), call_count_(0), requested_size_(0)
  {}

  void *alloc(const int64_t size) override
  {
    ++call_count_;
    requested_size_ = size;
    return fail_ ? nullptr : storage_;
  }

  void *alloc(const int64_t size, const lib::ObMemAttr &) override
  {
    return alloc(size);
  }

  void free(void *) override {}

  int64_t call_count() const { return call_count_; }
  int64_t requested_size() const { return requested_size_; }

private:
  bool fail_;
  int64_t call_count_;
  int64_t requested_size_;
  uint8_t storage_[64];
};

double squared_norm(const float *values, const int64_t dim)
{
  double norm = 0.0;
  for (int64_t i = 0; i < dim; ++i) {
    norm += static_cast<double>(values[i]) * values[i];
  }
  return norm;
}

void scalar_apply_signs(const int64_t dim,
                        const uint8_t *signs,
                        float *values)
{
  for (int64_t i = 0; i < dim; ++i) {
    if ((signs[i / 8] & (static_cast<uint8_t>(1U) << (i % 8))) != 0) {
      values[i] = -values[i];
    }
  }
}

void scalar_fht(const int64_t length, float *values)
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

void scalar_scale(const int64_t length,
                  const float factor,
                  float *values)
{
  for (int64_t i = 0; i < length; ++i) {
    values[i] *= factor;
  }
}

void scalar_kac(const int64_t length, float *values)
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
    values[length / 2] *= 1.4142135623730950488F;
  }
}

float simd_tolerance(const float expected)
{
  return 1e-5F * std::max(1.0F, fabsf(expected));
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

uint32_t float_bits(const float value)
{
  uint32_t bits = 0;
  memcpy(&bits, &value, sizeof(bits));
  return bits;
}

std::vector<float> make_guarded_values(const int64_t length)
{
  std::vector<float> values(length + 2, 0.0F);
  values.front() = 12345.5F;
  values.back() = -9876.25F;
  for (int64_t i = 0; i < length; ++i) {
    if (0 == i % 11) {
      values[i + 1] = 0.0F;
    } else if (1 == i % 11) {
      values[i + 1] = -0.0F;
    } else {
      const int64_t magnitude = (i * 37 + length * 13) % 53;
      values[i + 1] = static_cast<float>(magnitude - 26) * 0.125F;
    }
  }
  return values;
}

void expect_float_canaries(const std::vector<float> &values)
{
  EXPECT_EQ(float_bits(12345.5F), float_bits(values.front()));
  EXPECT_EQ(float_bits(-9876.25F), float_bits(values.back()));
}

const common::ObTargetArch SIMD_TARGETS[] = {
    common::ObTargetArch::Default,
    common::ObTargetArch::AVX2,
    common::ObTargetArch::AVX512,
    common::ObTargetArch::NEON};
const int64_t SIMD_TARGET_COUNT =
    static_cast<int64_t>(sizeof(SIMD_TARGETS) / sizeof(SIMD_TARGETS[0]));

} // namespace

TEST(TestFhtKacRotator, DefaultRotatorIsNotInitialized)
{
  ObFhtKacRotator rotator;
  EXPECT_FALSE(rotator.is_inited());
  EXPECT_EQ(nullptr, rotator.signs());
  EXPECT_EQ(0, rotator.signs_size());
}

TEST(TestFhtKacRotator, InitRetainsSignsSize)
{
  uint8_t signs[2] = {0x01, 0x02};
  ObFhtKacRotator rotator;

  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  EXPECT_EQ(sizeof(signs), rotator.signs_size());
}

TEST(TestFhtKacRotator, InitRejectsInvalidSignsArguments)
{
  uint8_t signs[1] = {};
  ObFhtKacRotator rotator;

  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.init(nullptr, sizeof(signs)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.init(signs, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.init(signs, -1));
  EXPECT_FALSE(rotator.is_inited());
  EXPECT_EQ(0, rotator.signs_size());
}

TEST(TestFhtKacRotator, RotateRejectsMismatchedSignsSize)
{
  uint8_t signs[9] = {};
  float values[9] = {};
  ObFhtKacRotator rotator;

  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, 7));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(9, values));
  rotator.reset();
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, 9));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(9, values));
}

TEST(TestFhtKacRotator, InitCopyResetAndReinitUseShallowViews)
{
  uint8_t first_signs[2] = {0x01, 0x02};
  uint8_t second_signs[3] = {0x03, 0x04, 0x05};
  ObFhtKacRotator original;

  ASSERT_EQ(OB_SUCCESS, original.init(first_signs, sizeof(first_signs)));
  EXPECT_TRUE(original.is_inited());
  EXPECT_EQ(first_signs, original.signs());
  EXPECT_EQ(sizeof(first_signs), original.signs_size());

  EXPECT_EQ(OB_INIT_TWICE, original.init(second_signs, sizeof(second_signs)));
  EXPECT_EQ(first_signs, original.signs());

  ObFhtKacRotator copy = original;
  ObFhtKacRotator assigned;
  assigned = original;
  original.reset();
  EXPECT_FALSE(original.is_inited());
  EXPECT_EQ(nullptr, original.signs());
  EXPECT_EQ(0, original.signs_size());
  EXPECT_TRUE(copy.is_inited());
  EXPECT_EQ(first_signs, copy.signs());
  EXPECT_EQ(sizeof(first_signs), copy.signs_size());
  EXPECT_TRUE(assigned.is_inited());
  EXPECT_EQ(first_signs, assigned.signs());
  EXPECT_EQ(sizeof(first_signs), assigned.signs_size());

  ASSERT_EQ(OB_SUCCESS, original.init(second_signs, sizeof(second_signs)));
  EXPECT_TRUE(original.is_inited());
  EXPECT_EQ(second_signs, original.signs());
  EXPECT_EQ(sizeof(second_signs), original.signs_size());
}

TEST(TestFhtKacRotator, InitValidatesNullBeforeExistingState)
{
  uint8_t signs[2] = {0x01, 0x02};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));

  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.init(nullptr, sizeof(signs)));
  EXPECT_TRUE(rotator.is_inited());
  EXPECT_EQ(signs, rotator.signs());
  EXPECT_EQ(sizeof(signs), rotator.signs_size());
}

TEST(TestFhtKacRotator, SimdResolverReturnsStableGenericOps)
{
  const ObFhtKacSimdOps *avx512_ops =
      get_fht_kac_simd_ops(common::ObTargetArch::AVX512);
  const ObFhtKacSimdOps *avx2_ops =
      get_fht_kac_simd_ops(common::ObTargetArch::AVX2);
  const ObFhtKacSimdOps *neon_ops =
      get_fht_kac_simd_ops(common::ObTargetArch::NEON);
  const ObFhtKacSimdOps *default_ops =
      get_fht_kac_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, default_ops);
  const ObFhtKacSimdOps *expected_ops = nullptr != avx512_ops
                                           ? avx512_ops
                                           : (nullptr != avx2_ops ? avx2_ops
                                                                 : (nullptr != neon_ops ? neon_ops : default_ops));
  const ObFhtKacSimdOps &first = get_fht_kac_simd_ops();
  const ObFhtKacSimdOps &second = get_fht_kac_simd_ops();
  EXPECT_EQ(expected_ops, &first);
  EXPECT_EQ(expected_ops, &second);
  EXPECT_EQ(&first, &second);
  ASSERT_NE(nullptr, first.apply_signs);
  ASSERT_NE(nullptr, first.fht);
  ASSERT_NE(nullptr, first.scale);
  ASSERT_NE(nullptr, first.kac);
}

TEST(TestFhtKacRotator, SimdApplySignsMatchesScalar)
{
  const int64_t dimensions[] = {
      1, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 65, 960, 1024, 1536};
  const ObFhtKacSimdOps *target_ops[SIMD_TARGET_COUNT] = {};
  for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
    target_ops[target_index] = get_fht_kac_simd_ops(SIMD_TARGETS[target_index]);
  }
  const ObFhtKacSimdOps *default_ops = target_ops[0];
  ASSERT_NE(nullptr, default_ops);
  ASSERT_NE(nullptr, default_ops->apply_signs);
  for (int64_t target_index = 1; target_index < SIMD_TARGET_COUNT; ++target_index) {
    const common::ObTargetArch target = SIMD_TARGETS[target_index];
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(target)) {
      ASSERT_NE(nullptr, target_ops[target_index]);
    }
#endif
    if (nullptr != target_ops[target_index]) {
      ASSERT_NE(nullptr, target_ops[target_index]->apply_signs);
      EXPECT_NE(default_ops->apply_signs, target_ops[target_index]->apply_signs);
    }
  }

  for (int64_t dim_index = 0;
       dim_index < static_cast<int64_t>(sizeof(dimensions) / sizeof(dimensions[0]));
       ++dim_index) {
    const int64_t dim = dimensions[dim_index];
    const int64_t sign_size = dim / 8 + (dim % 8 == 0 ? 0 : 1);
    for (int64_t pattern = 0; pattern < 4; ++pattern) {
      std::vector<uint8_t> guarded_signs(sign_size + 2, 0);
      guarded_signs.front() = 0xD3;
      guarded_signs.back() = 0x6C;
      for (int64_t byte_index = 0; byte_index < sign_size; ++byte_index) {
        if (1 == pattern) {
          guarded_signs[byte_index + 1] = 0xFF;
        } else if (2 == pattern) {
          guarded_signs[byte_index + 1] = 0xAA;
        } else if (3 == pattern) {
          guarded_signs[byte_index + 1] = static_cast<uint8_t>(
              (byte_index * 73 + dim * 29 + 0x35) & 0xFF);
        }
      }
      std::vector<float> expected = make_guarded_values(dim);
      scalar_apply_signs(dim, guarded_signs.data() + 1, expected.data() + 1);
      for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
        const common::ObTargetArch target = SIMD_TARGETS[target_index];
        const ObFhtKacSimdOps *ops = target_ops[target_index];
        SCOPED_TRACE(::testing::Message()
                     << "dim=" << dim
                     << ", pattern=" << pattern
                     << ", target=" << static_cast<uint32_t>(target));
        if (nullptr == ops) {
          continue;
        }
        std::vector<float> actual = make_guarded_values(dim);
        const std::vector<uint8_t> signs_snapshot = guarded_signs;
        ops->apply_signs(dim, guarded_signs.data() + 1, actual.data() + 1);
        expect_float_canaries(actual);
        EXPECT_EQ(0,
                  memcmp(signs_snapshot.data(),
                         guarded_signs.data(),
                         guarded_signs.size() * sizeof(uint8_t)));
        for (int64_t i = 0; i < dim; ++i) {
          EXPECT_EQ(float_bits(expected[i + 1]), float_bits(actual[i + 1]));
        }
      }
    }
  }
}

TEST(TestFhtKacRotator, SimdFhtMatchesScalar)
{
  const int64_t lengths[] = {1, 2, 4, 8, 16, 32, 64, 512, 1024};
  const ObFhtKacSimdOps *target_ops[SIMD_TARGET_COUNT] = {};
  for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
    target_ops[target_index] = get_fht_kac_simd_ops(SIMD_TARGETS[target_index]);
  }
  const ObFhtKacSimdOps *default_ops = target_ops[0];
  ASSERT_NE(nullptr, default_ops);
  ASSERT_NE(nullptr, default_ops->fht);
  for (int64_t target_index = 1; target_index < SIMD_TARGET_COUNT; ++target_index) {
    const common::ObTargetArch target = SIMD_TARGETS[target_index];
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(target)) {
      ASSERT_NE(nullptr, target_ops[target_index]);
    }
#endif
    if (nullptr != target_ops[target_index]) {
      ASSERT_NE(nullptr, target_ops[target_index]->fht);
      EXPECT_NE(default_ops->fht, target_ops[target_index]->fht);
    }
  }
  for (int64_t length_index = 0;
       length_index < static_cast<int64_t>(sizeof(lengths) / sizeof(lengths[0]));
       ++length_index) {
    const int64_t length = lengths[length_index];
    std::vector<float> expected = make_guarded_values(length);
    scalar_fht(length, expected.data() + 1);
    for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
      const common::ObTargetArch target = SIMD_TARGETS[target_index];
      const ObFhtKacSimdOps *ops = target_ops[target_index];
      SCOPED_TRACE(::testing::Message()
                   << "length=" << length
                   << ", target=" << static_cast<uint32_t>(target));
      if (nullptr == ops) {
        continue;
      }
      std::vector<float> actual = make_guarded_values(length);
      ops->fht(length, actual.data() + 1);
      expect_float_canaries(actual);
      for (int64_t i = 0; i < length; ++i) {
        EXPECT_NEAR(expected[i + 1],
                    actual[i + 1],
                    simd_tolerance(expected[i + 1]));
      }
    }
  }
}

TEST(TestFhtKacRotator, SimdScaleMatchesScalar)
{
  const int64_t lengths[] = {
      1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 960, 1024, 1536};
  const float factors[] = {0.0F, -0.75F, 1.001F};
  const ObFhtKacSimdOps *target_ops[SIMD_TARGET_COUNT] = {};
  for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
    target_ops[target_index] = get_fht_kac_simd_ops(SIMD_TARGETS[target_index]);
  }
  const ObFhtKacSimdOps *default_ops = target_ops[0];
  ASSERT_NE(nullptr, default_ops);
  ASSERT_NE(nullptr, default_ops->scale);
  for (int64_t target_index = 1; target_index < SIMD_TARGET_COUNT; ++target_index) {
    const common::ObTargetArch target = SIMD_TARGETS[target_index];
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(target)) {
      ASSERT_NE(nullptr, target_ops[target_index]);
    }
#endif
    if (nullptr != target_ops[target_index]) {
      ASSERT_NE(nullptr, target_ops[target_index]->scale);
      EXPECT_NE(default_ops->scale, target_ops[target_index]->scale);
    }
  }
  for (int64_t length_index = 0;
       length_index < static_cast<int64_t>(sizeof(lengths) / sizeof(lengths[0]));
       ++length_index) {
    const int64_t length = lengths[length_index];
    for (int64_t factor_index = 0;
         factor_index < static_cast<int64_t>(sizeof(factors) / sizeof(factors[0]));
         ++factor_index) {
      const float factor = factors[factor_index];
      std::vector<float> expected = make_guarded_values(length);
      scalar_scale(length, factor, expected.data() + 1);
      for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
        const common::ObTargetArch target = SIMD_TARGETS[target_index];
        const ObFhtKacSimdOps *ops = target_ops[target_index];
        SCOPED_TRACE(::testing::Message()
                     << "length=" << length
                     << ", factor=" << factor
                     << ", target=" << static_cast<uint32_t>(target));
        if (nullptr == ops) {
          continue;
        }
        std::vector<float> actual = make_guarded_values(length);
        ops->scale(length, factor, actual.data() + 1);
        expect_float_canaries(actual);
        for (int64_t i = 0; i < length; ++i) {
          EXPECT_EQ(float_bits(expected[i + 1]), float_bits(actual[i + 1]));
        }
      }
    }
  }
}

TEST(TestFhtKacRotator, SimdKacMatchesScalar)
{
  const int64_t lengths[] = {
      1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65,
      960, 1024, 1536};
  const ObFhtKacSimdOps *target_ops[SIMD_TARGET_COUNT] = {};
  for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
    target_ops[target_index] = get_fht_kac_simd_ops(SIMD_TARGETS[target_index]);
  }
  const ObFhtKacSimdOps *default_ops = target_ops[0];
  ASSERT_NE(nullptr, default_ops);
  ASSERT_NE(nullptr, default_ops->kac);
  for (int64_t target_index = 1; target_index < SIMD_TARGET_COUNT; ++target_index) {
    const common::ObTargetArch target = SIMD_TARGETS[target_index];
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(target)) {
      ASSERT_NE(nullptr, target_ops[target_index]);
    }
#endif
    if (nullptr != target_ops[target_index]) {
      ASSERT_NE(nullptr, target_ops[target_index]->kac);
      EXPECT_NE(default_ops->kac, target_ops[target_index]->kac);
    }
  }
  for (int64_t length_index = 0;
       length_index < static_cast<int64_t>(sizeof(lengths) / sizeof(lengths[0]));
       ++length_index) {
    const int64_t length = lengths[length_index];
    std::vector<float> expected = make_guarded_values(length);
    scalar_kac(length, expected.data() + 1);
    for (int64_t target_index = 0; target_index < SIMD_TARGET_COUNT; ++target_index) {
      const common::ObTargetArch target = SIMD_TARGETS[target_index];
      const ObFhtKacSimdOps *ops = target_ops[target_index];
      SCOPED_TRACE(::testing::Message()
                   << "length=" << length
                   << ", target=" << static_cast<uint32_t>(target));
      if (nullptr == ops) {
        continue;
      }
      std::vector<float> actual = make_guarded_values(length);
      ops->kac(length, actual.data() + 1);
      expect_float_canaries(actual);
      for (int64_t i = 0; i < length; ++i) {
        EXPECT_EQ(float_bits(expected[i + 1]), float_bits(actual[i + 1]));
      }
    }
  }
}

TEST(TestFhtKacRotator, GeneratedDimNineSignsMatchGoldenBytes)
{
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  const uint8_t expected[8] = {95, 1, 243, 0, 187, 1, 153, 0};
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(allocator, 9, rotator));
  EXPECT_EQ(1, allocator.call_count());
  EXPECT_EQ(8, allocator.requested_size());
  EXPECT_EQ(sizeof(expected), rotator.signs_size());
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(expected)); ++i) {
    EXPECT_EQ(expected[i], rotator.signs()[i]);
  }
}

TEST(TestFhtKacRotator, GeneratedDimOneSignsMatchGoldenBytes)
{
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  const uint8_t expected[4] = {1, 1, 1, 0};
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(allocator, 1, rotator));
  EXPECT_EQ(sizeof(expected), rotator.signs_size());
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(expected)); ++i) {
    EXPECT_EQ(expected[i], rotator.signs()[i]);
  }
}

TEST(TestFhtKacRotator, GeneratedDimEightSignsMatchGoldenBytes)
{
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  const uint8_t expected[4] = {95, 203, 243, 46};
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(allocator, 8, rotator));
  EXPECT_EQ(sizeof(expected), rotator.signs_size());
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(expected)); ++i) {
    EXPECT_EQ(expected[i], rotator.signs()[i]);
  }
}

TEST(TestFhtKacRotator, RepeatedGenerationProducesSameSigns)
{
  RecordingAllocator lhs_allocator;
  RecordingAllocator rhs_allocator;
  ObFhtKacRotator lhs;
  ObFhtKacRotator rhs;
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(lhs_allocator, 17, lhs));
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(rhs_allocator, 17, rhs));
  const int64_t signs_size = 4 * 3;
  EXPECT_EQ(signs_size, lhs.signs_size());
  EXPECT_EQ(signs_size, rhs.signs_size());
  for (int64_t i = 0; i < signs_size; ++i) {
    EXPECT_EQ(lhs.signs()[i], rhs.signs()[i]);
  }
}

TEST(TestFhtKacRotator, GeneratedSignsClearTailBitsInEveryRound)
{
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(allocator, 10, rotator));
  const int64_t bytes_per_round = 2;
  for (int64_t round = 0; round < 4; ++round) {
    EXPECT_EQ(0, rotator.signs()[(round + 1) * bytes_per_round - 1] & 0xFC);
  }
}

TEST(TestFhtKacRotator, NonPositiveDimDoesNotAllocateOrInitializeOutput)
{
  const int64_t invalid_dims[2] = {0, -1};
  for (int64_t i = 0; i < 2; ++i) {
    RecordingAllocator allocator;
    ObFhtKacRotator rotator;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObFhtKacRotator::generate(allocator, invalid_dims[i], rotator));
    EXPECT_EQ(0, allocator.call_count());
    EXPECT_FALSE(rotator.is_inited());
    EXPECT_EQ(nullptr, rotator.signs());
    EXPECT_EQ(0, rotator.signs_size());
  }
}

TEST(TestFhtKacRotator, GenerateRejectsUnsupportedAlgorithmVersion)
{
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObFhtKacRotator::generate(allocator, 8, rotator,
          ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION + 1));
  EXPECT_EQ(0, allocator.call_count());
  EXPECT_FALSE(rotator.is_inited());
}

TEST(TestFhtKacRotator, GenerateRejectsInitializedOutputBeforeAllocation)
{
  uint8_t sentinel_data[1] = {42};
  RecordingAllocator allocator;
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(sentinel_data, sizeof(sentinel_data)));

  EXPECT_EQ(OB_INIT_TWICE, ObFhtKacRotator::generate(allocator, 0, rotator));
  EXPECT_EQ(0, allocator.call_count());
  EXPECT_EQ(sentinel_data, rotator.signs());
  EXPECT_EQ(sizeof(sentinel_data), rotator.signs_size());
}

TEST(TestFhtKacRotator, AllocationFailureLeavesOutputUninitialized)
{
  RecordingAllocator allocator(true);
  ObFhtKacRotator rotator;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObFhtKacRotator::generate(allocator, 9, rotator));
  EXPECT_EQ(1, allocator.call_count());
  EXPECT_EQ(8, allocator.requested_size());
  EXPECT_FALSE(rotator.is_inited());
  EXPECT_EQ(nullptr, rotator.signs());
  EXPECT_EQ(0, rotator.signs_size());
}

TEST(TestFhtKacRotator, UninitializedRotateValidatesArgumentsBeforeState)
{
  ObFhtKacRotator rotator;
  float value = 1.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(0, &value));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(1, nullptr));
  EXPECT_EQ(OB_NOT_INIT, rotator.rotate(1, &value));
}

TEST(TestFhtKacRotator, RotateRejectsNonPositiveDimensionImmediately)
{
  uint8_t signs[1] = {};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  float value = 1.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(0, &value));
}

TEST(TestFhtKacRotator, RotateRejectsUninitializedRotator)
{
  ObFhtKacRotator rotator;
  float value = 1.0F;
  EXPECT_EQ(OB_NOT_INIT, rotator.rotate(1, &value));
}

TEST(TestFhtKacRotator, RotateRejectsNullValuesImmediately)
{
  uint8_t signs[1] = {};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rotator.rotate(1, nullptr));
}

TEST(TestFhtKacRotator, DimFourAllPositiveSignsAreIdentityAfterFourRounds)
{
  uint8_t signs[4] = {};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  float values[4] = {1.0F, -2.0F, 3.5F, 4.25F};
  const float expected[4] = {1.0F, -2.0F, 3.5F, 4.25F};
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(4, values));
  for (int64_t i = 0; i < 4; ++i) {
    EXPECT_NEAR(expected[i], values[i], 1e-6F);
  }
}

TEST(TestFhtKacRotator, SameExplicitSignsProduceSameOutput)
{
  uint8_t signs[4] = {0x01, 0x12, 0x04, 0x09};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  float lhs[5] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  float rhs[5] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(5, lhs));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(5, rhs));
  for (int64_t i = 0; i < 5; ++i) {
    EXPECT_FLOAT_EQ(lhs[i], rhs[i]);
  }
}

TEST(TestFhtKacRotator, DifferentExplicitSignsProduceDifferentOutput)
{
  uint8_t first_signs[4] = {0x01, 0x02, 0x04, 0x08};
  uint8_t second_signs[4] = {0x02, 0x04, 0x08, 0x10};
  ObFhtKacRotator first_rotator;
  ObFhtKacRotator second_rotator;
  ASSERT_EQ(OB_SUCCESS, first_rotator.init(first_signs, sizeof(first_signs)));
  ASSERT_EQ(OB_SUCCESS, second_rotator.init(second_signs, sizeof(second_signs)));
  float lhs[5] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  float rhs[5] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  ASSERT_EQ(OB_SUCCESS, first_rotator.rotate(5, lhs));
  ASSERT_EQ(OB_SUCCESS, second_rotator.rotate(5, rhs));
  bool differs = false;
  for (int64_t i = 0; i < 5; ++i) {
    differs = differs || lhs[i] != rhs[i];
  }
  EXPECT_TRUE(differs);
}

TEST(TestFhtKacRotator, DimensionsPreserveNormWithinTolerance)
{
  const int64_t dimensions[] = {5, 960, 1024, 1536};
  for (int64_t dimension_index = 0;
       dimension_index < static_cast<int64_t>(sizeof(dimensions) / sizeof(dimensions[0]));
       ++dimension_index) {
    const int64_t dim = dimensions[dimension_index];
    SCOPED_TRACE(::testing::Message() << "dim=" << dim);
    ObArenaAllocator allocator("FhtNorm");
    ObFhtKacRotator rotator;
    ASSERT_EQ(OB_SUCCESS, ObFhtKacRotator::generate(allocator, dim, rotator));
    std::vector<float> values(dim, 0.0F);
    for (int64_t i = 0; i < dim; ++i) {
      values[i] = static_cast<float>((i * 37 + dim * 13) % 53 - 26) * 0.125F;
    }
    const double expected_norm = squared_norm(values.data(), dim);
    ASSERT_GT(expected_norm, 0.0);
    ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, values.data()));
    EXPECT_NEAR(expected_norm,
                squared_norm(values.data(), dim),
                expected_norm * 1e-4);
  }
}

TEST(TestFhtKacRotator, DimThreeSignedRotationMatchesHandCalculatedGolden)
{
  uint8_t signs[4] = {0x01, 0x02, 0x04, 0x05};
  ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, rotator.init(signs, sizeof(signs)));
  float values[3] = {1.0F, 2.0F, 3.0F};
  const float expected[3] = {0.7544417382F, 0.2526019100F, -3.6560921677F};
  // Manual round outputs (before final /4), rounded:
  // [3.7071,-3,-2.2929] -> [7.4497,0.7071,-0.0355]
  // -> [5.8033,6.7426,5.7322] -> [3.0178,1.0104,-14.6244].
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(3, values));
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_NEAR(expected[i], values[i], 1e-5F);
  }
}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::quantization::init_arches_quietly();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
