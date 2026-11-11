/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"

#include <climits>
#include <cmath>
#include <random>

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

ObFhtKacRotator::ObFhtKacRotator()
    : signs_(nullptr), signs_size_(0)
{}

int ObFhtKacRotator::init(const uint8_t *signs, const int64_t signs_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == signs || signs_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FHT-Kac rotator signs",
             K(ret), KP(signs), K(signs_size));
  } else if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("FHT-Kac rotator is already initialized",
             K(ret), KP(signs), K(signs_size));
  } else {
    signs_ = signs;
    signs_size_ = signs_size;
  }
  return ret;
}

void ObFhtKacRotator::reset()
{
  signs_ = nullptr;
  signs_size_ = 0;
}

bool ObFhtKacRotator::is_inited() const
{
  return nullptr != signs_ && signs_size_ > 0;
}

const uint8_t *ObFhtKacRotator::signs() const
{
  return signs_;
}

int64_t ObFhtKacRotator::signs_size() const
{
  return signs_size_;
}

int ObFhtKacRotator::generate(common::ObIAllocator &allocator,
                              const int64_t dim,
                              ObFhtKacRotator &output,
                              const int32_t fht_algo_version)
{
  int ret = OB_SUCCESS;
  if (output.is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("FHT-Kac rotator output is already initialized", K(ret), K(dim));
  } else if (dim <= 0
      || CURRENT_FHT_ALGO_VERSION != fht_algo_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FHT-Kac rotator argument",
        K(ret), K(dim), K(fht_algo_version));
  } else {
    const int64_t bytes_per_round = sign_bytes_per_round(dim);
    const int64_t signs_size = ROUND_COUNT * bytes_per_round;
    uint8_t *signs_data = static_cast<uint8_t *>(allocator.alloc(signs_size));
    if (nullptr == signs_data) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate FHT-Kac rotation signs",
               K(ret), K(dim), K(signs_size));
    } else {
      // Version 1 fixes both the seed and the byte extraction rule. Do not use
      // uniform_int_distribution here: its engine-to-result mapping is not a
      // portable persistence contract across standard-library implementations.
      std::mt19937 generator(42);
      for (int64_t round = 0; round < ROUND_COUNT; ++round) {
        uint8_t *round_data = signs_data + round * bytes_per_round;
        for (int64_t i = 0; i < bytes_per_round; ++i) {
          // This is the version-1 [0, 255] mapping previously produced by
          // libstdc++ uniform_int_distribution for mt19937. Spell it out so
          // the persisted algorithm is independent of the standard library.
          round_data[i] = static_cast<uint8_t>(
              generator() >> ((sizeof(uint32_t) - 1) * CHAR_BIT));
        }
        if (dim % 8 != 0) {
          const uint8_t valid_mask =
              static_cast<uint8_t>((1U << (dim % 8)) - 1U);
          round_data[bytes_per_round - 1] &= valid_mask;
        }
      }
      output.signs_ = signs_data;
      output.signs_size_ = signs_size;
    }
  }
  return ret;
}

int ObFhtKacRotator::rotate(const int64_t dim, float *values) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dim <= 0 || nullptr == values)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FHT-Kac rotation arguments", K(ret), K(dim), KP(values));
  } else if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("FHT-Kac rotator is not initialized", K(ret), K(dim));
  } else if (OB_UNLIKELY(
                 signs_size_ != ROUND_COUNT * sign_bytes_per_round(dim))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("FHT-Kac rotator signs size does not match dimension",
             K(ret), K(dim), K(signs_size_));
  } else {
    const ObFhtKacSimdOps &simd_ops = get_fht_kac_simd_ops();
    const int64_t bytes_per_round = sign_bytes_per_round(dim);
    const int64_t trunc_dim = greatest_power_of_two(dim);
    const float factor = 1.0F / std::sqrt(static_cast<float>(trunc_dim));
    if (trunc_dim == dim) {
      for (int64_t round = 0; round < ROUND_COUNT; ++round) {
        simd_ops.apply_signs(dim,
                             signs_ + round * bytes_per_round,
                             values);
        simd_ops.fht(dim, values);
        simd_ops.scale(dim, factor, values);
      }
    } else {
      const int64_t start = dim - trunc_dim;
      for (int64_t round = 0; round < ROUND_COUNT; ++round) {
        simd_ops.apply_signs(dim,
                             signs_ + round * bytes_per_round,
                             values);
        float *fht_values = (round % 2 == 0) ? values : values + start;
        simd_ops.fht(trunc_dim, fht_values);
        simd_ops.scale(trunc_dim, factor, fht_values);
        simd_ops.kac(dim, values);
      }
      simd_ops.scale(dim, 0.25F, values);
    }
  }
  return ret;
}

int64_t ObFhtKacRotator::sign_bytes_per_round(const int64_t dim)
{
  return dim / 8 + (dim % 8 == 0 ? 0 : 1);
}

int64_t ObFhtKacRotator::greatest_power_of_two(const int64_t dim)
{
  int64_t power = 1;
  while (power <= dim / 2) {
    power <<= 1;
  }
  return power;
}

} // namespace quantization
} // namespace oceanbase
