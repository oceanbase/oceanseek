/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_fast_scan.h"

#include <algorithm>
#include <cmath>
#include <float.h>
#include <limits.h>
#include <string.h>

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

const int64_t ObRaBitQFastScanEncoder::BATCH_SIZE;
const int64_t ObRaBitQFastScanComputer::BATCH_SIZE;

namespace
{

int calc_code_bytes(const int64_t dim, int64_t &code_bytes)
{
  int ret = OB_SUCCESS;
  code_bytes = 0;
  if (OB_UNLIKELY(dim <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan dimension", K(ret), K(dim));
  } else {
    code_bytes = dim / CHAR_BIT + (0 == dim % CHAR_BIT ? 0 : 1);
  }
  return ret;
}

} // namespace

int ObRaBitQFastScanEncoder::calc_packed_size(const int64_t dim,
                                              const int64_t code_count,
                                              int64_t &packed_size)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  packed_size = 0;
  if (OB_UNLIKELY(code_count < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan code count", K(ret), K(code_count));
  } else if (OB_FAIL(calc_code_bytes(dim, code_bytes))) {
    LOG_WARN("failed to calculate RaBitQ code bytes", K(ret), K(dim));
  } else if (code_count > 0) {
    const int64_t block_count = code_count / BATCH_SIZE
                                + (0 == code_count % BATCH_SIZE ? 0 : 1);
    if (OB_UNLIKELY(code_bytes > INT64_MAX / BATCH_SIZE
                    || block_count > INT64_MAX / (code_bytes * BATCH_SIZE))) {
      ret = common::OB_SIZE_OVERFLOW;
      LOG_WARN("RaBitQ FastScan packed size overflows",
               K(ret), K(dim), K(code_count), K(code_bytes), K(block_count));
    } else {
      packed_size = block_count * code_bytes * BATCH_SIZE;
    }
  }
  return ret;
}

int ObRaBitQFastScanEncoder::pack_codes(
    const uint8_t *codes,
    const int64_t code_count,
    const int64_t dim,
    const ObQuantizationBuffer &packed_codes)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  if (OB_FAIL(calc_code_bytes(dim, code_bytes))) {
    LOG_WARN("failed to calculate tightly packed RaBitQ code stride",
             K(ret), K(dim), K(code_count));
  } else if (OB_FAIL(pack_codes(codes,
                                code_bytes,
                                code_count,
                                dim,
                                packed_codes))) {
    LOG_WARN("failed to pack tightly packed RaBitQ FastScan codes",
             K(ret), K(dim), K(code_count), K(code_bytes));
  }
  return ret;
}

int ObRaBitQFastScanEncoder::pack_codes(
    const uint8_t *codes,
    const int64_t code_stride,
    const int64_t code_count,
    const int64_t dim,
    const ObQuantizationBuffer &packed_codes)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  int64_t required_size = 0;
  if (OB_FAIL(calc_code_bytes(dim, code_bytes))) {
    LOG_WARN("failed to calculate RaBitQ FastScan source code bytes",
             K(ret), K(dim), K(code_count), K(code_stride));
  } else if (OB_FAIL(calc_packed_size(dim, code_count, required_size))) {
    LOG_WARN("failed to calculate RaBitQ FastScan destination size",
             K(ret), K(dim), K(code_count), K(code_stride));
  } else if (OB_UNLIKELY(code_stride < code_bytes)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("RaBitQ FastScan source stride is too small",
             K(ret), K(dim), K(code_count), K(code_stride), K(code_bytes));
  } else if (OB_UNLIKELY(code_count > 0
                         && code_count - 1 > (INT64_MAX - code_bytes) / code_stride)) {
    ret = common::OB_SIZE_OVERFLOW;
    LOG_WARN("RaBitQ FastScan source span overflows",
             K(ret), K(dim), K(code_count), K(code_stride), K(code_bytes));
  } else if (OB_UNLIKELY(code_count > 0 && OB_ISNULL(codes))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("RaBitQ FastScan source codes are null",
             K(ret), K(dim), K(code_count), K(code_stride));
  } else if (OB_UNLIKELY(required_size > 0 && OB_ISNULL(packed_codes.data_))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("RaBitQ FastScan destination is null",
             K(ret), K(dim), K(code_count), K(required_size));
  } else if (OB_UNLIKELY(packed_codes.size_ < required_size)) {
    ret = common::OB_BUF_NOT_ENOUGH;
    LOG_WARN("RaBitQ FastScan destination is too small",
             K(ret), K(dim), K(code_count), K(packed_codes.size_), K(required_size));
  } else if (required_size > 0) {
    MEMSET(packed_codes.data_, 0, required_size);
    const uint8_t tail_mask = 0 == dim % CHAR_BIT
                                  ? UINT8_MAX
                                  : static_cast<uint8_t>((1U << (dim % CHAR_BIT)) - 1U);
    const int64_t block_count = required_size / (code_bytes * BATCH_SIZE);
    for (int64_t block_index = 0; block_index < block_count; ++block_index) {
      const int64_t first_code = block_index * BATCH_SIZE;
      const int64_t remain = code_count - first_code;
      const int64_t codes_in_block = remain < BATCH_SIZE ? remain : BATCH_SIZE;
      uint8_t *block_output = packed_codes.data_ + block_index * code_bytes * BATCH_SIZE;
      for (int64_t byte_index = 0; byte_index < code_bytes; ++byte_index) {
        uint8_t source_bytes[BATCH_SIZE] = {};
        for (int64_t code_index = 0; code_index < codes_in_block; ++code_index) {
          const int64_t source_offset = (first_code + code_index) * code_stride + byte_index;
          source_bytes[code_index] = codes[source_offset];
        }
        if (byte_index == code_bytes - 1) {
          for (int64_t code_index = 0; code_index < codes_in_block; ++code_index) {
            source_bytes[code_index] &= tail_mask;
          }
        }

        uint8_t *column_output = block_output + byte_index * BATCH_SIZE;
        for (int64_t output_lane = 0;
             output_lane < BATCH_SIZE / ObRaBitQFastScanConstants::CODES_PER_PACKED_BYTE;
             ++output_lane) {
          const int64_t low_code = output_lane;
          const int64_t high_code = low_code + BATCH_SIZE / 2;
          column_output[output_lane] =
              static_cast<uint8_t>((source_bytes[low_code]
                                        & ObRaBitQFastScanConstants::NIBBLE_MASK)
                  | static_cast<uint8_t>((source_bytes[high_code]
                                              & ObRaBitQFastScanConstants::NIBBLE_MASK)
                      << ObRaBitQFastScanConstants::NIBBLE_BITS));
          column_output[output_lane + BATCH_SIZE / 2] =
              static_cast<uint8_t>((source_bytes[low_code]
                      >> ObRaBitQFastScanConstants::NIBBLE_BITS)
                  | (source_bytes[high_code]
                      & ObRaBitQFastScanConstants::HIGH_NIBBLE_MASK));
        }
      }
    }
  }
  return ret;
}

ObRaBitQFastScanComputer::ObRaBitQFastScanComputer()
  : allocator_(nullptr),
    metric_(share::VIDA_MAX),
    dim_(0),
    code_bytes_(0),
    query_residual_norm_(0.0F),
    query_raw_norm_(0.0F),
    lut_offset_(0.0F),
    lut_scale_(0.0F),
    lut_dot_error_(0.0F),
    dim_error_scale_(0.0F),
    quantized_luts_(nullptr),
    simd_ops_(nullptr),
    is_inited_(false)
{}

ObRaBitQFastScanComputer::~ObRaBitQFastScanComputer()
{
  reset();
}

void ObRaBitQFastScanComputer::reset()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(quantized_luts_)) {
    allocator_->free_align(quantized_luts_);
  }
  allocator_ = nullptr;
  metric_ = share::VIDA_MAX;
  dim_ = 0;
  code_bytes_ = 0;
  query_residual_norm_ = 0.0F;
  query_raw_norm_ = 0.0F;
  lut_offset_ = 0.0F;
  lut_scale_ = 0.0F;
  lut_dot_error_ = 0.0F;
  dim_error_scale_ = 0.0F;
  quantized_luts_ = nullptr;
  simd_ops_ = nullptr;
  is_inited_ = false;
}

int ObRaBitQFastScanComputer::init(
    common::ObIAllocator &allocator,
    const share::ObVectorIndexDistAlgorithm metric,
    const ObQuantizationParams &params,
    const float *query,
    const float *centroid)
{
  int ret = OB_SUCCESS;
  int64_t code_bytes = 0;
  float *normalized_residual = nullptr;
  float *float_luts = nullptr;
  uint8_t *quantized_luts = nullptr;
  const ObRaBitQFastScanSimdOps *simd_ops = nullptr;
  float query_residual_norm = 0.0F;
  float query_raw_norm = 0.0F;
  float lut_min = FLT_MAX;
  float lut_max = -FLT_MAX;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("RaBitQ FastScan computer is already initialized", K(ret));
  } else if (OB_UNLIKELY(params.dim_ <= 0 || 1 != params.bits_
      || share::VIDA_L2 > metric || share::VIDA_COS < metric
      || OB_ISNULL(query) || OB_ISNULL(centroid))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan computer argument",
        K(ret), K(metric), K(params.dim_), K(params.bits_), KP(query), KP(centroid));
  } else if (OB_FAIL(calc_code_bytes(params.dim_, code_bytes))) {
    LOG_WARN("failed to calculate RaBitQ FastScan code bytes", K(ret), K(params.dim_));
  } else if (OB_ISNULL(normalized_residual = static_cast<float *>(
      allocator.alloc_align(params.dim_ * sizeof(float), alignof(float))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate RaBitQ FastScan query residual", K(ret), K(params.dim_));
  } else if (OB_ISNULL(float_luts = static_cast<float *>(
      allocator.alloc_align(
          code_bytes * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE * sizeof(float),
          alignof(float))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate RaBitQ FastScan float LUT", K(ret), K(code_bytes));
  } else if (OB_ISNULL(quantized_luts = static_cast<uint8_t *>(
      allocator.alloc_align(
          code_bytes * ObRaBitQFastScanConstants::QUANTIZED_LUT_SIZE_PER_CODE_BYTE,
          alignof(uint8_t))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate RaBitQ FastScan quantized LUT", K(ret), K(code_bytes));
  } else {
    for (int64_t i = 0; i < params.dim_; ++i) {
      const float residual = query[i] - centroid[i];
      normalized_residual[i] = residual;
      query_residual_norm += residual * residual;
      query_raw_norm += query[i] * query[i];
    }
    query_residual_norm = std::sqrt(query_residual_norm);
    query_raw_norm = std::sqrt(query_raw_norm);
    if (query_residual_norm > 0.0F) {
      for (int64_t i = 0; i < params.dim_; ++i) {
        normalized_residual[i] /= query_residual_norm;
      }
    }

    const float inv_sqrt_dim = calc_inv_sqrt_dim(params.dim_);
    const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
    const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
    simd_ops = &get_rabitq_fast_scan_simd_ops();
    simd_ops->build_luts(
        normalized_residual, params.dim_, positive, negative,
        float_luts, lut_min, lut_max);
    const float lut_scale = lut_max > lut_min
        ? (lut_max - lut_min) / static_cast<float>(UINT16_MAX)
        : 0.0F;
    uint8_t *low_byte_luts = quantized_luts;
    uint8_t *high_byte_luts = quantized_luts
        + code_bytes * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE;
    simd_ops->quantize_luts(
        float_luts,
        code_bytes * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE,
        lut_min, lut_scale,
        low_byte_luts, high_byte_luts);
    allocator_ = &allocator;
    metric_ = metric;
    dim_ = params.dim_;
    code_bytes_ = code_bytes;
    query_residual_norm_ = query_residual_norm;
    query_raw_norm_ = query_raw_norm;
    lut_offset_ = 2.0F * static_cast<float>(code_bytes) * lut_min;
    lut_scale_ = lut_scale;
    // Each code byte selects two independently quantized nibble LUT values.
    // Round-to-nearest contributes at most half a quantization step per LUT.
    lut_dot_error_ = static_cast<float>(code_bytes) * lut_scale;
    dim_error_scale_ = params.dim_ > 1
        ? 2.0F / std::sqrt(static_cast<float>(params.dim_) - 1.0F) : 0.0F;
    quantized_luts_ = quantized_luts;
    quantized_luts = nullptr;
    simd_ops_ = simd_ops;
    is_inited_ = true;
  }

  if (OB_NOT_NULL(normalized_residual)) {
    allocator.free_align(normalized_residual);
  }
  if (OB_NOT_NULL(float_luts)) {
    allocator.free_align(float_luts);
  }
  if (OB_NOT_NULL(quantized_luts)) {
    allocator.free_align(quantized_luts);
  }
  return ret;
}

float ObRaBitQFastScanComputer::metric_error(
    const float base_residual_norm,
    const float correction,
    const float base_raw_norm) const
{
  const float correction_squared = correction * correction;
  const float normalized_dot_error =
      dim_error_scale_
          * std::sqrt(std::max(0.0F, 1.0F - correction_squared) / correction_squared)
      + lut_dot_error_ / std::abs(correction);
  float error = query_residual_norm_ * base_residual_norm * normalized_dot_error;
  if (share::VIDA_L2 == metric_) {
    error *= 2.0F;
  } else if (share::VIDA_COS == metric_) {
    error /= query_raw_norm_ * base_raw_norm;
  }
  return error;
}

int ObRaBitQFastScanComputer::compute_batch(
    const ObQuantizationBuffer &packed_codes,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t code_count,
    float *metric_values) const
{
  int ret = OB_SUCCESS;
  const int64_t required_size = code_bytes_ * BATCH_SIZE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("RaBitQ FastScan computer is not initialized", K(ret));
  } else if (OB_UNLIKELY(code_count <= 0 || code_count > BATCH_SIZE)
      || OB_ISNULL(residual_norms) || OB_ISNULL(corrections)
      || OB_ISNULL(raw_norms) || OB_ISNULL(metric_values)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan batch",
        K(ret), K(code_count), KP(residual_norms), KP(corrections),
        KP(raw_norms), KP(metric_values));
  } else if (OB_ISNULL(packed_codes.data_)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan packed-code buffer",
        K(ret), KP(packed_codes.data_));
  } else if (OB_UNLIKELY(packed_codes.size_ < required_size)) {
    ret = common::OB_BUF_NOT_ENOUGH;
    LOG_WARN("RaBitQ FastScan packed codes are too small",
        K(ret), K(packed_codes.size_), K(required_size));
  } else {
    uint32_t low_byte_sums[BATCH_SIZE];
    uint32_t high_byte_sums[BATCH_SIZE];
    const ObRaBitQFastScanSimdOps &ops = *simd_ops_;
    ops.accumulate_lut_pairs(
        packed_codes.data_, quantized_luts_,
        quantized_luts_ + code_bytes_ * ObRaBitQFastScanConstants::LUT_VALUES_PER_CODE_BYTE,
        code_bytes_, low_byte_sums, high_byte_sums);
    const int64_t invalid_index = ops.recover_metrics(
        low_byte_sums, high_byte_sums, residual_norms, corrections, raw_norms,
        code_count, *this, metric_values);
    if (OB_UNLIKELY(invalid_index >= 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid RaBitQ FastScan metadata",
          K(ret), K(invalid_index), K(residual_norms[invalid_index]),
          K(corrections[invalid_index]), K(raw_norms[invalid_index]));
    }
  }
  return ret;
}

int ObRaBitQFastScanComputer::compute_bounds_batch(
    const ObQuantizationBuffer &packed_codes,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t code_count,
    float *metric_values,
    float *lower_bounds,
    float *upper_bounds) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lower_bounds) || OB_ISNULL(upper_bounds)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan bounds output",
        K(ret), KP(lower_bounds), KP(upper_bounds));
  } else if (OB_FAIL(compute_batch(packed_codes, residual_norms, corrections,
      raw_norms, code_count, metric_values))) {
    LOG_WARN("failed to compute RaBitQ FastScan metrics", K(ret), K(code_count));
  } else {
    for (int64_t i = 0; i < code_count; ++i) {
      if ((share::VIDA_IP == metric_ || share::VIDA_COS == metric_)
          && (std::abs(query_raw_norm_) < ObRaBitQFastScanConstants::ZERO_EPSILON
              || std::abs(raw_norms[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON)) {
        lower_bounds[i] = metric_values[i];
        upper_bounds[i] = metric_values[i];
      } else if (dim_ <= 1
                 || std::abs(corrections[i]) < ObRaBitQFastScanConstants::ZERO_EPSILON) {
        lower_bounds[i] = -FLT_MAX;
        upper_bounds[i] = FLT_MAX;
      } else {
        const float error = metric_error(
            residual_norms[i], corrections[i], raw_norms[i]);
        lower_bounds[i] = metric_values[i] - error;
        upper_bounds[i] = metric_values[i] + error;
      }
    }
  }
  return ret;
}

int ObRaBitQFastScanComputer::compute_lower_bounds_batch(
    const ObQuantizationBuffer &packed_codes,
    const float *residual_norms,
    const float *corrections,
    const float *raw_norms,
    const int64_t code_count,
    float *lower_bounds) const
{
  int ret = OB_SUCCESS;
  float metric_values[BATCH_SIZE];
  if (OB_ISNULL(lower_bounds)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ FastScan lower-bound output", K(ret), KP(lower_bounds));
  } else if (OB_FAIL(compute_batch(packed_codes, residual_norms, corrections,
      raw_norms, code_count, metric_values))) {
    LOG_WARN("failed to compute RaBitQ FastScan metrics", K(ret), K(code_count));
  } else {
    simd_ops_->compute_lower_bounds(
        metric_values, residual_norms, corrections, raw_norms, code_count,
        *this, lower_bounds);
  }
  return ret;
}

} // namespace quantization
} // namespace oceanbase
