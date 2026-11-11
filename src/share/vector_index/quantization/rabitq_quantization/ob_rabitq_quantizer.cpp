/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"

#include <algorithm>
#include <cmath>
#include <cfloat>
#include <string.h>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

bool ObRaBitQQuantizer::is_aligned(const void *pointer,
                                   const int64_t alignment)
{
  return 0 == reinterpret_cast<uintptr_t>(pointer) % alignment;
}

int64_t ObRaBitQQuantizer::align_up(const int64_t value,
                                    const int64_t alignment)
{
  return value + (alignment - value % alignment) % alignment;
}

int ObRaBitQQuantizer::calculate_encode_scratch_size(const int64_t dim,
                                                     int64_t &size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dim <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ encode scratch dimension", K(ret), K(dim));
  } else {
    size = dim * static_cast<int64_t>(sizeof(float));
  }
  return ret;
}

// base_code 保存基础向量的最终 RaBitQ 编码，内部布局如下，所有 offset 的单位均为字节。
// 编码端输入的 x 和 c 已使用查询侧的同一 rotator 预旋转；正交旋转保持模长不变。
// 三种 metric 均量化同一残差 r_x = R(x) - R(c)：
//
// base_code.data_
//   + 0                     -> uint8_t packed_codes[ceil(dim / 8)]：每个维度占 1 bit
//   + residual_norm_offset_ -> float base_residual_norm：残差 r_x = x - c 的模长
//   + correction_offset_     -> float correction：量化方向相关性的校正系数
//   + raw_norm_offset_       -> float base_raw_norm：旋转后原始基础向量 x 的模长
//   + size_                  -> 基础向量编码缓冲区末尾
//
// packed codes 之后填充到 float 对齐，再依次保存三个浮点元数据。
struct ObRaBitQQuantizer::BaseCodeLayout
{
  int64_t residual_norm_offset_;
  int64_t correction_offset_;
  int64_t raw_norm_offset_;
  int64_t size_;

  static int calculate(const int64_t dim, BaseCodeLayout &layout)
  {
    int ret = OB_SUCCESS;
    BaseCodeLayout result = {};
    if (OB_UNLIKELY(dim <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid RaBitQ base code layout dimension", K(ret), K(dim));
    } else {
      const int64_t packed_size = dim / 8 + (dim % 8 == 0 ? 0 : 1);
      result.residual_norm_offset_ = align_up(packed_size, sizeof(float));
      result.correction_offset_ = result.residual_norm_offset_ + sizeof(float);
      result.raw_norm_offset_ = result.correction_offset_ + sizeof(float);
      result.size_ = result.raw_norm_offset_ + sizeof(float);
    }
    if (OB_SUCC(ret)) {
      layout = result;
    }
    return ret;
  }
};

// query_code 是一次查询的临时预处理结果，内部布局如下，所有 offset 的单位均为字节。
// prepare_query 接收已旋转的查询 q_rot = R(q) 和 centroid c_rot = R(c)；
// 查询端和 base 端均使用旋转坐标中的残差 r = R(vector) - R(centroid)：
//
// query_code.data_
//   + 0                           -> float normalized_residual[dim]：归一化后的旋转坐标残差
//   + residual_norm_offset_       -> float residual_norm：r_q 的模长
//   + raw_norm_offset_            -> float raw_norm：旋转查询向量 q_rot 的模长
//   + size_                       -> 查询编码缓冲区末尾
//
// 各区域按 float 对齐，prepare_query 生成该布局，后续 compute 直接复用其中的数据。
struct ObRaBitQQuantizer::QueryCodeLayout
{
  int64_t residual_norm_offset_;
  int64_t raw_norm_offset_;
  int64_t size_;

  static int calculate(const int64_t dim, QueryCodeLayout &layout)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout result = {};
    if (OB_UNLIKELY(dim <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid RaBitQ query code layout dimension", K(ret), K(dim));
    } else {
      const int64_t normalized_residual_size =
          dim * static_cast<int64_t>(sizeof(float));
      result.residual_norm_offset_ = normalized_residual_size;
      result.raw_norm_offset_ = result.residual_norm_offset_ + sizeof(float);
      result.size_ = result.raw_norm_offset_ + sizeof(float);
    }
    if (OB_SUCC(ret)) {
      layout = result;
    }
    return ret;
  }
};

template <share::ObVectorIndexDistAlgorithm METRIC>
struct ObRaBitQQuantizer::MetricImpl
{
  static int encode(const ObQuantizationParams &params,
                    const float *vector,
                    const float *centroid,
                    const ObQuantizationBuffer &encode_scratch,
                    const ObQuantizationBuffer &base_code)
  {
    int ret = OB_SUCCESS;
    // vector 和编码端 centroid 必须已使用查询端相同的旋转方案完成旋转。
    // 本函数直接在旋转后的坐标空间计算残差，不再重复执行随机旋转。
    BaseCodeLayout base_layout = {};
    int64_t encode_scratch_size = 0;
    if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, base_layout))) {
      LOG_WARN("failed to calculate RaBitQ base code layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_FAIL(calculate_encode_scratch_size(params.dim_, encode_scratch_size))) {
      LOG_WARN("failed to calculate RaBitQ encode scratch size",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_UNLIKELY(params.inv_sqrt_dim_ <= 0.0F)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid RaBitQ inverse square-root dimension",
               K(ret), K(params.inv_sqrt_dim_));
    } else if (OB_UNLIKELY(nullptr == vector || nullptr == centroid
                           || nullptr == encode_scratch.data_
                           || nullptr == base_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null RaBitQ vector encode argument",
               K(ret), KP(vector), KP(centroid), KP(encode_scratch.data_),
               KP(base_code.data_));
    } else if (OB_UNLIKELY(encode_scratch.size_ < encode_scratch_size
                           || base_code.size_ < base_layout.size_)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("RaBitQ vector encode buffer is too small",
               K(ret), K(encode_scratch.size_), K(encode_scratch_size),
               K(base_code.size_), K(base_layout.size_));
    } else if (OB_UNLIKELY(!is_aligned(base_code.data_, alignof(float)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("RaBitQ base code is not float-aligned",
               K(ret), KP(base_code.data_));
    } else if (OB_UNLIKELY(!is_aligned(encode_scratch.data_, alignof(float)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("RaBitQ encode scratch is not aligned for typed workspace",
               K(ret), KP(encode_scratch.data_));
    } else {
      float *work = reinterpret_cast<float *>(encode_scratch.data_);
      float base_raw_norm = 0.0F;
      float base_residual_norm = 0.0F;
      base_raw_norm = calc_vector_norm(params.dim_, vector);
      prepare_normalized_residual(params.dim_,
                                  vector,
                                  centroid,
                                  work,
                                  base_residual_norm);
      encode_base_code(params,
                       work,
                       base_residual_norm,
                       base_raw_norm,
                       base_layout,
                       base_code.data_);
    }
    return ret;
  }

  // query 和编码端 centroid 必须已使用查询端相同的旋转方案完成旋转。
  // 本函数直接在旋转后的坐标空间计算残差，不再重复执行随机旋转。
  static int prepare_query(const ObQuantizationParams &params,
                           const float *query, const float *centroid,
                           const ObQuantizationBuffer &query_code)
  {
    return prepare_query_impl(params, query, centroid, query_code, nullptr);
  }

  static int prepare_query_with_norm(const ObQuantizationParams &params,
                                     const float *query, const float *centroid,
                                     const float query_raw_norm,
                                     const ObQuantizationBuffer &query_code)
  {
    return prepare_query_impl(params, query, centroid, query_code, &query_raw_norm);
  }

  static int prepare_query_impl(const ObQuantizationParams &params,
                           const float *query,
                           const float *centroid,
                           const ObQuantizationBuffer &query_code,
                           const float *cached_raw_norm)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, query_layout))) {
      LOG_WARN("failed to calculate RaBitQ query code layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (nullptr == query || nullptr == centroid || nullptr == query_code.data_) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null RaBitQ query preparation argument",
               K(ret), KP(query), KP(centroid), KP(query_code.data_));
    } else if (nullptr != cached_raw_norm && *cached_raw_norm < 0.0F) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (query_code.size_ < query_layout.size_) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("RaBitQ query code buffer is too small",
               K(ret), K(query_code.size_), K(query_layout.size_));
    } else if (!is_aligned(query_code.data_, alignof(float))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("RaBitQ query code is not float-aligned",
               K(ret), KP(query_code.data_));
    } else {
      float *query_work = reinterpret_cast<float *>(query_code.data_);
      const float query_raw_norm = nullptr != cached_raw_norm
          ? *cached_raw_norm : calc_vector_norm(params.dim_, query);
      float query_residual_norm = 0.0F;
      prepare_normalized_residual(params.dim_,
                                  query,
                                  centroid,
                                  query_work,
                                  query_residual_norm);
      *reinterpret_cast<float *>(
          query_code.data_ + query_layout.residual_norm_offset_) =
          query_residual_norm;
      *reinterpret_cast<float *>(
          query_code.data_ + query_layout.raw_norm_offset_) = query_raw_norm;
    }
    return ret;
  }

  static int compute(const ObQuantizationParams &params,
                     const ObQuantizationBuffer &query_code,
                     const ObQuantizationBuffer &base_code,
                     float &metric_value)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    BaseCodeLayout base_layout = {};
    if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, query_layout))) {
      LOG_WARN("failed to calculate RaBitQ query code layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, base_layout))) {
      LOG_WARN("failed to calculate RaBitQ base code layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_UNLIKELY(params.inv_sqrt_dim_ <= 0.0F)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid RaBitQ inverse square-root dimension",
               K(ret), K(params.inv_sqrt_dim_));
    } else if (OB_UNLIKELY(nullptr == query_code.data_
                           || nullptr == base_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null RaBitQ compute code pointer",
               K(ret), KP(query_code.data_), KP(base_code.data_));
    } else if (OB_UNLIKELY(query_code.size_ < query_layout.size_
                           || base_code.size_ < base_layout.size_)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("RaBitQ compute code buffer is too small",
               K(ret), K(query_code.size_), K(query_layout.size_),
               K(base_code.size_), K(base_layout.size_));
    } else if (OB_UNLIKELY(!is_aligned(query_code.data_, alignof(float)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("RaBitQ query code is not float-aligned",
               K(ret), KP(query_code.data_));
    } else if (OB_FAIL(compute_impl(params,
                                    query_code.data_,
                                    base_code.data_,
                                    query_layout,
                                    base_layout,
                                    metric_value))) {
      LOG_WARN("failed to compute RaBitQ quantized metric",
               K(ret), K(params.dim_), K(params.bits_));
    }
    return ret;
  }

  static int compute_bounds(const ObQuantizationParams &params,
                            const ObQuantizationBuffer &query_code,
                            const ObQuantizationBuffer &base_code,
                            float &metric_value,
                            float &lower_bound,
                            float &upper_bound)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    BaseCodeLayout base_layout = {};
    if (OB_FAIL(compute(params, query_code, base_code, metric_value))) {
      LOG_WARN("failed to compute RaBitQ metric before calculating bounds",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, query_layout))) {
      LOG_WARN("failed to calculate RaBitQ query code layout", K(ret), K(params.dim_));
    } else if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, base_layout))) {
      LOG_WARN("failed to calculate RaBitQ base code layout", K(ret), K(params.dim_));
    } else {
      compute_bounds_impl(params, query_code, base_code, query_layout, base_layout,
          metric_value, lower_bound, upper_bound);
    }
    return ret;
  }

  static int compute_batch(const ObQuantizationParams &params,
                           const ObQuantizationBuffer &query_code,
                           const ObQuantizationBuffer *codes, const int64_t count,
                           float *metric_values)
  {
    return compute_row_batch<false>(params, query_code, codes, count, metric_values);
  }

  static int compute_lower_bounds_batch(const ObQuantizationParams &params,
                                        const ObQuantizationBuffer &query_code,
                                        const ObQuantizationBuffer *codes, const int64_t count,
                                        float *lower_bounds)
  {
    return compute_row_batch<true>(params, query_code, codes, count, lower_bounds);
  }

private:
  template <bool LOWER_ONLY>
  static int compute_row_batch(const ObQuantizationParams &params,
                               const ObQuantizationBuffer &query_code,
                               const ObQuantizationBuffer *codes, const int64_t count,
                               float *results)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    BaseCodeLayout base_layout = {};
    if (count < 0 || (count > 0 && (nullptr == codes || nullptr == results))) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, query_layout))) {
    } else if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, base_layout))) {
    } else if (params.inv_sqrt_dim_ <= 0.0F || nullptr == query_code.data_
        || !is_aligned(query_code.data_, alignof(float))) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (query_code.size_ < query_layout.size_) {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    // Validate all borrowed payloads before writing any result. Layout and
    // query checks are shared by the batch, with one metric dispatch per batch.
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (nullptr == codes[i].data_) {
        ret = common::OB_INVALID_ARGUMENT;
      } else if (codes[i].size_ < base_layout.size_) {
        ret = common::OB_BUF_NOT_ENOUGH;
      }
    }
    if (OB_SUCC(ret) && count > 0) {
      const float query_residual_norm = *reinterpret_cast<const float *>(
          query_code.data_ + query_layout.residual_norm_offset_);
      const float query_raw_norm = *reinterpret_cast<const float *>(
          query_code.data_ + query_layout.raw_norm_offset_);
      const float dim_error_scale = LOWER_ONLY && params.dim_ > 1
          ? 2.0F / std::sqrt(params.dim_ - 1.0F) : 0.0F;
      const auto &simd_ops = get_rabitq_simd_ops();
      if (query_residual_norm < 0.0F || query_raw_norm < 0.0F) {
        ret = common::OB_ERR_UNEXPECTED;
      }
      static constexpr int64_t DOT_BATCH_SIZE = 32;
      float dots[DOT_BATCH_SIZE];
      const uint8_t *packed_codes[DOT_BATCH_SIZE];
      for (int64_t start = 0; OB_SUCC(ret) && start < count; start += DOT_BATCH_SIZE) {
        const int64_t batch_count = std::min(DOT_BATCH_SIZE, count - start);
        for (int64_t row = 0; row < batch_count; ++row) { packed_codes[row] = codes[start + row].data_; }
        if (nullptr != simd_ops.dot_packed_codes_batch) {
          simd_ops.dot_packed_codes_batch(reinterpret_cast<const float *>(query_code.data_),
              packed_codes, batch_count, params.dim_, params.inv_sqrt_dim_, dots);
        } else {
          for (int64_t row = 0; row < batch_count; ++row) {
            dots[row] = simd_ops.dot_packed_codes(reinterpret_cast<const float *>(query_code.data_),
                packed_codes[row], params.dim_, params.inv_sqrt_dim_);
          }
        }
        for (int64_t row = 0; OB_SUCC(ret) && row < batch_count; ++row) {
          const int64_t i = start + row;
          float base_residual_norm, correction, base_raw_norm;
          // Cache payloads may be unaligned. Decode once for both metric and bound.
          MEMCPY(&base_residual_norm, codes[i].data_ + base_layout.residual_norm_offset_, sizeof(float));
          MEMCPY(&correction, codes[i].data_ + base_layout.correction_offset_, sizeof(float));
          MEMCPY(&base_raw_norm, codes[i].data_ + base_layout.raw_norm_offset_, sizeof(float));
          if (base_residual_norm < 0.0F || base_raw_norm < 0.0F) {
            ret = common::OB_ERR_UNEXPECTED;
          } else {
            const float effective_correction = is_approx_zero(correction)
                ? (correction > 0.0F ? 1.0F : -1.0F) : correction;
            const float normalized_dot = dots[row] / effective_correction;
            const float metric = recover_metric(query_residual_norm, query_raw_norm,
                base_residual_norm, base_raw_norm, normalized_dot);
            if constexpr (LOWER_ONLY) {
              float unused_upper = 0.0F;
              recover_bounds<true>(params.dim_, dim_error_scale, query_residual_norm, query_raw_norm,
                  base_residual_norm, correction, base_raw_norm, metric, results[i], unused_upper);
            } else {
              results[i] = metric;
            }
          }
        }
      }
    }
    return ret;
  }

  static void compute_bounds_impl(const ObQuantizationParams &params,
                                  const ObQuantizationBuffer &query_code,
                                  const ObQuantizationBuffer &base_code,
                                  const QueryCodeLayout &query_layout,
                                  const BaseCodeLayout &base_layout,
                                  const float metric_value,
                                  float &lower_bound, float &upper_bound)
  {
    const float query_residual_norm = *reinterpret_cast<const float *>(
        query_code.data_ + query_layout.residual_norm_offset_);
    const float query_raw_norm = *reinterpret_cast<const float *>(
        query_code.data_ + query_layout.raw_norm_offset_);
    float base_residual_norm = 0.0F;
    float correction = 0.0F;
    float base_raw_norm = 0.0F;
    MEMCPY(&base_residual_norm,
           base_code.data_ + base_layout.residual_norm_offset_,
           sizeof(base_residual_norm));
    MEMCPY(&correction,
           base_code.data_ + base_layout.correction_offset_,
           sizeof(correction));
    MEMCPY(&base_raw_norm,
           base_code.data_ + base_layout.raw_norm_offset_,
           sizeof(base_raw_norm));

    const float dim_error_scale = params.dim_ > 1 ? 2.0F / std::sqrt(params.dim_ - 1.0F) : 0.0F;
    recover_bounds<false>(params.dim_, dim_error_scale, query_residual_norm, query_raw_norm,
        base_residual_norm, correction, base_raw_norm, metric_value, lower_bound, upper_bound);
  }

  template <bool LOWER_ONLY>
  static void recover_bounds(const int64_t dim, const float dim_error_scale,
                              const float query_residual_norm, const float query_raw_norm,
                              const float base_residual_norm, const float correction,
                              const float base_raw_norm, const float metric_value,
                              float &lower_bound, float &upper_bound)
  {
    float metric_error = 0.0;
    if ((share::VIDA_IP == METRIC || share::VIDA_COS == METRIC)
        && (is_approx_zero(query_raw_norm) || is_approx_zero(base_raw_norm))) {
      lower_bound = metric_value;
      if constexpr (!LOWER_ONLY) { upper_bound = metric_value; }
    } else if (dim <= 1 || is_approx_zero(correction)) {
      lower_bound = -FLT_MAX;
      if constexpr (!LOWER_ONLY) { upper_bound = FLT_MAX; }
    } else {
      // Let q_r = q - centroid and x_r = x - centroid, and let s be the
      // inner product of their normalized residuals.  RaBitQ estimates s
      // with the binary code of x_r.  If correction is the inner product
      // between the normalized x_r and its normalized binary code, the
      // RaBitQ error radius of the normalized-dot estimate is
      //
      //   eps_s = 2 / sqrt(dim - 1)
      //           * sqrt((1 - correction^2) / correction^2).
      //
      // Propagate eps_s through each metric formula.  For squared L2,
      //
      //   d = ||q_r||^2 + ||x_r||^2 - 2||q_r||||x_r||s,
      //   eps_l2 = 2||q_r||||x_r||eps_s.
      //
      // The inner-product distance used here is recovered from squared L2,
      // so its error radius is ||q_r||||x_r||eps_s.  Cosine distance further
      // divides that radius by ||q||||x||.  The resulting interval centered
      // at metric_value is used to discard vectors whose lower bound is
      // already worse than the current Top-K upper-bound threshold.
      const float correction_squared = correction * correction;
      const float normalized_dot_error =
          dim_error_scale * std::sqrt(std::max(0.0F, 1.0F - correction_squared) / correction_squared);
      metric_error = query_residual_norm * base_residual_norm * normalized_dot_error;
      if constexpr (share::VIDA_L2 == METRIC) {
        metric_error *= 2.0F;
      } else if constexpr (share::VIDA_COS == METRIC) {
        metric_error /= query_raw_norm * base_raw_norm;
      }
      lower_bound = metric_value - metric_error;
      if constexpr (!LOWER_ONLY) { upper_bound = metric_value + metric_error; }
    }
  }

  static inline bool is_approx_zero(const float value)
  {
    return std::abs(value) < 1e-5F;
  }

  static float calc_vector_norm(const int64_t dim,
                                const float *vector)
  {
    float squared_norm = 0.0F;
    for (int64_t i = 0; i < dim; ++i) {
      squared_norm += vector[i] * vector[i];
    }
    return std::sqrt(squared_norm);
  }

  static void normalize_vector(const int64_t dim,
                               float *vector,
                               float &norm)
  {
    norm = calc_vector_norm(dim, vector);
    if (norm > 0.0F) {
      for (int64_t i = 0; i < dim; ++i) {
        vector[i] /= norm;
      }
    }
  }

  static void prepare_normalized_residual(const int64_t dim,
                                          const float *vector,
                                          const float *centroid,
                                          float *normalized_residual,
                                          float &norm)
  {
    for (int64_t i = 0; i < dim; ++i) {
      normalized_residual[i] = vector[i] - centroid[i];
    }
    normalize_vector(dim, normalized_residual, norm);
  }

  static void store_base_metadata(uint8_t *base_code,
                                  const BaseCodeLayout &base_layout,
                                  const float residual_norm,
                                  const float correction,
                                  const float raw_norm)
  {
    *reinterpret_cast<float *>(
        base_code + base_layout.residual_norm_offset_) = residual_norm;
    *reinterpret_cast<float *>(
        base_code + base_layout.correction_offset_) = correction;
    *reinterpret_cast<float *>(
        base_code + base_layout.raw_norm_offset_) = raw_norm;
  }

  static void encode_base_code(const ObQuantizationParams &params,
                               const float *normalized_residual,
                               const float residual_norm,
                               const float raw_norm,
                               const BaseCodeLayout &base_layout,
                               uint8_t *base_code)
  {
    MEMSET(base_code, 0, base_layout.size_);
    float correction = 1.0F;
    if (0.0F != residual_norm) {
      get_rabitq_simd_ops().pack_sign_codes(
          normalized_residual,
          params.dim_,
          base_code);
      correction = get_rabitq_simd_ops().dot_packed_codes(
          normalized_residual,
          base_code,
          params.dim_,
          params.inv_sqrt_dim_);
    }
    store_base_metadata(base_code,
                        base_layout,
                        residual_norm,
                        correction,
                        raw_norm);
  }

  static float estimate_normalized_dot(const ObQuantizationParams &params,
                                       const float *normalized_residual,
                                       const uint8_t *packed_codes,
                                       const float correction)
  {
    const float packed_normalized_dot_estimate = get_rabitq_simd_ops().dot_packed_codes(
        normalized_residual,
        packed_codes,
        params.dim_,
        params.inv_sqrt_dim_);
    const float effective_correction = is_approx_zero(correction)
                                           ? (correction > 0.0F ? 1.0F : -1.0F)
                                           : correction;
    return packed_normalized_dot_estimate / effective_correction;
  }

  static float recover_metric(const float query_residual_norm,
                              const float query_raw_norm,
                              const float base_residual_norm,
                              const float base_raw_norm,
                              const float normalized_dot_estimate)
  {
    bool is_max_distance = false;
    if constexpr (share::VIDA_IP == METRIC || share::VIDA_COS == METRIC) {
      is_max_distance = is_approx_zero(query_raw_norm)
                        || is_approx_zero(base_raw_norm);
    }
    if (is_max_distance) {
      return 1.0F;
    }
    const float estimated_l2_squared =
        base_residual_norm * base_residual_norm
        + query_residual_norm * query_residual_norm
        - 2.0F * base_residual_norm * query_residual_norm
                   * normalized_dot_estimate;
    if constexpr (share::VIDA_IP == METRIC) {
      return 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm - estimated_l2_squared)
                        * 0.5F;
    } else if constexpr (share::VIDA_COS == METRIC) {
      return 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm - estimated_l2_squared)
                        * 0.5F / (query_raw_norm * base_raw_norm);
    } else {
      return estimated_l2_squared;
    }
  }

  static int compute_impl(const ObQuantizationParams &params,
                          const uint8_t *query_code,
                          const uint8_t *base_code,
                          const QueryCodeLayout &query_layout,
                          const BaseCodeLayout &base_layout,
                          float &metric_value)
  {
    int ret = OB_SUCCESS;
    const float query_residual_norm = *reinterpret_cast<const float *>(
        query_code + query_layout.residual_norm_offset_);
    const float query_raw_norm = *reinterpret_cast<const float *>(
        query_code + query_layout.raw_norm_offset_);
    float base_residual_norm = 0.0F;
    float correction = 0.0F;
    float base_raw_norm = 0.0F;
    // base_code 的长度和浮点元数据偏移均按 sizeof(float) 对齐，但 SSTable 的
    // ROW_STORE 会紧凑存储 VARCHAR payload，无法保证 base_code 起始地址对齐，
    // 因此必须通过 MEMCPY 读取浮点元数据，避免未对齐的类型化内存访问。
    MEMCPY(&base_residual_norm,
           base_code + base_layout.residual_norm_offset_,
           sizeof(base_residual_norm));
    MEMCPY(&correction,
           base_code + base_layout.correction_offset_,
           sizeof(correction));
    MEMCPY(&base_raw_norm,
           base_code + base_layout.raw_norm_offset_,
           sizeof(base_raw_norm));
    if (OB_UNLIKELY(query_residual_norm < 0.0F
                    || query_raw_norm < 0.0F
                    || base_residual_norm < 0.0F
                    || base_raw_norm < 0.0F)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid RaBitQ encoded metadata",
               K(ret), K(query_residual_norm), K(query_raw_norm),
               K(base_residual_norm), K(correction), K(base_raw_norm));
    } else {
      const float *normalized_residual = reinterpret_cast<const float *>(query_code);
      const float normalized_dot_estimate = estimate_normalized_dot(
          params,
          normalized_residual,
          base_code,
          correction);
      metric_value = recover_metric(query_residual_norm,
                                    query_raw_norm,
                                    base_residual_norm,
                                    base_raw_norm,
                                    normalized_dot_estimate);
    }
    return ret;
  }
};

template <share::ObVectorIndexDistAlgorithm METRIC>
const ObQuantizerMetricOps &ObRaBitQQuantizer::get_metric_ops()
{
  static const ObQuantizerMetricOps OPS = {
      calc_code_size,
      calc_query_code_size,
      calc_encode_scratch_size,
      MetricImpl<METRIC>::encode,
      MetricImpl<METRIC>::prepare_query,
      MetricImpl<METRIC>::compute,
      MetricImpl<METRIC>::compute_bounds,
      nullptr,
      MetricImpl<METRIC>::compute_batch,
      MetricImpl<METRIC>::compute_lower_bounds_batch,
      MetricImpl<METRIC>::prepare_query_with_norm};
  return OPS;
}

int ObRaBitQQuantizer::calc_code_size(const ObQuantizationParams &params,
                                     int64_t &size)
{
  int ret = OB_SUCCESS;
  BaseCodeLayout layout = {};
  if (1 != params.bits_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ vector code bits", K(ret), K(params.bits_));
  } else if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, layout))) {
    LOG_WARN("failed to calculate RaBitQ vector code size",
             K(ret), K(params.dim_), K(params.bits_));
  } else {
    size = layout.size_;
  }
  return ret;
}

int ObRaBitQQuantizer::calc_query_code_size(const ObQuantizationParams &params,
                                            int64_t &size)
{
  int ret = OB_SUCCESS;
  QueryCodeLayout layout = {};
  if (1 != params.bits_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ query code bits", K(ret), K(params.bits_));
  } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, layout))) {
    LOG_WARN("failed to calculate RaBitQ query code size",
             K(ret), K(params.dim_), K(params.bits_));
  } else {
    size = layout.size_;
  }
  return ret;
}

int ObRaBitQQuantizer::calc_encode_scratch_size(const ObQuantizationParams &params,
                                                int64_t &size)
{
  int ret = OB_SUCCESS;
  if (1 != params.bits_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RaBitQ encode scratch bits", K(ret), K(params.bits_));
  } else if (OB_FAIL(calculate_encode_scratch_size(params.dim_, size))) {
    LOG_WARN("failed to calculate RaBitQ encode scratch size", K(ret), K(params.dim_));
  }
  return ret;
}

const ObQuantizerOps &ObRaBitQQuantizer::get_ops()
{
  static const ObQuantizerOps OPS = {{
      &get_metric_ops<share::VIDA_L2>(),
      &get_metric_ops<share::VIDA_IP>(),
      &get_metric_ops<share::VIDA_COS>()}};
  return OPS;
}

} // namespace quantization
} // namespace oceanbase
