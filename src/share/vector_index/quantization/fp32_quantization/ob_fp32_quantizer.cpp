/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/fp32_quantization/ob_fp32_quantizer.h"

#include <string.h>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_l2_distance.h"

namespace oceanbase
{
namespace quantization
{

namespace
{

int calculate_query_code_size(const int64_t dim, int64_t &size)
{
  int ret = OB_SUCCESS;
  if (dim <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FP32 query code dimension", K(ret), K(dim));
  } else {
    size = dim * static_cast<int64_t>(sizeof(float));
  }
  return ret;
}

} // namespace

int ObFp32Quantizer::calc_code_size(const ObQuantizationParams &params,
                                    int64_t &)
{
  const int ret = common::OB_NOT_SUPPORTED;
  LOG_WARN("FP32 code sizing is not supported", K(ret), K(params.dim_));
  return ret;
}

int ObFp32Quantizer::calc_query_code_size(const ObQuantizationParams &params,
                                          int64_t &size)
{
  return calculate_query_code_size(params.dim_, size);
}

int ObFp32Quantizer::calc_encode_scratch_size(const ObQuantizationParams &params,
                                              int64_t &)
{
  const int ret = common::OB_NOT_SUPPORTED;
  LOG_WARN("FP32 encode scratch sizing is not supported", K(ret), K(params.dim_));
  return ret;
}

template <share::ObVectorIndexDistAlgorithm METRIC>
struct ObFp32Quantizer::MetricImpl
{
  static int encode(const ObQuantizationParams &params,
                    const float *,
                    const float *,
                    const ObQuantizationBuffer &,
                    const ObQuantizationBuffer &)
  {
    const int ret = common::OB_NOT_SUPPORTED;
    LOG_WARN("FP32 encoding is not supported", K(ret), K(params.dim_));
    return ret;
  }

  static int prepare_query(const ObQuantizationParams &params,
                           const float *query,
                           const float *,
                           const ObQuantizationBuffer &query_code)
  {
    int ret = OB_SUCCESS;
    int64_t required_size = 0;
    if (OB_FAIL(calculate_query_code_size(params.dim_, required_size))) {
      LOG_WARN("failed to calculate FP32 query code size", K(ret), K(params.dim_));
    } else if (nullptr == query || nullptr == query_code.data_) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null FP32 query preparation argument", K(ret), KP(query), KP(query_code.data_));
    } else if (query_code.size_ < required_size) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("FP32 query code buffer is too short",
               K(ret), K(query_code.size_), K(required_size));
    } else {
      MEMMOVE(query_code.data_, query, required_size);
    }
    return ret;
  }

  static int compute_distance(const float *query, const float *base, int64_t dim,
                              bool is_norm, double &value);

  static int compute(const ObQuantizationParams &params,
                     const ObQuantizationBuffer &query_code,
                     const ObQuantizationBuffer &base_code,
                     float &metric_value)
  {
    int ret = OB_SUCCESS;
    double value = 0.0;
    int64_t required_size = 0;
    if (OB_FAIL(calculate_query_code_size(params.dim_, required_size))) {
      LOG_WARN("failed to calculate FP32 distance buffer size", K(ret), K(params.dim_));
    } else if (OB_UNLIKELY(nullptr == query_code.data_ || nullptr == base_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null FP32 distance code", K(ret), KP(query_code.data_), KP(base_code.data_));
    } else if (OB_UNLIKELY(query_code.size_ < required_size || base_code.size_ < required_size)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("FP32 distance buffer is too short",
               K(ret), K(params.dim_), K(required_size),
               K(query_code.size_), K(base_code.size_));
    } else if (OB_FAIL(compute_distance(reinterpret_cast<const float *>(query_code.data_),
                                        reinterpret_cast<const float *>(base_code.data_),
                                        params.dim_, params.is_norm_,
                                        value))) {
      LOG_WARN("failed to compute FP32 quantized distance", K(ret), K(params.dim_));
    } else {
      metric_value = static_cast<float>(value);
    }
    return ret;
  }
};

template <>
int ObFp32Quantizer::MetricImpl<share::VIDA_L2>::compute_distance(
    const float *query, const float *base, const int64_t dim,
    const bool /* is_norm */, double &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::ObVectorL2Distance<float>::l2_square_func(
          query, base, dim, value))) {
    LOG_WARN("failed to calculate FP32 L2 distance", K(ret), K(dim), KP(query), KP(base));
  }
  return ret;
}

template <>
int ObFp32Quantizer::MetricImpl<share::VIDA_IP>::compute_distance(
    const float *query, const float *base, const int64_t dim,
    const bool /* is_norm */, double &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common::ObVectorIpDistance<float>::ip_distance_func(
          query, base, dim, value))) {
    LOG_WARN("failed to calculate FP32 inner product", K(ret), K(dim), KP(query), KP(base));
  } else {
    value = 1.0 - value;
  }
  return ret;
}

template <>
int ObFp32Quantizer::MetricImpl<share::VIDA_COS>::compute_distance(
    const float *query, const float *base, const int64_t dim,
    const bool is_norm, double &value)
{
  int ret = OB_SUCCESS;
  if (is_norm) {
    if (OB_FAIL(MetricImpl<share::VIDA_IP>::compute_distance(query, base, dim, is_norm, value))) {
      LOG_WARN("failed to calculate FP32 ip distance", K(ret), K(dim), KP(query), KP(base));
    }
  } else if (OB_FAIL(common::ObVectorCosineDistance<float>::cosine_distance_func(
          query, base, dim, value))) {
    LOG_WARN("failed to calculate FP32 cosine distance",
             K(ret), K(dim), KP(query), KP(base));
  }
  return ret;
}

template <share::ObVectorIndexDistAlgorithm METRIC>
const ObQuantizerMetricOps &ObFp32Quantizer::get_metric_ops()
{
  static const ObQuantizerMetricOps OPS = {
      calc_code_size,
      calc_query_code_size,
      calc_encode_scratch_size,
      MetricImpl<METRIC>::encode,
      MetricImpl<METRIC>::prepare_query,
      MetricImpl<METRIC>::compute,
      nullptr,
      nullptr};
  return OPS;
}

const ObQuantizerOps &ObFp32Quantizer::get_ops()
{
  static const ObQuantizerOps OPS = {{
      &get_metric_ops<share::VIDA_L2>(),
      &get_metric_ops<share::VIDA_IP>(),
      &get_metric_ops<share::VIDA_COS>()}};
  return OPS;
}

} // namespace quantization
} // namespace oceanbase
