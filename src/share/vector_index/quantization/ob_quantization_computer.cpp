/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/ob_quantization_computer.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/ob_quantizer_registry.h"

namespace oceanbase
{
namespace quantization
{

ObQuantizationComputer::ObQuantizationComputer()
  : allocator_(nullptr), metric_ops_(nullptr), params_(), query_code_(nullptr),
    query_code_size_(0), is_inited_(false)
{}

ObQuantizationComputer::~ObQuantizationComputer()
{
  reset();
}

int ObQuantizationComputer::init(common::ObIAllocator &allocator,
                                 const ObQuantizationType type,
                                 const share::ObVectorIndexDistAlgorithm metric,
                                 const ObQuantizationParams &params,
                                 const float *query,
                                 const float *centroid,
                                 const float *query_raw_norm)
{
  int ret = OB_SUCCESS;
  const bool is_fp32 = ObQuantizationType::FP32 == type;
  const ObQuantizerMetricOps *metric_ops = nullptr;
  int64_t query_code_size = 0;
  uint8_t *query_code = nullptr;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("quantization computer is already initialized",
             K(ret), K(type), K(metric), K(params.dim_), K(params.bits_));
  } else if (is_fp32 && (query == nullptr || params.dim_ <= 0
      || params.dim_ > INT64_MAX / static_cast<int64_t>(sizeof(float)))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FP32 query argument", K(ret), K(type), K(metric),
        K(params.dim_), K(params.bits_), KP(query));
  } else if (OB_FAIL(ObQuantizerRegistry::get_ops(type, metric, metric_ops))) {
    LOG_WARN("failed to get quantizer metric operations", K(ret), K(type), K(metric));
  } else if (OB_FAIL(metric_ops->calc_query_code_size(params, query_code_size))) {
    LOG_WARN("failed to calculate quantization query code size",
             K(ret), K(type), K(metric), K(params.dim_), K(params.bits_));
  } else if (query_code_size < 0) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("quantizer returned a negative query code size",
             K(ret), K(type), K(metric), K(query_code_size));
  } else if (is_fp32) {
    // FP32 reads the caller-owned vector directly; no query encoding is needed.
    query_code = reinterpret_cast<uint8_t *>(const_cast<float *>(query));
  } else if (query_code_size > 0
             && nullptr == (query_code = static_cast<uint8_t *>(
                                allocator.alloc_align(
                                    query_code_size,
                                    alignof(float))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate quantization query code",
             K(ret), K(type), K(metric), K(query_code_size));
  } else {
    if (nullptr != query_raw_norm && nullptr != metric_ops->prepare_query_with_norm) {
      ret = metric_ops->prepare_query_with_norm(params, query, centroid, *query_raw_norm,
          {query_code, query_code_size});
    } else {
      ret = metric_ops->prepare_query(params, query, centroid, {query_code, query_code_size});
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to prepare quantization query",
               K(ret), K(type), K(metric), K(params.dim_), K(query_code_size));
    }
  }
  if (OB_SUCC(ret)) {
    allocator_ = is_fp32 ? nullptr : &allocator;
    metric_ops_ = metric_ops;
    params_ = params;
    params_.inv_sqrt_dim_ = calc_inv_sqrt_dim(params.dim_);
    query_code_ = query_code;
    query_code_size_ = query_code_size;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && !is_fp32) {
    allocator.free_align(query_code);
    query_code = nullptr;
  }
  return ret;
}

int ObQuantizationComputer::reprepare_query(const float *query, const float *centroid,
                                           const float *query_raw_norm)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
  } else if (nullptr == allocator_) {
    // FP32 borrows the caller's query; only owned buffers may be rewritten.
    ret = common::OB_NOT_SUPPORTED;
  } else if (nullptr != query_raw_norm && nullptr != metric_ops_->prepare_query_with_norm) {
    ret = metric_ops_->prepare_query_with_norm(params_, query, centroid, *query_raw_norm,
        {query_code_, query_code_size_});
  } else {
    ret = metric_ops_->prepare_query(params_, query, centroid, {query_code_, query_code_size_});
  }
  if (OB_FAIL(ret)) { reset(); }
  return ret;
}

int ObQuantizationComputer::compute(const ObQuantizationBuffer &code,
                                    float &metric_value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("quantization computer is not initialized", K(ret));
  } else if (OB_FAIL(metric_ops_->compute(params_,
                                           {query_code_, query_code_size_},
                                           code,
                                           metric_value))) {
    LOG_WARN("failed to compute quantized metric",
             K(ret), K(params_.dim_), K(code.size_), K(query_code_size_));
  }
  return ret;
}

int ObQuantizationComputer::compute_bounds(const ObQuantizationBuffer &code,
                                           float &metric_value,
                                           float &lower_bound,
                                           float &upper_bound) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("quantization computer is not initialized", K(ret));
  } else if (OB_ISNULL(metric_ops_->compute_bounds)) {
    ret = common::OB_NOT_SUPPORTED;
    LOG_WARN("quantizer does not support distance bounds", K(ret), K(params_.dim_));
  } else if (OB_FAIL(metric_ops_->compute_bounds(params_,
                                                  {query_code_, query_code_size_},
                                                  code,
                                                  metric_value,
                                                  lower_bound,
                                                  upper_bound))) {
    LOG_WARN("failed to compute quantized metric bounds",
             K(ret), K(params_.dim_), K(code.size_), K(query_code_size_));
  }
  return ret;
}

int ObQuantizationComputer::compute_batch(const ObQuantizationBuffer *codes,
                                          const int64_t count,
                                          float *metric_values) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(count < 0)
      || (count > 0 && (OB_ISNULL(codes) || OB_ISNULL(metric_values)))) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (count == 0) {
  } else if (nullptr != metric_ops_->compute_batch) {
    ret = metric_ops_->compute_batch(params_, {query_code_, query_code_size_},
        codes, count, metric_values);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = metric_ops_->compute(params_, {query_code_, query_code_size_}, codes[i], metric_values[i]);
    }
  }
  return ret;
}

int ObQuantizationComputer::compute_lower_bounds_batch(const ObQuantizationBuffer *codes,
                                                       const int64_t count,
                                                       float *lower_bounds) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
  } else if (count < 0 || (count > 0 && (nullptr == codes || nullptr == lower_bounds))) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (count == 0) {
  } else if (nullptr != metric_ops_->compute_lower_bounds_batch) {
    ret = metric_ops_->compute_lower_bounds_batch(params_, {query_code_, query_code_size_},
        codes, count, lower_bounds);
  } else if (nullptr == metric_ops_->compute_bounds) {
    ret = common::OB_NOT_SUPPORTED;
  } else {
    float metric = 0.0F, upper = 0.0F;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = metric_ops_->compute_bounds(params_, {query_code_, query_code_size_},
          codes[i], metric, lower_bounds[i], upper);
    }
  }
  return ret;
}

int ObQuantizationComputer::compute_xbits(const ObQuantizationBuffer &code,
                                          const float ip_msb,
                                          float &metric_value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("quantization computer is not initialized", K(ret));
  } else if (OB_ISNULL(metric_ops_->compute_xbits)) {
    ret = common::OB_NOT_SUPPORTED;
    LOG_WARN("quantizer does not support XBits compute",
             K(ret), K(params_.dim_), K(params_.bits_));
  } else if (OB_FAIL(metric_ops_->compute_xbits(params_,
                                                 {query_code_, query_code_size_},
                                                 code,
                                                 ip_msb,
                                                 metric_value))) {
    LOG_WARN("failed to compute XBits quantized metric",
             K(ret), K(params_.dim_), K(params_.bits_), K(code.size_),
             K(query_code_size_), K(ip_msb));
  }
  return ret;
}

void ObQuantizationComputer::reset()
{
  if (nullptr != allocator_) {
    allocator_->free_align(query_code_);
  }
  allocator_ = nullptr;
  metric_ops_ = nullptr;
  params_ = {};
  query_code_ = nullptr;
  query_code_size_ = 0;
  is_inited_ = false;
}

} // namespace quantization
} // namespace oceanbase
