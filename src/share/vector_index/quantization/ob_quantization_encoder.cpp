/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/ob_quantization_encoder.h"

#include <stdint.h>

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/ob_quantizer_registry.h"

namespace oceanbase
{
namespace quantization
{

ObQuantizationEncoder::ObQuantizationEncoder()
{
  reset();
}

ObQuantizationEncoder::~ObQuantizationEncoder()
{
  reset();
}

int ObQuantizationEncoder::init(common::ObIAllocator &allocator,
                                const ObQuantizationType type,
                                const share::ObVectorIndexDistAlgorithm metric,
                                const ObQuantizationParams &params)
{
  int ret = OB_SUCCESS;
  const ObQuantizerMetricOps *metric_ops = nullptr;
  int64_t code_size = 0;
  int64_t encode_scratch_size = 0;
  uint8_t *encode_scratch = nullptr;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("quantization encoder is already initialized",
             K(ret),
             K(type),
             K(metric),
             K(params.dim_),
             K(params.bits_));
  } else if (OB_FAIL(ObQuantizerRegistry::get_ops(type, metric, metric_ops))) {
    LOG_WARN("failed to get quantizer metric operations", K(ret), K(type), K(metric));
  } else if (OB_FAIL(metric_ops->calc_code_size(params, code_size))) {
    LOG_WARN("failed to calculate quantization code size",
             K(ret),
             K(type),
             K(metric),
             K(params.dim_),
             K(params.bits_));
  } else if (OB_FAIL(metric_ops->calc_encode_scratch_size(params,
                                                          encode_scratch_size))) {
    LOG_WARN("failed to calculate quantization encode scratch size",
             K(ret),
             K(type),
             K(metric),
             K(params.dim_),
             K(params.bits_));
  } else if (code_size < 0 || encode_scratch_size < 0) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("quantization encoder returned a negative buffer size",
             K(ret),
             K(type),
             K(metric),
             K(params.dim_),
             K(params.bits_),
             K(code_size),
             K(encode_scratch_size));
  } else if (encode_scratch_size > 0
             && OB_ISNULL(encode_scratch = static_cast<uint8_t *>(
                                allocator.alloc_align(
                                    encode_scratch_size,
                                    alignof(double))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate quantization encode scratch",
             K(ret),
             K(type),
             K(metric),
             K(params.dim_),
             K(params.bits_),
             K(encode_scratch_size));
  } else {
    metric_ops_ = metric_ops;
    params_ = params;
    params_.inv_sqrt_dim_ = calc_inv_sqrt_dim(params.dim_);
    encode_scratch_ = encode_scratch;
    encode_scratch_size_ = encode_scratch_size;
    code_size_ = code_size;
    is_inited_ = true;
  }
  return ret;
}

int ObQuantizationEncoder::encode(const float *vector,
                                  const float *centroid,
                                  const ObQuantizationBuffer &code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("quantization encoder is not initialized", K(ret));
  } else if (OB_FAIL(metric_ops_->encode(params_,
                                         vector,
                                         centroid,
                                         {encode_scratch_, encode_scratch_size_},
                                         code))) {
    LOG_WARN("failed to encode quantization code", K(ret));
  }
  return ret;
}

void ObQuantizationEncoder::reset()
{
  metric_ops_ = nullptr;
  params_ = {};
  encode_scratch_ = nullptr;
  encode_scratch_size_ = 0;
  code_size_ = 0;
  is_inited_ = false;
}

} // namespace quantization
} // namespace oceanbase
