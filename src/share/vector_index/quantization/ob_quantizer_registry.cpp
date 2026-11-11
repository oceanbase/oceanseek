/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/ob_quantizer_registry.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_quantizer.h"
#include "share/vector_index/quantization/fp32_quantization/ob_fp32_quantizer.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"

namespace oceanbase
{
namespace quantization
{
namespace
{

const ObQuantizerOps *const QUANTIZER_OPS[
    static_cast<int64_t>(ObQuantizationType::MAX_TYPE)] = {
        &ObFp32Quantizer::get_ops(),
        nullptr,
        &ObRaBitQQuantizer::get_ops(),
        &ObExtendRaBitQQuantizer::get_ops()};

} // namespace

int ObQuantizerRegistry::get_ops(const ObQuantizationType type,
                                 const share::ObVectorIndexDistAlgorithm metric,
                                 const ObQuantizerMetricOps *&metric_ops)
{
  int ret = OB_SUCCESS;
  metric_ops = nullptr;
  const int64_t type_index = static_cast<int64_t>(type);
  const int64_t metric_index = static_cast<int64_t>(metric);
  if (type_index < 0 || type_index >= static_cast<int64_t>(ObQuantizationType::MAX_TYPE)
      || metric_index < 0 || metric_index >= static_cast<int64_t>(share::VIDA_MAX)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quantizer type or metric", K(ret), K(type_index), K(metric_index));
  } else if (nullptr == QUANTIZER_OPS[type_index]
             || nullptr == QUANTIZER_OPS[type_index]->metric_ops[metric_index]) {
    ret = common::OB_NOT_SUPPORTED;
    LOG_WARN("quantizer type or metric is not supported",
             K(ret),
             K(type_index),
             K(metric_index));
  } else {
    metric_ops = QUANTIZER_OPS[type_index]->metric_ops[metric_index];
  }
  return ret;
}

} // namespace quantization
} // namespace oceanbase
