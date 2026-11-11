/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_FP32_QUANTIZATION_OB_FP32_QUANTIZER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_FP32_QUANTIZATION_OB_FP32_QUANTIZER_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"

namespace oceanbase
{
namespace quantization
{

class ObFp32Quantizer
{
public:
  static int calc_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_query_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_encode_scratch_size(const ObQuantizationParams &params,
                                      int64_t &size);
  static const ObQuantizerOps &get_ops();

private:
  ObFp32Quantizer() = delete;

  template <share::ObVectorIndexDistAlgorithm METRIC>
  struct MetricImpl;

  template <share::ObVectorIndexDistAlgorithm METRIC>
  static const ObQuantizerMetricOps &get_metric_ops();
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_FP32_QUANTIZATION_OB_FP32_QUANTIZER_H_
