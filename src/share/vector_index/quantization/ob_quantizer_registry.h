/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_REGISTRY_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_REGISTRY_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"

namespace oceanbase
{
namespace quantization
{

class ObQuantizerRegistry
{
public:
  static int get_ops(ObQuantizationType type,
                     share::ObVectorIndexDistAlgorithm metric,
                     const ObQuantizerMetricOps *&metric_ops);
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_REGISTRY_H_
