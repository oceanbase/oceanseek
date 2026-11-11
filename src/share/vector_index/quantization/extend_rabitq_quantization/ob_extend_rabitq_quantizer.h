/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_QUANTIZER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_QUANTIZER_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"


/** Extended RaBitQ quantization.
 *
 *  Reference:
 *  [1] Jianyang Gao, Yutong Gou, Yuexuan Xu, Yongyi Yang, Cheng Long, and Raymond Chi-Wing
 *  Wong. 2025. Practical and Asymptotically Optimal Quantization of High-Dimensional Vectors
 *  in Euclidean Space for Approximate Nearest Neighbor Search. Proc. ACM Manag. Data 3, 3,
 *  Article 202 (June 2025), 26 pages.
 */

namespace oceanbase
{
namespace quantization
{

class ObExtendRaBitQQuantizer
{
public:
  static int calc_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_query_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_encode_scratch_size(const ObQuantizationParams &params, int64_t &size);
  static const ObQuantizerOps &get_ops();

private:
  ObExtendRaBitQQuantizer() = delete;

  struct BaseCodeLayout;
  struct QueryCodeLayout;
  struct EncodeScratchLayout;

  template <share::ObVectorIndexDistAlgorithm METRIC>
  struct MetricImpl;

  template <share::ObVectorIndexDistAlgorithm METRIC>
  static const ObQuantizerMetricOps &get_metric_ops();
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_QUANTIZER_H_
