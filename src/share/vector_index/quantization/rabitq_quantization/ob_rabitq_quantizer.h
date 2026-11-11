/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_QUANTIZER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_QUANTIZER_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"


/** One-bit RaBitQ quantization.
 *
 *  Reference:
 *  [1] Jianyang Gao and Cheng Long. 2024. RaBitQ: Quantizing High-Dimensional Vectors with a
 *  Theoretical Error Bound for Approximate Nearest Neighbor Search. Proc. ACM Manag. Data 2,
 *  3, Article 167 (June 2024), 27 pages.
 */

namespace oceanbase
{
namespace quantization
{

class ObRaBitQQuantizer
{
public:
  static int calc_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_query_code_size(const ObQuantizationParams &params, int64_t &size);
  static int calc_encode_scratch_size(const ObQuantizationParams &params, int64_t &size);
  static const ObQuantizerOps &get_ops();

private:
  ObRaBitQQuantizer() = delete;

  struct BaseCodeLayout;
  struct QueryCodeLayout;

  static bool is_aligned(const void *pointer, int64_t alignment);
  static int64_t align_up(int64_t value, int64_t alignment);
  static int calculate_encode_scratch_size(int64_t dim, int64_t &size);

  template <share::ObVectorIndexDistAlgorithm METRIC>
  struct MetricImpl;

  template <share::ObVectorIndexDistAlgorithm METRIC>
  static const ObQuantizerMetricOps &get_metric_ops();
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_QUANTIZER_H_
