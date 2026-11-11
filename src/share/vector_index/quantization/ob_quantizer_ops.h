/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_OPS_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_OPS_H_

#include "share/vector_index/quantization/ob_quantization_struct.h"

namespace oceanbase
{
namespace quantization
{

struct ObQuantizerMetricOps
{
  int (*calc_code_size)(const ObQuantizationParams &params, int64_t &size);
  int (*calc_query_code_size)(const ObQuantizationParams &params, int64_t &size);
  int (*calc_encode_scratch_size)(const ObQuantizationParams &params, int64_t &size);
  int (*encode)(const ObQuantizationParams &params,
                const float *vector,
                const float *centroid,
                const ObQuantizationBuffer &encode_scratch,
                const ObQuantizationBuffer &code);
  int (*prepare_query)(const ObQuantizationParams &params,
                       const float *query,
                       const float *centroid,
                       const ObQuantizationBuffer &query_code);
  int (*compute)(const ObQuantizationParams &params,
                 const ObQuantizationBuffer &query_code,
                 const ObQuantizationBuffer &code,
                 float &metric_value);
  int (*compute_bounds)(const ObQuantizationParams &params,
                        const ObQuantizationBuffer &query_code,
                        const ObQuantizationBuffer &code,
                        float &metric_value,
                        float &lower_bound,
                        float &upper_bound);
  int (*compute_xbits)(const ObQuantizationParams &params,
                       const ObQuantizationBuffer &query_code,
                       const ObQuantizationBuffer &code,
                       float ip_msb,
                       float &metric_value);
  // Optional row-format batch kernels. Other quantizers use the scalar fallback.
  int (*compute_batch)(const ObQuantizationParams &params,
                       const ObQuantizationBuffer &query_code,
                       const ObQuantizationBuffer *codes, int64_t count,
                       float *metric_values) = nullptr;
  int (*compute_lower_bounds_batch)(const ObQuantizationParams &params,
                                    const ObQuantizationBuffer &query_code,
                                    const ObQuantizationBuffer *codes, int64_t count,
                                    float *lower_bounds) = nullptr;
  // Optional query preparation using the caller's norm of the same rotated query.
  int (*prepare_query_with_norm)(const ObQuantizationParams &params,
                                 const float *query, const float *centroid,
                                 float query_raw_norm,
                                 const ObQuantizationBuffer &query_code) = nullptr;
};

struct ObQuantizerOps
{
  const ObQuantizerMetricOps
      *metric_ops[static_cast<int64_t>(share::VIDA_MAX)];
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZER_OPS_H_
