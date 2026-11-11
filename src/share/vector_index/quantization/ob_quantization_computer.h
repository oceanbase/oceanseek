/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_COMPUTER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_COMPUTER_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace quantization
{

class ObQuantizationComputer
{
public:
  ObQuantizationComputer();
  ~ObQuantizationComputer();

  // FP32 reads query directly: keep it valid and unchanged until reset/destruction.
  // Other types own their prepared query code allocated through allocator.
  int init(common::ObIAllocator &allocator,
           ObQuantizationType type,
           share::ObVectorIndexDistAlgorithm metric,
           const ObQuantizationParams &params,
           const float *query,
           const float *centroid,
           const float *query_raw_norm = nullptr);
  // Same dimension/metric, new centroid or query; reuses the owned code buffer.
  // FP32 is not supported because its query buffer is borrowed.
  // A supplied norm must belong to this rotated query. Failure resets the computer.
  int reprepare_query(const float *query, const float *centroid,
                      const float *query_raw_norm = nullptr);
  bool is_inited() const { return is_inited_; }
  int compute(const ObQuantizationBuffer &code, float &metric_value) const;
  int compute_bounds(const ObQuantizationBuffer &code,
                     float &metric_value,
                     float &lower_bound,
                     float &upper_bound) const;
  // Borrowed row-format codes; no packing or copying is required. Empty batches
  // are valid. On failure callers must discard the entire output batch.
  int compute_batch(const ObQuantizationBuffer *codes, int64_t count,
                    float *metric_values) const;
  int compute_lower_bounds_batch(const ObQuantizationBuffer *codes, int64_t count,
                                 float *lower_bounds) const;
  int compute_xbits(const ObQuantizationBuffer &code,
                    float ip_msb,
                    float &metric_value) const;
  void reset();

private:
  common::ObIAllocator *allocator_; // Null when FP32 reads the caller-owned query directly.
  const ObQuantizerMetricOps *metric_ops_;
  ObQuantizationParams params_;
  uint8_t *query_code_;
  int64_t query_code_size_;
  bool is_inited_;

  ObQuantizationComputer(const ObQuantizationComputer &) = delete;
  ObQuantizationComputer &operator=(const ObQuantizationComputer &) = delete;
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_COMPUTER_H_
