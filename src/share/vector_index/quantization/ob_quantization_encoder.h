/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_ENCODER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_ENCODER_H_

#include "share/vector_index/quantization/ob_quantizer_ops.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace quantization
{

// An encoder is owned by one build worker and is not thread-safe.
class ObQuantizationEncoder
{
public:
  ObQuantizationEncoder();
  ~ObQuantizationEncoder();

  // Base encoding does not consume rotator state or signs. RaBitQ vector and
  // centroid inputs must already be rotated with the query's rotator.
  int init(common::ObIAllocator &allocator,
           ObQuantizationType type,
           share::ObVectorIndexDistAlgorithm metric,
           const ObQuantizationParams &params);
  int encode(const float *vector,
             const float *centroid,
             const ObQuantizationBuffer &code);
  int64_t code_size() const { return code_size_; }
  void reset();

private:
  const ObQuantizerMetricOps *metric_ops_;
  ObQuantizationParams params_;
  uint8_t *encode_scratch_;
  int64_t encode_scratch_size_;
  int64_t code_size_;
  bool is_inited_;

  ObQuantizationEncoder(const ObQuantizationEncoder &) = delete;
  ObQuantizationEncoder &operator=(const ObQuantizationEncoder &) = delete;
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_ENCODER_H_
