/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_SIMD_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_SIMD_H_

#include <stdint.h>

#include "common/ob_target_specific.h"

namespace oceanbase
{
namespace quantization
{

struct ObExtendRaBitQSimdOps
{
  // Cache per-dimension absolute values and return their maximum (initially 0).
  float (*absolute_and_max)(const float *residual, float *absolute, int64_t dim);

  typedef float (*DotExtraCodesFn)(const float *query,
                                   const uint8_t *packed_extra,
                                   int64_t dim);

  void (*pack_codes)(const float *normalized_residual,
                     int64_t dim,
                     int8_t extra_bits,
                     double best_t,
                     uint8_t *packed_msb,
                     uint8_t *packed_extra,
                     float &correction);
  float (*dot_msb_codes)(const float *query,
                         const uint8_t *packed_msb,
                         int64_t dim);
  DotExtraCodesFn dot_extra_codes[8];
};

const ObExtendRaBitQSimdOps &get_extend_rabitq_simd_ops();
const ObExtendRaBitQSimdOps *get_extend_rabitq_simd_ops(
    common::ObTargetArch arch);

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_EXTEND_RABITQ_QUANTIZATION_OB_EXTEND_RABITQ_SIMD_H_
