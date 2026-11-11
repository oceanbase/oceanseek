/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_SIMD_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_SIMD_H_

#include <stdint.h>

#include "common/ob_target_specific.h"
#include "share/vector_index/ob_vector_index_common_define.h"

namespace oceanbase
{
namespace quantization
{

class ObRaBitQFastScanComputer;

struct ObRaBitQSimdOps
{
  void (*pack_sign_codes)(const float *normalized_residual,
                          int64_t dim,
                          uint8_t *packed_codes);
  float (*dot_packed_codes)(const float *query,
                            const uint8_t *packed_codes,
                            int64_t dim,
                            float inv_sqrt_dim);
  // Optional row-major kernel. Borrow codes without transposing them; each
  // row keeps the same accumulation order as dot_packed_codes.
  void (*dot_packed_codes_batch)(const float *query,
                                 const uint8_t *const *packed_codes,
                                 int64_t count, int64_t dim,
                                 float inv_sqrt_dim, float *results);
};

struct ObFhtKacSimdOps
{
  void (*apply_signs)(int64_t dim, const uint8_t *signs, float *values);
  void (*fht)(int64_t length, float *values);
  void (*scale)(int64_t length, float factor, float *values);
  void (*kac)(int64_t length, float *values);
};

struct ObRaBitQFastScanSimdOps
{
  void (*build_luts)(const float *normalized_residual,
                     int64_t dim,
                     float positive,
                     float negative,
                     float *luts,
                     float &lut_min,
                     float &lut_max);
  void (*quantize_luts)(const float *float_luts,
                        int64_t lut_entry_count,
                        float lut_min,
                        float lut_scale,
                        uint8_t *low_byte_luts,
                        uint8_t *high_byte_luts);
  void (*accumulate_lut_pairs)(const uint8_t *packed_codes,
                               const uint8_t *low_byte_luts,
                               const uint8_t *high_byte_luts,
                               int64_t code_bytes,
                               uint32_t *low_byte_sums,
                               uint32_t *high_byte_sums);
  int64_t (*recover_metrics)(const uint32_t *low_byte_sums,
                             const uint32_t *high_byte_sums,
                             const float *residual_norms,
                             const float *corrections,
                             const float *raw_norms,
                             int64_t vector_count,
                             const ObRaBitQFastScanComputer &computer,
                             float *metric_values);
  void (*compute_lower_bounds)(const float *metric_values,
                               const float *residual_norms,
                               const float *corrections,
                               const float *raw_norms,
                               int64_t vector_count,
                               const ObRaBitQFastScanComputer &computer,
                               float *lower_bounds);
};

const ObRaBitQSimdOps &get_rabitq_simd_ops();
const ObRaBitQSimdOps *get_rabitq_simd_ops(common::ObTargetArch arch);
const ObFhtKacSimdOps &get_fht_kac_simd_ops();
const ObFhtKacSimdOps *get_fht_kac_simd_ops(common::ObTargetArch arch);
const ObRaBitQFastScanSimdOps &get_rabitq_fast_scan_simd_ops();
const ObRaBitQFastScanSimdOps *get_rabitq_fast_scan_simd_ops(
    common::ObTargetArch arch);

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_RABITQ_SIMD_H_
