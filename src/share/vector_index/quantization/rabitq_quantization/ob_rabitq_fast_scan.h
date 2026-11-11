/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_RABITQ_FAST_SCAN_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_RABITQ_FAST_SCAN_H_

#include <stdint.h>

#include "share/vector_index/quantization/ob_quantization_struct.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace quantization
{

// Constants shared by the FastScan encoder, computer, and ISA-specific kernels.
// Keep the binary-layout contract here so a format change cannot update only
// one side.
struct ObRaBitQFastScanConstants
{
  static constexpr int64_t BATCH_SIZE = 32;
  static constexpr int64_t CODES_PER_PACKED_BYTE = 2;
  static constexpr int64_t NIBBLE_BITS = 4;
  static constexpr int64_t NIBBLE_LUT_SIZE = 1L << NIBBLE_BITS;
  static constexpr uint8_t NIBBLE_MASK =
      static_cast<uint8_t>(NIBBLE_LUT_SIZE - 1);
  static constexpr uint8_t HIGH_NIBBLE_MASK =
      static_cast<uint8_t>(NIBBLE_MASK << NIBBLE_BITS);
  static constexpr int64_t LUT_VALUES_PER_CODE_BYTE = 2 * NIBBLE_LUT_SIZE;
  static constexpr int64_t QUANTIZED_LUT_BYTES = 2;
  static constexpr int64_t QUANTIZED_LUT_SIZE_PER_CODE_BYTE =
      QUANTIZED_LUT_BYTES * LUT_VALUES_PER_CODE_BYTE;
  static constexpr float ZERO_EPSILON = 1e-5F;
};

struct ObRaBitQFastScanSimdOps;

// Convert row-major one-bit RaBitQ codes into the 32-vector blocked layout
// consumed by FastScan SIMD lookup-table kernels. Each output block stores
// ceil(dim / 8) consecutive 32-byte columns. Missing vectors in the last block
// and unused bits in the last source byte are encoded as zero.
// In each column, byte j and byte j+16 contain the first/second four dimensions
// of rows j and j+16: the low nibble belongs to row j, the high nibble to row j+16.
// Slots j=0..15 follow natural row order, with no row permutation.
class ObRaBitQFastScanEncoder
{
public:
  static const int64_t BATCH_SIZE = ObRaBitQFastScanConstants::BATCH_SIZE;

  static int calc_packed_size(int64_t dim,
                              int64_t code_count,
                              int64_t &packed_size);

  // The input is a tightly packed [code_count][ceil(dim / 8)] byte matrix.
  static int pack_codes(const uint8_t *codes,
                        int64_t code_count,
                        int64_t dim,
                        const ObQuantizationBuffer &packed_codes);

  // code_stride allows each input code to carry metadata after its sign bits.
  // Only the first ceil(dim / 8) bytes of each row are packed.
  static int pack_codes(const uint8_t *codes,
                        int64_t code_stride,
                        int64_t code_count,
                        int64_t dim,
                        const ObQuantizationBuffer &packed_codes);

private:
  ObRaBitQFastScanEncoder() = delete;
};

// Compute one FastScan batch directly from ObRaBitQFastScanEncoder's packed
// layout. The query and centroid must already be rotated, matching the regular
// ObQuantizationComputer contract for RaBitQ.
class ObRaBitQFastScanComputer
{
public:
  static const int64_t BATCH_SIZE = ObRaBitQFastScanEncoder::BATCH_SIZE;

  ObRaBitQFastScanComputer();
  ~ObRaBitQFastScanComputer();

  int init(common::ObIAllocator &allocator,
           share::ObVectorIndexDistAlgorithm metric,
           const ObQuantizationParams &params,
           const float *query,
           const float *centroid);

  int compute_batch(const ObQuantizationBuffer &packed_codes,
                    const float *residual_norms,
                    const float *corrections,
                    const float *raw_norms,
                    int64_t code_count,
                    float *metric_values) const;

  int compute_bounds_batch(const ObQuantizationBuffer &packed_codes,
                           const float *residual_norms,
                           const float *corrections,
                           const float *raw_norms,
                           int64_t code_count,
                           float *metric_values,
                           float *lower_bounds,
                           float *upper_bounds) const;

  int compute_lower_bounds_batch(const ObQuantizationBuffer &packed_codes,
                                 const float *residual_norms,
                                 const float *corrections,
                                 const float *raw_norms,
                                 int64_t code_count,
                                 float *lower_bounds) const;

  // Read-only query/CID state shared by the ISA-specific compute kernels.
  share::ObVectorIndexDistAlgorithm get_metric() const { return metric_; }
  int64_t get_dim() const { return dim_; }
  float get_lut_offset() const { return lut_offset_; }
  float get_lut_scale() const { return lut_scale_; }
  float get_query_residual_norm() const { return query_residual_norm_; }
  float get_query_raw_norm() const { return query_raw_norm_; }
  float get_dim_error_scale() const { return dim_error_scale_; }
  float get_lut_dot_error() const { return lut_dot_error_; }

  void reset();

private:
  float metric_error(float base_residual_norm,
                     float correction,
                     float base_raw_norm) const;

private:
  common::ObIAllocator *allocator_;
  share::ObVectorIndexDistAlgorithm metric_;
  int64_t dim_;
  int64_t code_bytes_;
  float query_residual_norm_;
  float query_raw_norm_;
  float lut_offset_;
  float lut_scale_;
  float lut_dot_error_;
  float dim_error_scale_;
  uint8_t *quantized_luts_;
  const ObRaBitQFastScanSimdOps *simd_ops_;
  bool is_inited_;

  ObRaBitQFastScanComputer(const ObRaBitQFastScanComputer &) = delete;
  ObRaBitQFastScanComputer &operator=(const ObRaBitQFastScanComputer &) = delete;
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_RABITQ_FAST_SCAN_H_
