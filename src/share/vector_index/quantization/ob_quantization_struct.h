/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_STRUCT_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_STRUCT_H_

#include <cmath>
#include <stdint.h>

#include "lib/vector/ob_vsag_adaptor.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"

namespace oceanbase
{
namespace quantization
{

using ObQuantizationType = common::obvsag::QuantizationType;

inline float calc_inv_sqrt_dim(const int64_t dim)
{
  return 1.0F / std::sqrt(static_cast<float>(dim));
}

struct ObQuantizationBuffer
{
  uint8_t *data_;
  int64_t size_;
};

struct ObQuantizationParams
{
  int64_t dim_;
  int8_t bits_;
  ObFhtKacRotator rotator_;
  float inv_sqrt_dim_ = 0.0F;
  bool is_norm_ = false; // FP32 cosine inputs are already normalized. Zero vectors remain zero.
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_OB_QUANTIZATION_STRUCT_H_
