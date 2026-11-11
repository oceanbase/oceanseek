/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_FHT_KAC_ROTATOR_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_FHT_KAC_ROTATOR_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace quantization
{

class ObFhtKacRotator
{
public:
  static const int32_t CURRENT_FHT_ALGO_VERSION = 1;
  static const int64_t ROUND_COUNT = 4;

  ObFhtKacRotator();

  static int generate(common::ObIAllocator &allocator,
                      int64_t dim,
                      ObFhtKacRotator &output,
                      int32_t fht_algo_version = CURRENT_FHT_ALGO_VERSION);

  // Signs storage is borrowed and must outlive this rotator and all its copies;
  // It must remain unchanged while any copy or concurrent consumer uses it.
  // Reset and destruction never free it.
  int init(const uint8_t *signs, int64_t signs_size);
  void reset();
  bool is_inited() const;
  const uint8_t *signs() const;
  int64_t signs_size() const;
  int rotate(int64_t dim, float *values) const;

private:
  static int64_t sign_bytes_per_round(int64_t dim);
  static int64_t greatest_power_of_two(int64_t dim);

  const uint8_t *signs_;
  int64_t signs_size_;
};

} // namespace quantization
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_QUANTIZATION_RABITQ_QUANTIZATION_OB_FHT_KAC_ROTATOR_H_
