/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_PX_OB_PX_SAMPLE_UTIL_H_
#define OCEANBASE_SQL_ENGINE_PX_OB_PX_SAMPLE_UTIL_H_

#include <stdint.h>

namespace oceanbase
{
namespace sql
{

inline int64_t get_one_thread_sampling_count_by_parallel(const int64_t parallel)
{
  int64_t sampling_count = parallel;
  if (sampling_count < 192 && sampling_count > 0) {
    const int64_t factor_count = 6;
    const int64_t amplification_factors[factor_count] = { 96, 48, 24, 12, 6, 3 };
    int64_t factor_idx = -1;
    for (int64_t value = sampling_count; value > 1; value >>= 1) {
      ++factor_idx;
    }
    if (factor_idx >= 0 && factor_idx < factor_count) {
      sampling_count *= amplification_factors[factor_idx];
    }
  }
  return sampling_count;
}

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_PX_OB_PX_SAMPLE_UTIL_H_
