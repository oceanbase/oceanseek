/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_prewarm_parallelism_controller.h"

namespace oceanbase
{
namespace storage
{

void ObPrewarmParController::try_inc_parallelism(
    const int64_t max_parallelism,
    int64_t &parallelism)
{
  if (parallelism < max_parallelism) {
    if (parallelism * 2 < max_parallelism) {
      parallelism = parallelism * 2;
    } else {
      parallelism = max_parallelism;
    }
  }
}

void ObPrewarmParController::try_dec_parallelism(
    const int64_t min_parallelism,
    int64_t &parallelism)
{
  if (parallelism > min_parallelism) {
    if ((parallelism * 3 / 4) < min_parallelism) {
      parallelism = min_parallelism;
    } else {
      parallelism = (parallelism * 3 / 4);
    }
  }
}

void ObPrewarmParController::try_adjust_parallelism(
    const bool has_failed_io,
    const int64_t min_parallelism,
    const int64_t max_parallelism,
    int64_t &parallelism)
{
  if (has_failed_io) {
    consecutive_succ_cnt_ = 0;
    try_dec_parallelism(min_parallelism, parallelism);
  } else {
    consecutive_succ_cnt_++;
    if (consecutive_succ_cnt_ > 10) {
      try_inc_parallelism(max_parallelism, parallelism);
      consecutive_succ_cnt_ = 0;
    }
  }
}

} /* namespace storage */
} /* namespace oceanbase */
