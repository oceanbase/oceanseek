/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_policy.h"

#include <algorithm>

namespace oceanbase
{
namespace share
{

int64_t ObSpfreshPolicy::ceil_div_(int64_t numerator, int64_t denominator)
{
  int64_t result = 0;
  if (0 < numerator && 0 < denominator) {
    result = 1 + (numerator - 1) / denominator;
  }
  return result;
}

int64_t ObSpfreshPolicy::get_target_posting_rows()
{
  return TARGET_POSTING_ROWS;
}

int64_t ObSpfreshPolicy::get_bootstrap_trigger_rows()
{
  return BOOTSTRAP_TRIGGER_ROWS;
}

int64_t ObSpfreshPolicy::get_bootstrap_center_count(int64_t live_count, int64_t target_nlist)
{
  int64_t center_count = 1;
  if (0 <= live_count && 2 <= target_nlist) {
    int64_t desired_count = ceil_div_(live_count, TARGET_POSTING_ROWS);
    desired_count = std::max(desired_count, BOOTSTRAP_MIN_CENTER_COUNT);
    center_count = std::min(target_nlist,
                            std::min(desired_count, BOOTSTRAP_MAX_CENTER_COUNT));
    center_count = std::max(center_count, 2L);
  }
  return center_count;
}

int64_t ObSpfreshPolicy::get_desired_center_count(int64_t live_count, int64_t target_nlist)
{
  int64_t desired_count = 1;
  if (0 <= live_count && 2 <= target_nlist) {
    int64_t bootstrap_floor = std::min(target_nlist, BOOTSTRAP_MIN_CENTER_COUNT);
    desired_count = std::max(bootstrap_floor,
                             ceil_div_(live_count, TARGET_POSTING_ROWS));
    desired_count = std::min(desired_count, target_nlist);
  }
  return desired_count;
}

int64_t ObSpfreshPolicy::get_split_high_rows()
{
  return TARGET_POSTING_ROWS * SPLIT_HIGH_RATIO;
}

int64_t ObSpfreshPolicy::get_merge_low_rows()
{
  return ceil_div_(TARGET_POSTING_ROWS * MERGE_LOW_RATIO_NUMERATOR,
                   MERGE_LOW_RATIO_DENOMINATOR);
}

int64_t ObSpfreshPolicy::get_merge_capacity_rows()
{
  return ceil_div_(get_split_high_rows() * MERGE_CAPACITY_RATIO_NUMERATOR,
                   MERGE_CAPACITY_RATIO_DENOMINATOR);
}

int64_t ObSpfreshPolicy::get_merge_center_floor(int64_t desired_center_count)
{
  int64_t center_floor = std::max(desired_center_count, 1L);
  center_floor += ceil_div_(center_floor * CENTER_HYSTERESIS_NUMERATOR,
                            CENTER_HYSTERESIS_DENOMINATOR);
  return center_floor;
}

int64_t ObSpfreshPolicy::get_max_pending_split_count()
{
  return MAX_PENDING_SPLIT_COUNT;
}

int64_t ObSpfreshPolicy::get_max_merge_per_round()
{
  return MAX_MERGE_PER_ROUND;
}

int64_t ObSpfreshPolicy::get_split_retry_interval_us()
{
  return SPLIT_RETRY_INTERVAL_US;
}

}  // namespace share
}  // namespace oceanbase
