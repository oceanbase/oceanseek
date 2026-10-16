/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_POLICY_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_POLICY_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{

class ObSpfreshPolicy
{
public:
  static int64_t get_target_posting_rows();
  static int64_t get_bootstrap_trigger_rows();
  static int64_t get_bootstrap_center_count(int64_t live_count, int64_t target_nlist);
  static int64_t get_desired_center_count(int64_t live_count, int64_t target_nlist);
  static int64_t get_split_high_rows();
  static int64_t get_merge_low_rows();
  static int64_t get_merge_capacity_rows();
  static int64_t get_merge_center_floor(int64_t desired_center_count);
  static int64_t get_max_pending_split_count();
  static int64_t get_max_merge_per_round();
  static int64_t get_split_retry_interval_us();

private:
  static constexpr int64_t TARGET_POSTING_ROWS = 1000;
  static constexpr int64_t BOOTSTRAP_TRIGGER_ROWS = 8192;
  static constexpr int64_t BOOTSTRAP_MIN_CENTER_COUNT = 16;
  static constexpr int64_t BOOTSTRAP_MAX_CENTER_COUNT = 32;
  static constexpr int64_t SPLIT_HIGH_RATIO = 2;
  static constexpr int64_t MERGE_LOW_RATIO_NUMERATOR = 1;
  static constexpr int64_t MERGE_LOW_RATIO_DENOMINATOR = 10;
  static constexpr int64_t MERGE_CAPACITY_RATIO_NUMERATOR = 8;
  static constexpr int64_t MERGE_CAPACITY_RATIO_DENOMINATOR = 10;
  static constexpr int64_t CENTER_HYSTERESIS_NUMERATOR = 5;
  static constexpr int64_t CENTER_HYSTERESIS_DENOMINATOR = 100;
  static constexpr int64_t MAX_PENDING_SPLIT_COUNT = 8;
  static constexpr int64_t MAX_MERGE_PER_ROUND = 1;
  static constexpr int64_t SPLIT_RETRY_INTERVAL_US = 1L * 60L * 1000L * 1000L;

  static int64_t ceil_div_(int64_t numerator, int64_t denominator);

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshPolicy);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_POLICY_H_
