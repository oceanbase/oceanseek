/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include <gtest/gtest.h>
#include "lib/stat/ob_diagnostic_info.h"

namespace oceanbase
{
namespace common
{

TEST(ObDiExperimentalFeatureFlags, bits)
{
  ObDiExperimentalFeatureFlags flags;
  // default value of _enable_di_experimental_feature_flags is 7: bit0-bit2 on, bit3/bit4 off
  flags.set_flags(7);
  ASSERT_TRUE(flags.mysql_obrequest_ref());
  ASSERT_TRUE(flags.di_rpc_cache());
  ASSERT_TRUE(flags.rpc_throttle());
  ASSERT_FALSE(flags.skip_rpc_di());
  ASSERT_FALSE(flags.skip_worker_rpc_di());

  // bit3 alone only skips the deliver stage
  flags.set_flags(15);
  ASSERT_TRUE(flags.mysql_obrequest_ref());
  ASSERT_TRUE(flags.di_rpc_cache());
  ASSERT_TRUE(flags.rpc_throttle());
  ASSERT_TRUE(flags.skip_rpc_di());
  ASSERT_FALSE(flags.skip_worker_rpc_di());

  flags.set_flags(23);
  ASSERT_TRUE(flags.rpc_throttle());
  ASSERT_FALSE(flags.skip_rpc_di());
  ASSERT_TRUE(flags.skip_worker_rpc_di());

  flags.set_flags(31);
  ASSERT_TRUE(flags.mysql_obrequest_ref());
  ASSERT_TRUE(flags.di_rpc_cache());
  ASSERT_TRUE(flags.rpc_throttle());
  ASSERT_TRUE(flags.skip_rpc_di());
  ASSERT_TRUE(flags.skip_worker_rpc_di());

  // writing 24 (bit3+bit4 without bit0-bit2) clears the original switches, as documented
  flags.set_flags(24);
  ASSERT_FALSE(flags.mysql_obrequest_ref());
  ASSERT_FALSE(flags.di_rpc_cache());
  ASSERT_FALSE(flags.rpc_throttle());
  ASSERT_TRUE(flags.skip_rpc_di());
  ASSERT_TRUE(flags.skip_worker_rpc_di());

  flags.set_flags(0);
  ASSERT_FALSE(flags.skip_rpc_di());
  ASSERT_FALSE(flags.skip_worker_rpc_di());
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
