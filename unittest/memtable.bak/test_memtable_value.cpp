/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_memtable_value.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtableValue, smoke_test)
{
  ObMemtableValue mtv;

  ObCStringHelper helper;
  fprintf(stdout, "mtv=[%s]\n", helper.convert(mtv));
  fprintf(stdout, "mtv=[%s]\n", helper.convert(mtv, true));
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_memtable_value.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
