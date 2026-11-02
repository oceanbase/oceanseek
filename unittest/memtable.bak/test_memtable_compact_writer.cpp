/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_memtable_compact_writer.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObMemtableCompactWriter, smoke_test)
{
  CharArena allocator;
  ObMemtableCompactWriter *mcw_nil = NULL;
  ObTransDataNode *tdn_nil = NULL;

  ObMemtableCompactWriter *mcw = ObMemtableCompactWriter::alloc();
  ASSERT_NE(mcw_nil, mcw);

  ObTransDataNode *tdn = mcw->copy_to_node(allocator);
  EXPECT_NE(tdn_nil, tdn);

  ObMemtableCompactWriter::free(mcw);
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_memtable_compact_writer.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
