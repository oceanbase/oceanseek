/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_query_engine.h"

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

#include "utils_rowkey_builder.h"
#include "utils_mod_allocator.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

#define INIT_MTV(mtv, node) \
  mtv.list_head_ = &node; \
  mtv.list_tail_ = &node;

#define TEST_SET_AND_GET(mtk, mtv) \
  { \
    ret = qe.set(mtk, &mtv); \
    EXPECT_EQ(OB_SUCCESS, ret); \
    ObMemtableValue *v = qe.get(mtk, NULL); \
    EXPECT_EQ(v, &mtv); \
  }

#define TEST_SCAN(start, include_start, end, include_end) \
  { \
    ObQueryEngineIterator qei; \
    ret = qe.scan(mtk[start], 0, !include_start, mtk[end], 0, !include_end, qei); \
    EXPECT_EQ(OB_SUCCESS, ret); \
    if (start <= end) \
    { \
      for (int64_t i = (include_start ? start : (start + 1)); i <= (include_end ? end : (end - 1)); i++) \
      { \
        ret = qei.next(); \
        EXPECT_EQ(OB_SUCCESS, ret); \
        EXPECT_EQ(mtk[i], qei.get_key()); \
        EXPECT_EQ(&mtv[i], qei.get_value()); \
      } \
    } \
    else \
    { \
      for (int64_t i = (include_start ? start : (start - 1)); i >= (include_end ? end : (end + 1)); i--) \
      { \
        ret = qei.next(); \
        EXPECT_EQ(OB_SUCCESS, ret); \
        EXPECT_EQ(mtk[i], qei.get_key()); \
        EXPECT_EQ(&mtv[i], qei.get_value()); \
      } \
    } \
    ret = qei.next(); \
    EXPECT_EQ(OB_ITER_END, ret); \
    ret = qei.next(); \
    EXPECT_EQ(OB_ITER_END, ret); \
  }

TEST(TestObQueryEngine, smoke_test)
{
  const int64_t R_COUNT = 6;
  const int64_t QE_SIZE = 1024;

  ObModAllocator allocator;
  ObQueryEngine qe(allocator);
  int ret = OB_SUCCESS;

  ret = qe.init(QE_SIZE);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObMemtableKey mtk[R_COUNT];
  INIT_MTK(allocator, mtk[0], 1000, V("aaaa", 4), I(1024), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[1], 1000, V("aaaa", 4), I(1024), N("1234567890.01234567891"));
  INIT_MTK(allocator, mtk[2], 1000, V("aaaa", 4), I(2048), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[3], 1000, V("aaaa", 4), I(2048), N("1234567890.01234567891"));
  INIT_MTK(allocator, mtk[4], 1000, V("bbbb", 4), I(2048), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[5], 1000, V("bbbb", 4), I(2048), N("1234567890.01234567891"));

  ObTransDataNode tdn[R_COUNT];
  ObMemtableValue mtv[R_COUNT];
  INIT_MTV(mtv[0], tdn[0]);
  INIT_MTV(mtv[1], tdn[1]);
  INIT_MTV(mtv[2], tdn[2]);
  INIT_MTV(mtv[3], tdn[3]);
  INIT_MTV(mtv[4], tdn[4]);
  INIT_MTV(mtv[5], tdn[5]);

  TEST_SET_AND_GET(mtk[0], mtv[0]);
  TEST_SET_AND_GET(mtk[1], mtv[1]);
  TEST_SET_AND_GET(mtk[2], mtv[2]);
  TEST_SET_AND_GET(mtk[3], mtv[3]);
  TEST_SET_AND_GET(mtk[4], mtv[4]);
  TEST_SET_AND_GET(mtk[5], mtv[5]);

  EXPECT_EQ(R_COUNT, qe.btree_size());
  EXPECT_EQ(R_COUNT, qe.hash_size());

  TEST_SCAN(0, true,  5, true);
  TEST_SCAN(0, false, 5, true);
  TEST_SCAN(0, true,  5, false);
  TEST_SCAN(0, false, 5, false);

  TEST_SCAN(5, true,  0, true);
  TEST_SCAN(5, false, 0, true);
  TEST_SCAN(5, true,  0, false);
  TEST_SCAN(5, false, 0, false);

  TEST_SCAN(1, true,  4, true);
  TEST_SCAN(1, false, 4, true);
  TEST_SCAN(1, true,  4, false);
  TEST_SCAN(1, false, 4, false);

  TEST_SCAN(4, true,  1, true);
  TEST_SCAN(4, false, 1, true);
  TEST_SCAN(4, true,  1, false);
  TEST_SCAN(4, false, 1, false);

  TEST_SCAN(0, true,  0, true);
  TEST_SCAN(1, true,  1, true);
  TEST_SCAN(2, true,  2, true);
  TEST_SCAN(3, true,  3, true);
  TEST_SCAN(4, true,  4, true);
  TEST_SCAN(5, true,  5, true);

  TEST_SCAN(0, false,  0, false);
  TEST_SCAN(1, false,  1, false);
  TEST_SCAN(2, false,  2, false);
  TEST_SCAN(3, false,  3, false);
  TEST_SCAN(4, false,  4, false);
  TEST_SCAN(5, false,  5, false);
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_query_engine.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
