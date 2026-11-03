/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_lighty_hash.h"

#include "memtable/ob_memtable_key.h"
#include "memtable/ob_memtable_value.h"

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

#include "utils_rowkey_builder.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

TEST(TestObLightyHash, smoke_test)
{
  typedef ObLightyHashMap<ObMemtableKey, ObMemtableValue*, DefaultPageAllocator, DefaultPageAllocator> LHash;
  static const int64_t BUCKET_NUM = 1024;

  DefaultPageAllocator hash_allocator;
  CharArena mtk_allocator;
  LHash lh(hash_allocator, hash_allocator);
  int ret = OB_SUCCESS;

  ret = lh.create(BUCKET_NUM);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t table_id = 1024;
  RK rk(
    V("hello", 5),
    V(NULL, 0),
    I(1024),
    N("3.14"),
    B(true),
    T(::oceanbase::common::ObTimeUtility::current_time()),
    U(),
    MIN(),
    MAX(),
    V("world", 5),
    V(NULL, 0),
    I(-1024),
    N("-3.14"),
    B(false),
    T(-::oceanbase::common::ObTimeUtility::current_time()),
    U(),
    MIN(),
    MAX());
  ObRowkeyXcodeBuffer eb;
  ObMemtableKey mtk;
  ObMemtableValue mtv;
  ret = mtk.encode(table_id, rk.get_rowkey(), eb);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = lh.insert(mtk, &mtv);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObMemtableValue *mtv_get = NULL;
  ret = lh.get(mtk, mtv_get);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(mtv_get, &mtv);

  ret = lh.insert(mtk, &mtv);
  EXPECT_EQ(OB_ENTRY_EXIST, ret);

  ObMemtableValue *mtv_erased = NULL;
  ret = lh.erase(mtk, &mtv_erased);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(mtv_erased, &mtv);

  ret = lh.get(mtk, mtv_get);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ret = lh.insert(mtk, &mtv);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = lh.get(mtk, mtv_get);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(mtv_get, &mtv);
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_lighty_hash.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
