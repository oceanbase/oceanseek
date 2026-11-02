/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "memtable/ob_memtable_key.h"

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

TEST(TestObMemtableKey, test_encode_decode)
{
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
  ObRowkeyXcodeBuffer db;
  ObMemtableKey mtk;
  int ret = OB_SUCCESS;

  ret = mtk.encode(table_id, rk.get_rowkey(), eb);
  EXPECT_EQ(OB_SUCCESS, ret);

  int64_t first_encode_len = eb.get_position();
  int64_t rowkey_len = 0;
  ret = mtk.decode_rowkey_len(rowkey_len);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), rowkey_len);

  ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  uint64_t decode_table_id = OB_INVALID_ID;
  ObRowkey decode_rk(objs, rowkey_len);
  ret = mtk.decode(decode_table_id, decode_rk, db);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(table_id, decode_table_id);
  EXPECT_EQ(rk.get_rowkey(), decode_rk);

  int64_t orig_encode_len = eb.get_position();
  ret = mtk.encode(decode_table_id, decode_rk, eb);
  EXPECT_EQ(OB_SUCCESS, ret);
  int64_t second_encode_len = eb.get_position() - orig_encode_len;
  EXPECT_EQ(first_encode_len, second_encode_len);

  CharArena allocator;
  ObMemtableKey copied_mtk;
  ret = copied_mtk.deep_copy(mtk, allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(mtk, copied_mtk);

  ObCStringHelper helper;
  fprintf(stdout, "mtk=[%s]\n", helper.convert(mtk));
}

}
}

int main(int argc, char **argv)
{
  _OB_LOGGER.setFileName("test_memtable_key.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
