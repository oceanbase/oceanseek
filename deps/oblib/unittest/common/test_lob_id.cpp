/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "common/object/ob_object.h"

using namespace oceanbase;
using namespace oceanbase::common;

TEST(TestLobId, binary_string_round_trip)
{
  const unsigned char expected[] = {
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
      0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
      0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28};
  ObLobId source;
  source.assign(0x0102030405060708ULL,
                0x1112131415161718ULL,
                0x2122232425262728ULL);

  const ObString binary = source.get_string();
  ASSERT_EQ(sizeof(ObLobId), binary.length());
  ASSERT_EQ(0, MEMCMP(expected, binary.ptr(), sizeof(expected)));

  ObLobId decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.from_string(binary));
  ASSERT_EQ(source, decoded);
  ASSERT_EQ(0x0102030405060708ULL, decoded.get_column_idx());
  ASSERT_EQ(0x1112131415161718ULL, decoded.get_tablet_id());
  ASSERT_EQ(0x2122232425262728ULL, decoded.get_auto_inc_id());
}

TEST(TestLobId, invalid_binary_string_length)
{
  char buf[sizeof(ObLobId) + 1] = {};
  ObLobId lob_id;

  ASSERT_EQ(OB_INVALID_ARGUMENT, lob_id.from_string(ObString()));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            lob_id.from_string(ObString(sizeof(ObLobId) - 1, buf)));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            lob_id.from_string(ObString(sizeof(ObLobId) + 1, buf)));
  ASSERT_EQ(OB_SUCCESS, lob_id.from_string(ObString(sizeof(ObLobId), buf)));
  ASSERT_FALSE(lob_id.is_valid());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
