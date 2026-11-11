/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstring>
#include "common/object/ob_object.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace
{

void make_outrow_disk_locator(char *buf, const ObLobId &lob_id)
{
  ObLobCommon lob_common;
  lob_common.in_row_ = 0;
  MEMCPY(buf, &lob_common, sizeof(lob_common));
  MEMCPY(buf + sizeof(lob_common), lob_id.get_string().ptr(), sizeof(lob_id));
  const int64_t seq_no = 1;
  MEMCPY(buf + sizeof(lob_common) + sizeof(lob_id), &seq_no, sizeof(seq_no));
}

void expect_invalid_lob_id(const ObLobLocatorV2 &locator)
{
  char sentinel[] = "sentinel";
  const char *expected_ptr = sentinel;
  const int64_t expected_len = sizeof(sentinel) - 1;
  ObString actual(expected_len, sentinel);
  EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_lob_id(actual));
  EXPECT_EQ(expected_ptr, actual.ptr());
  EXPECT_EQ(expected_len, actual.length());

  ObLobId expected_id;
  expected_id.assign(7, 1001, 2002);
  ObLobId actual_id;
  actual_id.assign(expected_id);
  EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_lob_id(actual_id));
  EXPECT_EQ(expected_id, actual_id);
}

} // namespace

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

TEST(TestLobId, short_c_string_still_invalid)
{
  char mutable_buf[] = "short";
  const char *const_buf = "short";
  ObLobId lob_id;

  EXPECT_EQ(OB_INVALID_ARGUMENT, lob_id.from_string(mutable_buf));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lob_id.from_string(const_buf));
}

TEST(TestLobId, outrow_disk_locator_lob_id_view)
{
  ObLobId expected;
  expected.assign(9, 200001, 0x0100020304050607ULL);
  char buf[ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE + 1] = {};
  make_outrow_disk_locator(buf, expected);

  ObString actual;
  ObLobId actual_id;
  for (uint32_t size = sizeof(ObLobCommon) + sizeof(ObLobId); size <= sizeof(buf); ++size) {
    SCOPED_TRACE(size);
    ObLobLocatorV2 locator(buf, size);
    ASSERT_EQ(OB_SUCCESS, locator.get_lob_id(actual));
    EXPECT_EQ(buf + sizeof(ObLobCommon), actual.ptr());
    ASSERT_EQ(sizeof(ObLobId), actual.length());
    EXPECT_EQ(0, MEMCMP(expected.get_string().ptr(), actual.ptr(), sizeof(ObLobId)));
    ObLobId decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.from_string(actual));
    EXPECT_EQ(expected, decoded);
    ASSERT_EQ(OB_SUCCESS, locator.get_lob_id(actual_id));
    EXPECT_EQ(expected, actual_id);
  }
  buf[sizeof(ObLobCommon)] = 0x7f;
  EXPECT_EQ(0x7f, static_cast<unsigned char>(actual.ptr()[0]));
  EXPECT_EQ(expected, actual_id);
}

TEST(TestLobId, outrow_disk_locator_to_string)
{
  ObLobId lob_id;
  lob_id.assign(9, 200001, 2002);
  char buf[ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE];
  make_outrow_disk_locator(buf, lob_id);
  ObLobLocatorV2 locator(buf, sizeof(buf));

  char expected_id[ObLobId::MAX_TO_STRING_BUF_SIZE] = {};
  ASSERT_GT(lob_id.to_string(expected_id, sizeof(expected_id)), 0);
  char print_buf[1024] = {};
  ASSERT_GT(locator.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_NE(nullptr, std::strstr(print_buf, "lob_id"));
  EXPECT_NE(nullptr, std::strstr(print_buf, expected_id));
  EXPECT_NE(nullptr, std::strstr(print_buf, "column_id"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "tablet_id"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "auto_inc_id"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "seq_no"));

  char truncated_buf[sizeof(ObLobCommon) + sizeof(ObLobId)];
  MEMCPY(truncated_buf, buf, sizeof(truncated_buf));
  ObLobLocatorV2 truncated_locator(truncated_buf, sizeof(truncated_buf));
  MEMSET(print_buf, 0, sizeof(print_buf));
  ASSERT_GT(truncated_locator.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_NE(nullptr, std::strstr(print_buf, "size"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "version"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "in_row"));
  EXPECT_NE(nullptr, std::strstr(print_buf, "lob_id"));
  EXPECT_NE(nullptr, std::strstr(print_buf, expected_id));
  EXPECT_EQ(nullptr, std::strstr(print_buf, "seq_no"));
}

TEST(TestLobId, invalid_disk_locator_lob_id)
{
  ObLobId lob_id;
  lob_id.assign(9, 200001, 2002);
  char buf[ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE + 1] = {};
  make_outrow_disk_locator(buf, lob_id);

  expect_invalid_lob_id(ObLobLocatorV2());
  expect_invalid_lob_id(ObLobLocatorV2(nullptr, ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE));
  expect_invalid_lob_id(ObLobLocatorV2(buf, 0));
  for (uint32_t size = 1; size < sizeof(ObLobCommon) + sizeof(ObLobId); ++size) {
    SCOPED_TRACE(size);
    expect_invalid_lob_id(ObLobLocatorV2(buf, size));
  }

  ObLobCommon lob_common;
  lob_common.in_row_ = 0;
  lob_common.version_ = 0;
  MEMCPY(buf, &lob_common, sizeof(lob_common));
  expect_invalid_lob_id(ObLobLocatorV2(buf, ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE));
  lob_common.version_ = ObLobCommon::LOB_DATA_VERSION;
  lob_common.reserved_ = 1;
  MEMCPY(buf, &lob_common, sizeof(lob_common));
  expect_invalid_lob_id(ObLobLocatorV2(buf, ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE));
  lob_common.reserved_ = 0;
  lob_common.in_row_ = 1;
  MEMCPY(buf, &lob_common, sizeof(lob_common));
  expect_invalid_lob_id(ObLobLocatorV2(buf, ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE));
  lob_common.in_row_ = 0;
  lob_common.is_mem_loc_ = 1;
  MEMCPY(buf, &lob_common, sizeof(lob_common));
  expect_invalid_lob_id(ObLobLocatorV2(buf, ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE));
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
