/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>

#define protected public
#define private public

#include "storage/lob/access/ob_lob_retry.h"
#include "storage/lob/common/ob_lob_util.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobUtil : public ::testing::Test
{
public:
  TestLobUtil() {}
  virtual ~TestLobUtil() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

//==================== adjust_range ====================
TEST_F(TestLobUtil, adjust_range_in_range)
{
  uint64_t offset = 10;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(10, offset);
  ASSERT_EQ(50, len);
}

TEST_F(TestLobUtil, adjust_range_offset_overflow)
{
  uint64_t offset = 200;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(100, offset);
  ASSERT_EQ(0, len);
}

TEST_F(TestLobUtil, adjust_range_len_overflow)
{
  uint64_t offset = 80;
  uint64_t len = 50;
  adjust_range(100, offset, len);
  ASSERT_EQ(80, offset);
  ASSERT_EQ(20, len); // clamped to length - offset
}

//==================== convert_char_range_to_byte_range ====================
TEST_F(TestLobUtil, convert_range_ascii)
{
  const char *data = "abcdefghij"; // 10 ascii chars
  uint64_t char_offset = 2;
  uint64_t char_length = 3;
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_BIN, data, 10,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(2, byte_offset);
  ASSERT_EQ(3, byte_length);
}

TEST_F(TestLobUtil, convert_range_multibyte)
{
  // five "中", each 3 bytes in utf8mb4 -> 15 bytes
  const char *data = "\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad";
  uint64_t char_offset = 1;
  uint64_t char_length = 2;
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_GENERAL_CI, data, 15,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(3, byte_offset);  // skip 1 char = 3 bytes
  ASSERT_EQ(6, byte_length);  // 2 chars = 6 bytes
}

TEST_F(TestLobUtil, convert_range_clamp)
{
  const char *data = "abcdefghij"; // 10 chars
  uint64_t char_offset = 8;
  uint64_t char_length = 100; // beyond remaining chars
  uint64_t byte_offset = 0;
  uint64_t byte_length = 0;
  convert_char_range_to_byte_range(CS_TYPE_UTF8MB4_BIN, data, 10,
                                   char_offset, char_length, byte_offset, byte_length);
  ASSERT_EQ(8, byte_offset);
  ASSERT_EQ(2, byte_length); // clamped to remaining 2 chars
}

//==================== ObLobCharsetUtil::get_collation_type ====================
TEST_F(TestLobUtil, get_collation_type_json_to_binary)
{
  ASSERT_EQ(CS_TYPE_BINARY,
            ObLobCharsetUtil::get_collation_type(ObJsonType, CS_TYPE_UTF8MB4_BIN));
}

TEST_F(TestLobUtil, get_collation_type_non_json_keep)
{
  ASSERT_EQ(CS_TYPE_UTF8MB4_BIN,
            ObLobCharsetUtil::get_collation_type(ObVarcharType, CS_TYPE_UTF8MB4_BIN));
}

TEST_F(TestLobUtil, tenant_not_in_server_can_retry)
{
  ASSERT_TRUE(ObLobRetryUtil::is_remote_ret_can_retry(OB_TENANT_NOT_IN_SERVER));
  ASSERT_FALSE(ObLobRetryUtil::is_remote_ret_can_retry(OB_TENANT_HAS_BEEN_DROPPED));
}

//==================== ObLobCharsetUtil::transform_query_result_charset ====================
TEST_F(TestLobUtil, transform_query_result_ascii)
{
  const char *data = "abcdefghij"; // 10 ascii chars
  uint32_t byte_st = 2;  // in: char offset
  uint32_t byte_len = 3; // in: char count
  ObLobCharsetUtil::transform_query_result_charset(CS_TYPE_UTF8MB4_BIN, data, 10, byte_len, byte_st);
  ASSERT_EQ(2u, byte_st);
  ASSERT_EQ(3u, byte_len);
}

TEST_F(TestLobUtil, transform_query_result_multibyte)
{
  const char *data = "\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad\xe4\xb8\xad"; // 5 "中" = 15 bytes
  uint32_t byte_st = 1;  // in: char offset
  uint32_t byte_len = 2; // in: char count
  ObLobCharsetUtil::transform_query_result_charset(CS_TYPE_UTF8MB4_GENERAL_CI, data, 15, byte_len, byte_st);
  ASSERT_EQ(3u, byte_st);
  ASSERT_EQ(6u, byte_len);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_util.log*");
  OB_LOGGER.set_file_name("test_lob_util.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
