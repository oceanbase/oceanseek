/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RPC_TEST

#include <gtest/gtest.h>
#include <fstream>
#include "lib/json_type/ob_json_bin.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

class TestObMySQLUtil
    : public ::testing::Test
{
public:
  TestObMySQLUtil()
  {}

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
protected:
};

#define PREPEND_ZEROS(char_size, offset, src_str, result_str) { \
    memset(buf, 0, 1024); \
    memcpy(buf, src_str, char_size); \
    ObMySQLUtil::prepend_zeros(buf, char_size, offset); \
    EXPECT_TRUE(0 == strcmp(buf, result_str)); \
}

#define INT_CELL_STR(val, is_unsigned, obj_type, type, zerofill, zflength, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result<<"value = "<<val<<", is_unsigned = "<<is_unsigned<<", zerofill = "<< zerofill<<", len = "<<zflength<<std::endl; \
    ASSERT_EQ(ERR, ObMySQLUtil::int_cell_str(buf, len, val, obj_type, is_unsigned, type, pos, zerofill, zflength)); \
    if (OB_SUCCESS == ERR) { \
      of_result << buf << std::endl; \
    } else { \
      of_result << "err" << std::endl; \
    } \
}

#define FLOAT_CELL_STR(val, type, scale, zerofill, zflength, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result<<"value="<<val<<",scale="<<scale<<",zerofill="<<zerofill<<",zflength"<<zflength<<std::endl; \
    ASSERT_EQ(ERR, ObMySQLUtil::float_cell_str(buf, len, val, type, pos, scale, zerofill, zflength)); \
    if (OB_SUCCESS == ERR) { \
      of_result << buf << std::endl; \
    } else { \
      of_result << "err" << std::endl; \
    } \
}

#define DOUBLE_CELL_STR(val, type, scale, zerofill, zflength, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result<<"value="<<val<<",scale="<<scale<<",zerofill="<<zerofill<<",zflength"<<zflength<<std::endl; \
    ASSERT_EQ(ERR, ObMySQLUtil::double_cell_str(buf, len, val, type, pos, scale, zerofill, zflength)); \
    if (OB_SUCCESS == ERR) { \
      of_result << buf << std::endl; \
    } else { \
      of_result << "err" << std::endl; \
    } \
}

#define DATETIME_CELL_STR(val_str, type, ERR) { \
  memset(buf, 0, 1024);                         \
  pos = 0;                                      \
  of_result<<"val = "<< val_str << std::endl;   \
  int64_t val = 0;                              \
  ObTimeConvertCtx cvrt_ctx(&tz_info, true);                            \
  ASSERT_EQ(ERR, ObTimeConverter::str_to_datetime(val_str, cvrt_ctx, val, &scale)); \
  if (ERR == OB_SUCCESS) {                                              \
    ASSERT_EQ(ERR, ObMySQLUtil::datetime_cell_str(buf, len, val, type, pos, &tz_info, scale)); \
    if (OB_SUCCESS == ERR) {                                            \
      of_result << buf << std::endl;                                    \
    } else {                                                            \
      of_result << "err" << std::endl;                                  \
    }                                                                   \
  } else {                                                              \
    of_result << "invalid datetime" << std::endl;                       \
  }                                                                     \
  }

#define DATE_CELL_STR(val_str, type, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result<<"val = "<< val_str << std::endl; \
    int32_t val = 0; \
    ASSERT_EQ(ERR, ObTimeConverter::str_to_date(val_str, val)); \
    if (ERR == OB_SUCCESS) { \
      ASSERT_EQ(ERR, ObMySQLUtil::date_cell_str(buf, len, val, type, pos)); \
      if (OB_SUCCESS == ERR) { \
        of_result << buf << " "<< std::endl; \
      } else { \
        of_result << "err" << std::endl; \
      } \
    } else { \
      of_result << "invalid date" << std::endl; \
    } \
}

#define TIME_CELL_STR(val_str, type, scale, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result << "val = " << val_str << "scale = " << scale << std::endl; \
    int64_t val = 0; \
    ASSERT_EQ(ERR, ObTimeConverter::str_to_time(val_str, val, &scale)); \
    if (OB_SUCCESS == ERR) { \
      ASSERT_EQ(ERR, ObMySQLUtil::time_cell_str(buf, len, val, type, pos, scale)); \
      if (OB_SUCCESS == ERR) { \
        of_result << buf << std::endl; \
      } else { \
        of_result << "err" << std::endl; \
      } \
    } else { \
      of_result << "invalid time" << std::endl; \
    } \
}

#define YEAR_CELL_STR(yval, type, ERR) { \
  memset(buf, 0, 1024); \
  pos = 0; \
  of_result << "val = "<< yval <<std::endl; \
  uint8_t val = 0; \
  ASSERT_EQ(ERR, ObTimeConverter::int_to_year(yval, val)); \
  if (OB_SUCCESS == ERR) { \
    ASSERT_EQ(ERR, ObMySQLUtil::year_cell_str(buf, len, val, type, pos)); \
    if (OB_SUCCESS == ERR) { \
      of_result << buf << std::endl; \
    } else { \
      of_result << "err" << std::endl; \
    } \
  } else { \
    of_result << "invalid year" << std::endl; \
  } \
}

#define VARCHAR_CELL_STR(str, str_len, ERR) { \
    memset(buf, 0, 1024); \
    pos = 0; \
    of_result << "val = "; \
    for (int i = 0 ; i < str_len ; i++) { \
      of_result << str[i] ; \
    } \
    of_result << std::endl; \
    of_result << "str_len = " << str_len << std::endl; \
    ObString val; \
    val.assign_ptr(str, str_len); \
    ASSERT_EQ(ERR, ObMySQLUtil::varchar_cell_str(buf, len, val, false, pos)); \
    if (OB_SUCCESS == ERR) { \
      of_result << buf << std::endl; \
    } else { \
      of_result << "err" << std::endl; \
    } \
}

#define NUMBER_CELL_STR(val, scale, zerofill, zflen, ERR) { \
  memset(buf, 0, 1024); \
  pos = 0; \
  of_result << "val" << val.format() << ", scale = " <<  \
  scale << ", zerofill=" << zerofill <<", zflen = " << zflen << std::endl; \
  ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::number_cell_str(buf, len, val, pos, scale, zerofill, zflen)); \
  if (OB_SUCCESS == ERR) { \
    of_result << buf << std::endl; \
  } else { \
    of_result << "err" << std::endl; \
  } \
}

TEST_F(TestObMySQLUtil, TestPrependZero)
{
  char buf[1024];
  // test func prepend_zeros
  // char_size, offset, src_str, result_str
  PREPEND_ZEROS(5, 3, "hello", "000hello");
  PREPEND_ZEROS(5, 0, "hello", "hello");
  PREPEND_ZEROS(5, 1, "hello", "0hello");
  PREPEND_ZEROS(5, 2, "hello", "00hello");
  PREPEND_ZEROS(0, 2, "hello", "00");
  PREPEND_ZEROS(0, 5, "hello", "00000");
  //
  LOG_INFO("buf", K(ObString(buf)));
}

TEST_F(TestObMySQLUtil, serialize_test)
{
  const char *tmp_file = "test_mysql_util.tmp";
  const char *result_file = "test_mysql_util.result";
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  char buf[1024];
  char bitmap[1024];
  memset(buf, 0, sizeof(buf));
  memset(bitmap, 0, sizeof(bitmap));
  int64_t len = 1024;
  int64_t pos = 0;
  // test func int_cell_str
  int ret = OB_SUCCESS;
  of_result << "***********INT_TEST***********"<<std::endl;
  INT_CELL_STR(10, true, ObUInt64Type, TEXT, true, 5, ret);
  INT_CELL_STR(100, true, ObUInt64Type, TEXT, true, 5, ret);
  INT_CELL_STR(100, false, ObIntType, TEXT, true, 5, ret);
  INT_CELL_STR(100, true, ObUInt64Type, TEXT, false, 5, ret);
  INT_CELL_STR(100, false, ObIntType, TEXT, false, 5, ret);
  INT_CELL_STR(-100, false, ObIntType, TEXT, false, 5, ret);
  INT_CELL_STR(-100, true, ObUInt64Type, TEXT, false, 5, ret);
  INT_CELL_STR(-1, false, ObIntType, TEXT, true, 5, ret);
  INT_CELL_STR(0, false, ObIntType, TEXT, false, 5, ret);
  INT_CELL_STR(18446744073709551615ULL, false, ObIntType, TEXT, false, 5, ret);
  INT_CELL_STR(18446744073709551615ULL, true, ObUInt64Type, TEXT, false, 5, ret);
  INT_CELL_STR(18446744073709551615ULL, false, ObIntType, TEXT, true, 5, ret);
  INT_CELL_STR(18446744073709551615ULL, true, ObUInt64Type, TEXT, true, 20, ret);
  INT_CELL_STR(18446744073709551615ULL, true, ObUInt64Type, TEXT, true, 21, ret);
  INT_CELL_STR(9223372036854775807LL, true, ObUInt64Type, TEXT, true, 20, ret);
  INT_CELL_STR(9223372036854775807LL, false, ObIntType, TEXT, true, 20, ret);
  INT_CELL_STR(9223372036854775807LL, false, ObIntType, TEXT, false, 20, ret);
  INT_CELL_STR(-9223372036854775807LL, false, ObIntType, TEXT, false, 20, ret);
  INT_CELL_STR(-9223372036854775807LL, true, ObUInt64Type, TEXT, false, 20, ret);
  INT_CELL_STR(-9223372036854775807LL, false, ObIntType, TEXT, true, 20, ret);
  INT_CELL_STR(-9223372036854775807LL, true, ObUInt64Type, TEXT, true, 20, ret);
  INT_CELL_STR(0, false, ObIntType, TEXT, false, 10000, ret);

  // error
  ret = OB_SIZE_OVERFLOW;
  INT_CELL_STR(0, false, ObIntType, TEXT, true, 10000, ret);

  // test func float_cell_str
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********FLOAT_TEST***********"<<std::endl;
  FLOAT_CELL_STR(1.5, TEXT, 1, true, 5, ret);
  FLOAT_CELL_STR(1.5, TEXT, 1, true, 5, ret);
  FLOAT_CELL_STR(1.5, TEXT, 10, true, 10, ret);
  FLOAT_CELL_STR(1.50, TEXT, 10, true, 10, ret);
  float fval = 1.0;
  FLOAT_CELL_STR(fval, TEXT, 10, true, 10, ret);
  FLOAT_CELL_STR(fval, TEXT, 20, true, 10, ret);
  FLOAT_CELL_STR(fval, TEXT, 20, true, 20, ret);
  FLOAT_CELL_STR(fval, TEXT, 10, true, 20, ret);
  double dval = 1e-15;
  of_result << std::endl;
  of_result << "***********DOUBLE_TEST***********"<<std::endl;
  DOUBLE_CELL_STR(dval, TEXT, 10, true, 20, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, true, 20, ret);
  dval = 1e-16;
  DOUBLE_CELL_STR(dval, TEXT, 10, true, 20, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, true, 20, ret);
  DOUBLE_CELL_STR(0, TEXT, 20, true, 20, ret);
  DOUBLE_CELL_STR(1.555555555, TEXT, 10, true, 15, ret);
  DOUBLE_CELL_STR(1.0000000005, TEXT, 5, true, 15, ret);
  dval = 1.23e+12;
  DOUBLE_CELL_STR(dval, TEXT, 5, true, 15, ret);
  DOUBLE_CELL_STR(dval, TEXT, 5, true, 10, ret);
  DOUBLE_CELL_STR(dval, TEXT, 0, true, 5, ret);
  DOUBLE_CELL_STR(dval, TEXT, 0, false, 20, ret);
  DOUBLE_CELL_STR(dval, TEXT, 3, true, 20, ret);
  dval = 1.23333333333333333333e+10;
  DOUBLE_CELL_STR(dval, TEXT, 5, false, 10, ret);
  DOUBLE_CELL_STR(dval, TEXT, 5, true, 30, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, true, 10, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, true, 30, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, false, 40, ret);
  DOUBLE_CELL_STR(dval, TEXT, 20, true, 40, ret);

  // test func datetime_cell_str
  ret = OB_SUCCESS;
  ObTimeZoneInfo tz_info;
  int16_t scale = 0;
  of_result << std::endl;
  of_result << "***********DATETIME_TEST***********"<<std::endl;
  DATETIME_CELL_STR("2011-01-01 12:01:01", TEXT, ret);
  DATETIME_CELL_STR("2011-01-01 12:01:01.555", TEXT, ret);
  DATETIME_CELL_STR("99991231235959", TEXT, ret);
  DATETIME_CELL_STR("99991231235959.999999", TEXT, ret);
  DATETIME_CELL_STR("0000-00-00 00:00:00", TEXT, ret);
  DATETIME_CELL_STR("1000-01-01 00:00:00", TEXT, ret);
  DATETIME_CELL_STR("2011-01-01 12:01:01.555555", TEXT, ret);
  DATETIME_CELL_STR("2011-01-01 23:01:01.5", TEXT, ret);
  DATETIME_CELL_STR("2011-12-01 12:01:01.55", TEXT, ret);

  // error
  ret = OB_INVALID_DATE_VALUE;
  DATETIME_CELL_STR("0000-00-00 00:00:00.555", TEXT, ret);
  DATETIME_CELL_STR("2011-13-01 12:01:01.55", TEXT, ret);
  ret = OB_DATETIME_FUNCTION_OVERFLOW;
  DATETIME_CELL_STR("99991231235959.9999999", TEXT, ret);

  // test func date_cell_str  length=10 -> '\n'
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********DATE_TEST***********"<<std::endl;
  DATE_CELL_STR("2011-01-01", TEXT, ret);
  DATE_CELL_STR("2011-01-01", TEXT, ret);
  DATE_CELL_STR("99991231", TEXT, ret);
  DATE_CELL_STR("99990131", TEXT, ret);
  DATE_CELL_STR("0000-00-00", TEXT, ret);
  DATE_CELL_STR("1000-01-01", TEXT, ret);
  DATE_CELL_STR("2011-01-01", TEXT, ret);
  DATE_CELL_STR("2018-12-01", TEXT, ret);
  DATE_CELL_STR("2011-12-01", TEXT, ret);

  // error
  ret = OB_INVALID_DATE_VALUE;
  DATE_CELL_STR("10000-00-00", TEXT, ret);
  DATE_CELL_STR("99990132", TEXT, ret);
  DATE_CELL_STR("2011-13-01", TEXT, ret);

  //test func time_cell_str
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********TIME_TEST***********"<<std::endl;
  TIME_CELL_STR("23:00:00", TEXT, scale, ret);
  TIME_CELL_STR("00:00:00", TEXT, scale, ret);
  TIME_CELL_STR("12:00:00.1233", TEXT, scale, ret);
  TIME_CELL_STR("12:00:00.123323232", TEXT, scale, ret);
  TIME_CELL_STR("-838:59:59", TEXT, scale, ret);
  TIME_CELL_STR("838:59:59", TEXT, scale, ret);

  //err
  ret = OB_ERR_TRUNCATED_WRONG_VALUE;
  TIME_CELL_STR("838:59:59.1212", TEXT, scale, ret);
  TIME_CELL_STR("-838:59:59.1212", TEXT, scale, ret);
  TIME_CELL_STR("839:59:59.9999999", TEXT, scale, ret);
  TIME_CELL_STR("-839:59:59.9999999", TEXT, scale, ret);

  //test func year_cell_str
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********YEAR_TEST***********"<<std::endl;
  YEAR_CELL_STR(2012, TEXT, ret);
  YEAR_CELL_STR(0, TEXT, ret);
  YEAR_CELL_STR(1901, TEXT, ret);
  YEAR_CELL_STR(2155, TEXT, ret);
  YEAR_CELL_STR(12, TEXT, ret);
  YEAR_CELL_STR(69, TEXT, ret);
  YEAR_CELL_STR(70, TEXT, ret);

  // err
  ret = OB_DATA_OUT_OF_RANGE;
  YEAR_CELL_STR(-1, TEXT, ret);

  //test func varchar_cell_str
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********VARCHAR_TEST***********"<<std::endl;
  VARCHAR_CELL_STR("", 0, ret);
  VARCHAR_CELL_STR("123", 3, ret);
  VARCHAR_CELL_STR("adafa", 5, ret);
  VARCHAR_CELL_STR("123abc", 6, ret);
  char src_buf[2048];
  memset(src_buf, 0, 2048);
  for (int i = 0 ; i < 2048 ; i++) {
    src_buf[i] = 'o';
  }
  // err
  ret = OB_SIZE_OVERFLOW;
  VARCHAR_CELL_STR(src_buf, 2048, ret);
  VARCHAR_CELL_STR(src_buf, 1124, ret);
  VARCHAR_CELL_STR(src_buf, 1024, ret);
  VARCHAR_CELL_STR(src_buf, 1022, ret);
  ret = OB_SUCCESS;
  VARCHAR_CELL_STR(src_buf, 1020, ret);

  // test func number_cell_str
  ret = OB_SUCCESS;
  of_result << std::endl;
  of_result << "***********NUMBER_TEST***********"<<std::endl;
  ObMalloc allocator;
  number::ObNumber nmb;
  nmb.from("123", allocator);
  scale = 0;
  NUMBER_CELL_STR(nmb, scale, true, 20, ret);
  scale = 10;
  NUMBER_CELL_STR(nmb, scale, true, 20, ret);
  NUMBER_CELL_STR(nmb, scale, false, 20, ret);
  scale = 20;
  NUMBER_CELL_STR(nmb, scale, true, 20, ret);
  number::ObNumber nmb1;
  nmb1.from("", allocator);
  scale = 0;
  NUMBER_CELL_STR(nmb1, scale, true, 20, ret);
  NUMBER_CELL_STR(nmb1, scale, false, 20, ret);
  scale = 10;
  NUMBER_CELL_STR(nmb1, scale, true, 20, ret);
  scale = 20;
  NUMBER_CELL_STR(nmb1, scale, true, 20, ret);
  number::ObNumber nmb2;
  nmb2.from("122323.23123123123123123131311", allocator);
  scale = 0;
  NUMBER_CELL_STR(nmb2, scale, true, 20, ret);
  NUMBER_CELL_STR(nmb2, scale, false, 20, ret);
  scale = 10;
  NUMBER_CELL_STR(nmb2, scale, true, 20, ret);
  scale = 20;
  NUMBER_CELL_STR(nmb2, scale, true, 20, ret);

  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
  LOG_INFO("buf", K(ObString(buf)));
}

static int make_json_binary(const std::string &text, ObIAllocator &allocator, ObString &raw)
{
  ObIJsonBase *json = nullptr;
  int ret = ObJsonBaseFactory::get_json_base(&allocator,
      ObString(static_cast<int32_t>(text.size()), text.data()),
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, json);
  if (OB_SUCC(ret)) {
    ret = json->get_raw_binary(raw, &allocator);
  }
  return ret;
}

TEST_F(TestObMySQLUtil, json_cell_bin_view_matches_jsonbase)
{
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  const std::string cases[] = {
    "null", "true", "false", "7", "-7", "18446744073709551615", "1.25",
    "\"\"", "\"hello\"", "\"a\\n\\\"b\\\\c\"", "\"中文\"", "[]", "{}",
    "{\"a\":[1,2,\"x\"],\"b\":false}", "[{},[],null,true,-128,256]",
    "\"" + std::string(70000, 'a') + "\""
  };
  for (const std::string &text : cases) {
    ObArenaAllocator allocator;
    ObString raw;
    ASSERT_EQ(OB_SUCCESS, make_json_binary(text, allocator, raw));
    ObJsonBin bin(raw.ptr(), raw.length(), &allocator);
    bin.set_seek_flag(true);
    ASSERT_EQ(OB_SUCCESS, bin.reset_iter());
    ObJsonBuffer printed(&allocator);
    ASSERT_EQ(OB_SUCCESS, bin.print(printed, true, raw.length()));
    std::string expected(printed.length() + 16, '#');
    int64_t expected_pos = 3;
    ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::store_length(&expected[0], expected.size(),
                                                   printed.length(), expected_pos));
    MEMCPY(&expected[expected_pos], printed.ptr(), printed.length());
    expected_pos += printed.length();
    for (bool enabled : {false, true}) {
      std::string packet(expected.size(), '#');
      int64_t pos = 3;
      ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
          &packet[0], packet.size(), raw, pos, enabled));
      EXPECT_EQ(expected_pos, pos);
      EXPECT_EQ(expected, packet);
    }
    std::string packet(expected.size(), '#');
    int64_t pos = 3;
    ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
        &packet[0], packet.size(), raw, pos));
    EXPECT_EQ(expected_pos, pos);
    EXPECT_EQ(expected, packet);
  }
}

TEST_F(TestObMySQLUtil, json_cell_bin_view_packet_limits_and_retry)
{
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  for (int length : {0, 248, 249, 65533, 65534}) {
    ObArenaAllocator allocator;
    ObString raw;
    ASSERT_EQ(OB_SUCCESS, make_json_binary("\"" + std::string(length, 'a') + "\"", allocator, raw));
    const int64_t payload_length = length + 2;
    const int64_t prefix_length = payload_length < 251 ? 1 : (payload_length < 65536 ? 3 : 4);
    const int64_t start = 4;
    const int64_t end = start + prefix_length + payload_length;
    for (bool enabled : {false, true}) {
      std::string packet(end + 1, '#');
      int64_t pos = start;
      EXPECT_EQ(OB_SIZE_OVERFLOW, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
          &packet[0], end - 1, raw, pos, enabled));
      EXPECT_EQ(start, pos);
      EXPECT_EQ(std::string(start, '#'), packet.substr(0, start));
      ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
          &packet[0], end, raw, pos, enabled));
      EXPECT_EQ(end, pos);
      EXPECT_EQ('"', packet[start + prefix_length]);
      EXPECT_EQ('"', packet[end - 1]);
      EXPECT_EQ('#', packet[end]);
      const unsigned char marker = static_cast<unsigned char>(packet[start]);
      EXPECT_EQ(prefix_length == 1 ? payload_length : (prefix_length == 3 ? 252 : 253), marker);
    }
  }
}

TEST_F(TestObMySQLUtil, json_cell_bin_view_empty_and_invalid_input)
{
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ObString empty;
  for (bool enabled : {false, true}) {
    char packet[16] = {};
    int64_t pos = 2;
    ASSERT_EQ(OB_SUCCESS, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
        packet, sizeof(packet), empty, pos, enabled));
    EXPECT_EQ(3, pos);
    EXPECT_EQ(0xfb, static_cast<unsigned char>(packet[2]));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
        nullptr, sizeof(packet), empty, pos, enabled));
    EXPECT_EQ(3, pos);
  }
  ObArenaAllocator allocator;
  ObString raw;
  ASSERT_EQ(OB_SUCCESS, make_json_binary("{\"k\":\"abc\"}", allocator, raw));
  ASSERT_GT(raw.length(), 2);
  ObString truncated(1, raw.ptr());
  char packet[128] = {};
  int64_t pos = 2;
  EXPECT_NE(OB_SUCCESS, ObMySQLUtil::json_cell_str(OB_SERVER_TENANT_ID,
      packet, sizeof(packet), truncated, pos, true));
  EXPECT_EQ(2, pos);
}

int main(int argc, char *argv[])
{
  system("rm -rf test_mysql_util.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_mysql_util.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
