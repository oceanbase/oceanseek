/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX LIB

#include <gtest/gtest.h>

#include "lib/string/ob_sensitive_string.h"

namespace oceanbase
{
namespace common
{
static void check_sensitive_string(const char *input, const char *expected)
{
  char buf[1024] = {0};
  const int64_t pos = ObSensitiveString(input).to_string(buf, sizeof(buf));
  ASSERT_EQ(STRLEN(expected), pos);
  ASSERT_STREQ(expected, buf);
}

static void check_sensitive_string_no_leak(const char *input, const int64_t buf_len, const char *secret)
{
  char buf[256] = {0};
  ASSERT_LE(buf_len, static_cast<int64_t>(sizeof(buf)));
  (void)ObSensitiveString(input).to_string(buf, buf_len);
  ASSERT_EQ(nullptr, STRSTR(buf, secret));
}

TEST(TestSensitiveString, mask_access_key)
{
  check_sensitive_string(
      "s3://bucket/path?host=http://example.invalid&access_id=id_value&access_key=key_value",
      "s3://bucket/path?host=http://example.invalid&access_id=id_value&access_key={hidden_access_key}");
  check_sensitive_string(
      "s3://bucket/path?access_key=first&foo=bar&ACCESS_KEY=second&access_key_extra=keep",
      "s3://bucket/path?access_key={hidden_access_key}&foo=bar&access_key={hidden_access_key}"
      "&access_key={hidden_access_key}");
  check_sensitive_string(
      "s3://bucket/path?access_key=&access_id=id_value",
      "s3://bucket/path?access_key={hidden_access_key}&access_id=id_value");
  check_sensitive_string(
      "access_id=id_value&access_key=key_value?marker",
      "access_id=id_value&access_key={hidden_access_key}");
}

TEST(TestSensitiveString, keep_path_access_key)
{
  check_sensitive_string(
      "s3://bucket/access_key_probe?host=http://example.invalid&access_id=id_value&access_key=key_value",
      "s3://bucket/access_key_probe?host=http://example.invalid&access_id=id_value&access_key={hidden_access_key}");
  check_sensitive_string(
      "s3://bucket/path?note=access_key_probe&access_id=id_value&access_key=key_value",
      "s3://bucket/path?note=access_key={hidden_access_key}&access_id=id_value&access_key={hidden_access_key}");
  check_sensitive_string(
      "s3://bucket/access_key=path_value?host=http://example.invalid&access_key=key_value",
      "s3://bucket/access_key=path_value?host=http://example.invalid&access_key={hidden_access_key}");
  check_sensitive_string("s3://bucket/access_key_probe", "s3://bucket/access_key={hidden_access_key}");
}

TEST(TestSensitiveString, mask_semicolon_in_access_key)
{
  const char *url = "s3://bucket/path?host=example.invalid&access_id=id_value&access_key=prefix;SYNTHETIC_SECRET";
  check_sensitive_string(
      url,
      "s3://bucket/path?host=example.invalid&access_id=id_value&access_key={hidden_access_key}");
  check_sensitive_string_no_leak(url, 256, "SYNTHETIC_SECRET");
  check_sensitive_string(
      "access_id=id_value&access_key=prefix;SYNTHETIC_SECRET&access_id=second",
      "access_id=id_value&access_key={hidden_access_key}&access_id=second");
}

TEST(TestSensitiveString, mask_structured_string)
{
  check_sensitive_string(
      "{path:s3://bucket?host=example.invalid,value:access_key=key_value&access_id=id_value}",
      "{path:s3://bucket?host=example.invalid,value:access_key={hidden_access_key}&access_id=id_value}");
  check_sensitive_string(
      "s3://bucket/path?host=example.invalid&\raccess_key=first;\nACCESS_KEY=second",
      "s3://bucket/path?host=example.invalid&\raccess_key={hidden_access_key}");
  check_sensitive_string("1://access_key=key_value?marker=value", "1://access_key={hidden_access_key}");
  check_sensitive_string("+://access_key=key_value?marker=value", "+://access_key={hidden_access_key}");
  check_sensitive_string(
      "a1+.-://bucket/access_key=path_value?access_key=key_value",
      "a1+.-://bucket/access_key=path_value?access_key={hidden_access_key}");
}

TEST(TestSensitiveString, truncated_buffer)
{
  const char *input = "s3://bucket/path?host=example.invalid&access_key=PLAIN_SECRET_VALUE&access_id=id_value";
  check_sensitive_string_no_leak(input, 1, "PLAIN");
  check_sensitive_string_no_leak(input, 32, "PLAIN");
  check_sensitive_string_no_leak(input, 64, "PLAIN");
  check_sensitive_string_no_leak(input, 96, "PLAIN");
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
