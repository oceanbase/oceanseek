/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "share/vector_index/ob_ivf_cache_rewarm_task.h"

namespace oceanbase
{
namespace share
{

class FakeIdResult
{
public:
  FakeIdResult(const common::ObObj &namespace_id,
               const common::ObObj &ltable_id,
               const int get_ret = common::OB_SUCCESS)
      : namespace_id_(namespace_id), ltable_id_(ltable_id), get_ret_(get_ret)
  {}

  int get_obj(const char *column_name, common::ObObj &obj) const
  {
    int ret = get_ret_;
    if (common::OB_SUCCESS == ret) {
      if (0 == STRCMP(column_name, "namespace_id")) {
        obj = namespace_id_;
      } else if (0 == STRCMP(column_name, "ltable_id")) {
        obj = ltable_id_;
      } else {
        ret = common::OB_ERR_COLUMN_NOT_FOUND;
      }
    }
    return ret;
  }

private:
  common::ObObj namespace_id_;
  common::ObObj ltable_id_;
  int get_ret_;
};

static void check_value_for_both_columns(const common::ObObj &obj, const uint64_t expected)
{
  FakeIdResult result(obj, obj);
  const char *columns[] = {"namespace_id", "ltable_id"};
  for (int64_t i = 0; i < ARRAYSIZEOF(columns); ++i) {
    uint64_t value = 17;
    ASSERT_EQ(common::OB_SUCCESS,
              ObIvfCacheRewarmTask::read_compatible_integer_id(result, columns[i], value));
    EXPECT_EQ(expected, value);
  }
}

TEST(TestIvfCacheRewarmIdReader, valid_signed_and_unsigned_boundaries)
{
  common::ObObj obj;
  const int64_t signed_values[] = {0, 42, INT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(signed_values); ++i) {
    obj.set_int(signed_values[i]);
    check_value_for_both_columns(obj, static_cast<uint64_t>(signed_values[i]));
  }

  const uint64_t unsigned_values[] = {
      0, 42, static_cast<uint64_t>(INT64_MAX) + 1, UINT64_MAX};
  for (int64_t i = 0; i < ARRAYSIZEOF(unsigned_values); ++i) {
    obj.set_uint64(unsigned_values[i]);
    check_value_for_both_columns(obj, unsigned_values[i]);
  }
}

TEST(TestIvfCacheRewarmIdReader, rejects_invalid_values_without_changing_output)
{
  common::ObObj valid;
  valid.set_uint64(1);
  common::ObObj invalid_values[3];
  invalid_values[0].set_int(-1);
  invalid_values[1].set_null();
  invalid_values[2].set_varchar("not-an-id");
  const int expected_ret[] = {
      common::OB_INVALID_DATA, common::OB_OBJ_TYPE_ERROR, common::OB_OBJ_TYPE_ERROR};

  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_values); ++i) {
    FakeIdResult result(invalid_values[i], invalid_values[i]);
    uint64_t value = 12345;
    EXPECT_EQ(expected_ret[i], ObIvfCacheRewarmTask::read_compatible_integer_id(
                                   result, "namespace_id", value));
    EXPECT_EQ(12345, value);
    EXPECT_EQ(expected_ret[i], ObIvfCacheRewarmTask::read_compatible_integer_id(
                                   result, "ltable_id", value));
    EXPECT_EQ(12345, value);
  }

  FakeIdResult failed_result(valid, valid, common::OB_TIMEOUT);
  uint64_t value = 12345;
  EXPECT_EQ(common::OB_TIMEOUT, ObIvfCacheRewarmTask::read_compatible_integer_id(
                                    failed_result, "namespace_id", value));
  EXPECT_EQ(12345, value);
  EXPECT_EQ(common::OB_TIMEOUT, ObIvfCacheRewarmTask::read_compatible_integer_id(
                                    failed_result, "ltable_id", value));
  EXPECT_EQ(12345, value);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
