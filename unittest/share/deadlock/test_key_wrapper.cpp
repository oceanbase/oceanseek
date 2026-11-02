/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "src/share/deadlock/ob_deadlock_detector_common_define.h"
#include "share/deadlock/test/test_key.h"

#include <gmock/gmock.h>

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace std;


class TestUserBinaryKey : public ::testing::Test {
public:
  TestUserBinaryKey() {}
  ~TestUserBinaryKey() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestUserBinaryKey, transaction_key_uses_typed_detector_key)
{
  using namespace transaction;
  const ObTransID tx_id(123);
  UserBinaryKey implicit_key;
  UserBinaryKey default_key;
  UserBinaryKey remote_key;
  ASSERT_EQ(OB_SUCCESS, implicit_key.set_user_key(tx_id));
  ASSERT_EQ(OB_SUCCESS, default_key.set_user_key(
      ObTransDeadlockDetectorKey(ObDeadlockKeyType::DEFAULT, tx_id)));
  ASSERT_EQ(OB_SUCCESS, remote_key.set_user_key(
      ObTransDeadlockDetectorKey(ObDeadlockKeyType::REMOTE_EXEC_SIDE, tx_id)));
  EXPECT_TRUE(implicit_key == default_key);
  EXPECT_FALSE(implicit_key == remote_key);
  char buffer[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, implicit_key.serialize(buffer, sizeof(buffer), pos));
  UserBinaryKey decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  EXPECT_TRUE(decoded == default_key);
}

// 序列化和反序列化的功能
TEST_F(TestUserBinaryKey, serialization) {
  UserBinaryKey key1;
  key1.set_user_key(ObDeadLockTestIntKey(1));
  const int64_t length = 1024;
  int64_t pos = 0;
  char* buffer = new char[key1.get_serialize_size()];
  ASSERT_EQ(OB_SUCCESS, key1.serialize(buffer, length, pos));
  UserBinaryKey key2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, key2.deserialize(buffer, length, pos));
  ASSERT_EQ(true, key1 == key2);
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_key_wrapper.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_key_wrapper.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}