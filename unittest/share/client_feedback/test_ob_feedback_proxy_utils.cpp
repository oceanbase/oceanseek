/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define UNITTEST_DEBUG
#include <atomic>
#include <thread>
#define private public
#include "observer/mysql/ob_feedback_proxy_utils.h"
#undef private
#include "src/sql/session/ob_sql_session_info.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{

using namespace observer;
using namespace common;
using namespace std;

bool verifyHexString(const char *data, const size_t data_size, const std::string &hex_string)
{
  // data length mismatch
  if (data_size * 2 != hex_string.size()) {
    return false;
  }

  std::ostringstream stream;
  for (size_t i = 0; i < data_size; ++i) {
    stream << std::hex << std::setfill('0') << std::setw(2) << (0xFF & static_cast<unsigned char>(data[i]));
  }

  return stream.str() == hex_string;
}

TEST(ObFeedbackProxyInfo, test_serialize)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  bool verified = false;

  ObIsLockSessionInfo is_lock_session(ObFeedbackProxyInfoType::IS_LOCK_SESSION, '1');
  ret = is_lock_session.serialize(buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 7);

  // result in hex
  std::string hex_string = "00000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, test_serialize_from_session)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  session.set_is_lock_session(true);
  ret = ObFeedbackProxyUtils::serialize_(session, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 7);

  // result in hex
  std::string hex_string = "00000100000031";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}

TEST(ObFeedbackProxyUtils, concurrent_session_feedback_isolation)
{
  const int64_t THREAD_COUNT = 8;
  const int64_t ITERATION_COUNT = 100000;
  const int64_t FEEDBACK_LENGTH = 7;
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start(false);
  std::thread threads[THREAD_COUNT];
  int64_t serialize_errors[THREAD_COUNT] = {};
  int64_t lock_mismatches[THREAD_COUNT] = {};
  int64_t payload_mismatches[THREAD_COUNT] = {};
  ObLogger &logger = ObLogger::get_logger();
  const int32_t old_log_level = logger.get_log_level();
  logger.set_log_level(OB_LOG_LEVEL_WARN);

  for (int64_t thread_id = 0; thread_id < THREAD_COUNT; ++thread_id) {
    threads[thread_id] = std::thread([&, thread_id]() {
      // Each worker owns its session and output buffer. Only the implementation
      // under test can introduce shared mutable feedback state.
      sql::ObSQLSessionInfo session;
      const bool is_lock_session = (thread_id % 2) != 0;
      session.set_is_lock_session(is_lock_session);
      const char expected[FEEDBACK_LENGTH] = {
          0, 0, 1, 0, 0, 0, is_lock_session ? '1' : '0'};
      ready_count.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int64_t iteration = 0; iteration < ITERATION_COUNT; ++iteration) {
        char buf[FEEDBACK_LENGTH] = {};
        int64_t pos = 0;
        const int64_t size = ObFeedbackProxyUtils::get_serialize_size_(session);
        const int ret = ObFeedbackProxyUtils::serialize_(session, buf, sizeof(buf), pos);
        if (OB_SUCCESS != ret || FEEDBACK_LENGTH != pos || FEEDBACK_LENGTH != size) {
          ++serialize_errors[thread_id];
        } else {
          if (buf[6] != expected[6]) {
            ++lock_mismatches[thread_id];
          }
          if (0 != MEMCMP(buf, expected, sizeof(expected))) {
            ++payload_mismatches[thread_id];
          }
        }
      }
    });
  }
  while (THREAD_COUNT != ready_count.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  for (int64_t thread_id = 0; thread_id < THREAD_COUNT; ++thread_id) {
    threads[thread_id].join();
  }
  logger.set_log_level(old_log_level);
  for (int64_t thread_id = 0; thread_id < THREAD_COUNT; ++thread_id) {
    EXPECT_EQ(0, serialize_errors[thread_id]) << "thread_id=" << thread_id;
    EXPECT_EQ(0, lock_mismatches[thread_id]) << "thread_id=" << thread_id;
    EXPECT_EQ(0, payload_mismatches[thread_id]) << "thread_id=" << thread_id;
  }
}

TEST(ObFeedbackProxyUtils, test_serialize_from_kv)
{
  INIT_SUCC(ret);
  const int64_t LEN = 100;
  char buf[LEN];
  int64_t pos = 0;
  sql::ObSQLSessionInfo session;
  bool verified = false;

  ObSEArray<short, 3> test_array;
  test_array.push_back(1);
  test_array.push_back(2);
  test_array.push_back(3);
  ret = ObFeedbackProxyUtils::serialize_(ObFeedbackProxyInfoType::IS_LOCK_SESSION, test_array, buf, LEN, pos);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(pos, 10);

  // result in hex
  std::string hex_string = "00000400000003010203";
  verified = verifyHexString(buf, pos, hex_string);
  ASSERT_TRUE(verified);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_feedback_proxy_utils.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_feedback_proxy_utils.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
