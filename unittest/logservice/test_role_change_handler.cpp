/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#define private public
#include "logservice/rcservice/ob_role_change_handler.h"
#undef private
#include "logservice/ob_garbage_collector.h"

namespace oceanbase
{
using namespace logservice;
#ifdef OB_BUILD_SHARED_STORAGE
namespace logservice
{
class MockSSWriterRoleChangeHandler : public ObIRoleChangeSubHandler
{
public:
  MockSSWriterRoleChangeHandler() : switch_to_leader_called_(false) {}
  void switch_to_follower_forcedly() override final {}
  int switch_to_leader() override final
  {
    switch_to_leader_called_.store(true, std::memory_order_release);
    return OB_SUCCESS;
  }
  int switch_to_follower_gracefully() override final { return OB_SUCCESS; }
  int resume_leader() override final { return OB_SUCCESS; }

  std::atomic<bool> switch_to_leader_called_;
};

TEST(TestGCHandler, switch_to_leader_does_not_block_sswriter_takeover)
{
  ObGCHandler gc_handler;
  MockSSWriterRoleChangeHandler sswriter_handler;
  ObRoleChangeHandler role_change_handler;
  RCDiagnoseInfo diagnose_info;
  std::atomic<bool> role_change_finished(false);
  int switch_ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS,
            role_change_handler.register_handler(ObLogBaseType::GC_LS_LOG_BASE_TYPE,
                                                 &gc_handler));
  ASSERT_EQ(OB_SUCCESS,
            role_change_handler.register_handler(ObLogBaseType::SHARED_STORAGE_SSWRITER_LOG_BASE_TYPE,
                                                 &sswriter_handler));
  ASSERT_TRUE(gc_handler.rwlock_.try_wrlock());

  std::thread takeover_thread([&]() {
    switch_ret = role_change_handler.switch_to_leader(diagnose_info);
    role_change_finished.store(true, std::memory_order_release);
  });
  for (int64_t i = 0;
       i < 1000 && !role_change_finished.load(std::memory_order_acquire);
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool finished_while_gc_locked =
      role_change_finished.load(std::memory_order_acquire);

  gc_handler.rwlock_.unlock();
  takeover_thread.join();

  EXPECT_TRUE(finished_while_gc_locked);
  EXPECT_EQ(OB_SUCCESS, switch_ret);
  EXPECT_TRUE(sswriter_handler.switch_to_leader_called_.load(std::memory_order_acquire));
}

TEST(TestGCHandler, follower_switch_still_waits_for_gc)
{
  ObGCHandler gc_handler;
  std::atomic<bool> follower_switch_started(false);
  std::atomic<bool> follower_switch_finished(false);
  int switch_ret = OB_SUCCESS;

  ASSERT_TRUE(gc_handler.rwlock_.try_wrlock());
  std::thread follower_thread([&]() {
    follower_switch_started.store(true, std::memory_order_release);
    switch_ret = gc_handler.switch_to_follower_gracefully();
    follower_switch_finished.store(true, std::memory_order_release);
  });
  while (!follower_switch_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const bool finished_while_gc_locked =
      follower_switch_finished.load(std::memory_order_acquire);

  gc_handler.rwlock_.unlock();
  follower_thread.join();

  EXPECT_FALSE(finished_while_gc_locked);
  EXPECT_EQ(OB_SUCCESS, switch_ret);
  EXPECT_TRUE(follower_switch_finished.load(std::memory_order_acquire));
}
} // end namespace logservice
#endif
namespace unittest
{
class MockRoleChangeHandler : public ObIRoleChangeSubHandler
{
public:
  void switch_to_follower_forcedly() override final
  {}
  int switch_to_leader() override final
  {
    return OB_SUCCESS;
  }
  int switch_to_follower_gracefully() override final
  {
    return OB_SUCCESS;
  }
  int resume_leader() override final
  {
    return OB_SUCCESS;
  }
};
TEST(TestRoleChangeHander, test_basic_func)
{
  ObRoleChangeHandler handler;
  ObLogBaseType type = ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE;
  MockRoleChangeHandler mock_handler;
  RCDiagnoseInfo unused_diagnose_info;
  handler.register_handler(type, &mock_handler);
  handler.switch_to_leader(unused_diagnose_info);
  handler.switch_to_follower_forcedly();
  handler.switch_to_follower_gracefully();
}
} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_role_change_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_role_change_handler");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
