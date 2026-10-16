/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "common/ob_clock_generator.h"
#include "lib/time/ob_time_utility.h"
#define private public
#define protected public
#include "logservice/replayservice/ob_log_replay_service.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;

TEST(TestObReplayErrorInfo, RecordsFirstErrorAndClearsFailureDuration)
{
  // Exercise the real caller without starting a replay scheduler or creating a palf handle.
  ObLogReplayService service;
  ObReplayStatus status;
  ObReplayServiceReplayTask task_queue;
  ObLogReplayTask replay_task;

  service.process_replay_ret_code_(OB_SUCCESS, status, task_queue, replay_task);
  EXPECT_EQ(OB_SUCCESS, task_queue.get_err_info_ret_code());
  EXPECT_EQ(0, task_queue.err_info_.fail_ts_);
  EXPECT_EQ(0, task_queue.err_info_.fail_cost_);

  const int64_t before = ObTimeUtility::fast_current_time();
  service.process_replay_ret_code_(OB_EAGAIN, status, task_queue, replay_task);
  const int64_t after = ObTimeUtility::fast_current_time();
  ASSERT_EQ(OB_EAGAIN, task_queue.get_err_info_ret_code());
  const int64_t first_fail_ts = task_queue.err_info_.fail_ts_;
  ASSERT_GT(first_fail_ts, 0);
  EXPECT_GE(first_fail_ts, before);
  EXPECT_LE(first_fail_ts, after);
  EXPECT_FALSE(task_queue.need_replay_immediately());

  // Use a deterministic recovery time to verify the failure duration without sleeping.
  const int64_t fail_cost = 12345;
  task_queue.clear_err_info(first_fail_ts + fail_cost);
  EXPECT_EQ(OB_SUCCESS, task_queue.get_err_info_ret_code());
  EXPECT_EQ(0, task_queue.err_info_.fail_ts_);
  EXPECT_EQ(fail_cost, task_queue.err_info_.fail_cost_);
  EXPECT_TRUE(task_queue.need_replay_immediately());
  task_queue.clear_err_info(first_fail_ts + 2 * fail_cost);
  EXPECT_EQ(fail_cost, task_queue.err_info_.fail_cost_);
}

TEST(TestObReplayErrorInfo, RepeatedErrorsPreserveFirstFailureTime)
{
  ObLogReplayService service;
  ObReplayStatus status;
  ObReplayServiceReplayTask task_queue;
  ObLogReplayTask replay_task;
  // A fixed earlier timestamp detects accidental updates even within the same clock tick.
  const int64_t first_fail_ts = 123456789;
  task_queue.set_simple_err_info(OB_EAGAIN, first_fail_ts);
  for (const int ret_code : {OB_EAGAIN, OB_ALLOCATE_MEMORY_FAILED, OB_IO_ERROR}) {
    service.process_replay_ret_code_(ret_code, status, task_queue, replay_task);
    EXPECT_EQ(ret_code, task_queue.get_err_info_ret_code());
    EXPECT_EQ(first_fail_ts, task_queue.err_info_.fail_ts_);
    EXPECT_EQ(0, task_queue.err_info_.fail_cost_);
    EXPECT_FALSE(task_queue.has_fatal_error());
    EXPECT_FALSE(status.has_fatal_error());
  }
}
}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObClockGenerator::init();
  OB_LOGGER.set_file_name("test_ob_replay_error_info.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
