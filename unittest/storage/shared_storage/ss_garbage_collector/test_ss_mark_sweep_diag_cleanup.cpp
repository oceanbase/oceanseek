/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "common/ob_clock_generator.h"
#include "lib/atomic/ob_atomic.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#define private public
#define protected public
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_cleanup_task.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_diag_record_cleanup_task.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
class TestSSMarkSweepDiagCleanup : public ::testing::Test
{
public:
  TestSSMarkSweepDiagCleanup() {}
  virtual ~TestSSMarkSweepDiagCleanup() {}
};

TEST_F(TestSSMarkSweepDiagCleanup, diag_type_is_not_shared_data_task)
{
  const ObSharedGCTaskType type = ObSharedGCTaskType::MARK_SWEEP_GC_DIAG;
  ASSERT_TRUE(is_valid_shared_gc_task_type(type));
  ASSERT_TRUE(is_mark_sweep_gc_diag_type(type));
  ASSERT_FALSE(is_shared_data_gc_task_type(type));
  ASSERT_FALSE(is_mark_sweep_gc_type(type));
  ASSERT_STREQ("MARK_SWEEP_GC_DIAG", ob_shared_gc_task_type_to_string(type));
  ASSERT_EQ(type, ob_string_to_shared_gc_task_type(ObString::make_string("MARK_SWEEP_GC_DIAG")));
}

TEST_F(TestSSMarkSweepDiagCleanup, round_marker_needs_no_payload)
{
  ObSharedGCTask task;
  task.key_ = ObSharedGCTaskKey(1002, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, 100, 0);
  ASSERT_TRUE(task.value_.empty());
  ASSERT_TRUE(task.is_valid());
  task.value_ = ObString::make_string("");
  ASSERT_NE(nullptr, task.value_.ptr());
  ASSERT_TRUE(task.value_.empty());
  ASSERT_TRUE(task.is_valid());
  // Existing values are ignored; the key alone identifies the directory to reclaim.
  task.value_ = ObString::make_string("unused legacy value");
  ASSERT_TRUE(task.is_valid());
  task.key_.round_id_ = 0;
  ASSERT_FALSE(task.is_valid());
  task.key_.round_id_ = 100;
  task.key_.task_id_ = 1;
  ASSERT_FALSE(task.is_valid());
}

TEST_F(TestSSMarkSweepDiagCleanup, other_task_types_require_payload)
{
  ObSharedGCTask task;
  task.key_ = ObSharedGCTaskKey(1002, ObSharedGCTaskType::MARK_SWEEP_GC, 100, 0);
  ASSERT_FALSE(task.is_valid());
  task.value_ = ObString::make_string("");
  ASSERT_FALSE(task.is_valid());
  task.key_.type_ = ObSharedGCTaskType::FAIL_GC;
  ASSERT_FALSE(task.is_valid());
  task.key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT;
  ASSERT_FALSE(task.is_valid());
  task.value_ = ObString::make_string("1");
  ASSERT_TRUE(task.is_valid());
}

TEST_F(TestSSMarkSweepDiagCleanup, not_init_and_stop_do_not_access_storage)
{
  volatile bool is_stopped = false;
  ObSSMarkSweepDiagCleanupTask cleanup(is_stopped);
  ObSSMarkSweepDiagRecordCleanupTask record_cleanup(is_stopped);
  ObSharedGCTask task;
  ASSERT_EQ(OB_NOT_INIT, cleanup.cleanup_stale_rounds_by_dir_(101));
  ASSERT_EQ(OB_NOT_INIT, record_cleanup.cleanup_stale_rounds_by_task_(101));
  ASSERT_EQ(OB_SUCCESS, cleanup.init(1002));
  ASSERT_EQ(OB_SUCCESS, record_cleanup.init(1002));
  ATOMIC_STORE(&is_stopped, true);
  ASSERT_EQ(OB_SERVICE_STOPPED, cleanup.do_cleanup_());
  ASSERT_EQ(OB_SERVICE_STOPPED, cleanup.cleanup_stale_rounds_by_dir_(101));
  ASSERT_EQ(OB_SERVICE_STOPPED, record_cleanup.do_cleanup_());
  ASSERT_EQ(OB_SERVICE_STOPPED, record_cleanup.cleanup_stale_rounds_by_task_(101));
  ASSERT_EQ(OB_SERVICE_STOPPED, record_cleanup.cleanup_task_(task));
  ASSERT_EQ(OB_SERVICE_STOPPED, record_cleanup.delete_diag_dir_(100));
}

TEST_F(TestSSMarkSweepDiagCleanup, reject_task_outside_cleanup_scope)
{
  volatile bool is_stopped = false;
  ObSSMarkSweepDiagRecordCleanupTask cleanup(is_stopped);
  ObSharedGCTask task;
  ASSERT_EQ(OB_SUCCESS, cleanup.init(1002));
  task.key_ = ObSharedGCTaskKey(1002, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG,
      100, 0/*task_id*/);
  task.key_.tenant_id_ = 1004;
  ASSERT_EQ(OB_INVALID_ARGUMENT, cleanup.cleanup_task_(task));
  task.key_.tenant_id_ = 1002;
  task.key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC;
  ASSERT_EQ(OB_INVALID_ARGUMENT, cleanup.cleanup_task_(task));
  task.key_.type_ = ObSharedGCTaskType::MARK_SWEEP_GC_DIAG;
  task.key_.round_id_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, cleanup.cleanup_task_(task));
}

TEST_F(TestSSMarkSweepDiagCleanup, reject_invalid_directory_round)
{
  volatile bool is_stopped = false;
  ObSSMarkSweepDiagRecordCleanupTask cleanup(is_stopped);
  ASSERT_EQ(OB_NOT_INIT, cleanup.delete_diag_dir_(100));
  ASSERT_EQ(OB_SUCCESS, cleanup.init(1002));
  ASSERT_EQ(OB_INVALID_ARGUMENT, cleanup.delete_diag_dir_(0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, cleanup.delete_diag_dir_(-1));
}

TEST_F(TestSSMarkSweepDiagCleanup, cleanup_intervals_are_fixed)
{
  volatile bool is_stopped = false;
  ObSSMarkSweepDiagCleanupTask cleanup(is_stopped);
  ObSSMarkSweepDiagRecordCleanupTask record_cleanup(is_stopped);
  int64_t interval_us = 0;
  cleanup.get_gc_exec_interval_us_(interval_us);
  ASSERT_EQ(7LL * 24LL * 60LL * 60LL * 1000LL * 1000LL,
      interval_us);
  record_cleanup.get_gc_exec_interval_us_(interval_us);
  ASSERT_EQ(1LL * 60LL * 60LL * 1000LL * 1000LL,
      interval_us);
}
} // namespace unittest
} // namespace oceanbase

int main(
    int argc,
    char **argv)
{
  int ret = oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  if (oceanbase::common::OB_SUCCESS == ret) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
