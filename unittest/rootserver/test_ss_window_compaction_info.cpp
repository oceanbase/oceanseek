/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "gtest/gtest.h"

#define private public
#include "rootserver/freeze/ob_major_merge_info_manager.h"
#include "rootserver/freeze/ob_major_merge_scheduler.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#undef private

#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "lib/stat/ob_session_stat.h"
#include "share/config/ob_server_config.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestSSWindowCompactionInfo : public ::testing::Test
{
public:
  TestSSWindowCompactionInfo() {}
  virtual ~TestSSWindowCompactionInfo() {}
  virtual void SetUp();
  virtual void TearDown();

protected:
  static const uint64_t DEFAULT_TENANT_ID = 1001;
  static const int64_t EXPECTED_EPOCH = 7;
  static const int64_t BASE_SCN_VAL = 100;
  static const int64_t TARGET_SCN_VAL = 200;
  static const int64_t WINDOW_START_TIME_US = 123456789;

  SCN make_scn(const uint64_t val);
  ObWindowParameters make_window_params(const int64_t duration_us);
  void build_global_merge_info(
      const uint64_t frozen_scn_val,
      const uint64_t global_broadcast_scn_val,
      const uint64_t last_merged_scn_val,
      const ObZoneMergeInfo::MergeStatus merge_status,
      const ObGlobalMergeInfo::MergeMode merge_mode,
      ObGlobalMergeInfo &global_merge_info);
  void set_zone_mgr_memory_global_info(
      const uint64_t frozen_scn_val,
      const uint64_t global_broadcast_scn_val,
      const uint64_t last_merged_scn_val,
      const ObZoneMergeInfo::MergeStatus merge_status,
      const ObGlobalMergeInfo::MergeMode merge_mode);

protected:
  ObMajorMergeInfoManager merge_info_mgr_;
  ObZoneMergeManager zone_merge_mgr_;
  ObServerMode origin_startup_mode_;
  bool origin_enable_major_freeze_;
};

const uint64_t TestSSWindowCompactionInfo::DEFAULT_TENANT_ID;
const int64_t TestSSWindowCompactionInfo::EXPECTED_EPOCH;
const int64_t TestSSWindowCompactionInfo::BASE_SCN_VAL;
const int64_t TestSSWindowCompactionInfo::TARGET_SCN_VAL;
const int64_t TestSSWindowCompactionInfo::WINDOW_START_TIME_US;

SCN TestSSWindowCompactionInfo::make_scn(const uint64_t val)
{
  SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_inner_table_field(val));
  return scn;
}

ObWindowParameters TestSSWindowCompactionInfo::make_window_params(const int64_t duration_us)
{
  ObWindowParameters params;
  params.is_inited_ = true;
  params.tenant_id_ = DEFAULT_TENANT_ID;
  params.enable_window_compaction_ = true;
  params.window_duration_us_ = duration_us;
  params.window_start_time_us_ = WINDOW_START_TIME_US;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_WINDOW;
  return params;
}

void TestSSWindowCompactionInfo::build_global_merge_info(
    const uint64_t frozen_scn_val,
    const uint64_t global_broadcast_scn_val,
    const uint64_t last_merged_scn_val,
    const ObZoneMergeInfo::MergeStatus merge_status,
    const ObGlobalMergeInfo::MergeMode merge_mode,
    ObGlobalMergeInfo &global_merge_info)
{
  global_merge_info.reset();
  global_merge_info.tenant_id_ = DEFAULT_TENANT_ID;
  global_merge_info.frozen_scn_.set_scn(make_scn(frozen_scn_val), false);
  global_merge_info.global_broadcast_scn_.set_scn(make_scn(global_broadcast_scn_val), false);
  global_merge_info.last_merged_scn_.set_scn(make_scn(last_merged_scn_val), false);
  global_merge_info.merge_status_.set_val(merge_status, false);
  global_merge_info.merge_mode_.set_val(merge_mode, false);
}

void TestSSWindowCompactionInfo::set_zone_mgr_memory_global_info(
    const uint64_t frozen_scn_val,
    const uint64_t global_broadcast_scn_val,
    const uint64_t last_merged_scn_val,
    const ObZoneMergeInfo::MergeStatus merge_status,
    const ObGlobalMergeInfo::MergeMode merge_mode)
{
  zone_merge_mgr_.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_mgr_.is_inited_ = true;
  zone_merge_mgr_.is_loaded_ = true;
  zone_merge_mgr_.shadow_.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_mgr_.shadow_.is_inited_ = true;
  zone_merge_mgr_.shadow_.is_loaded_ = true;
  build_global_merge_info(
      frozen_scn_val,
      global_broadcast_scn_val,
      last_merged_scn_val,
      merge_status,
      merge_mode,
      zone_merge_mgr_.global_merge_info_);
}

void TestSSWindowCompactionInfo::SetUp()
{
  origin_startup_mode_ = GCTX.startup_mode_;
  origin_enable_major_freeze_ = GCONF.enable_major_freeze;
#ifndef OB_BUILD_SHARED_STORAGE
  GTEST_SKIP() << "shared storage mode is not enabled in this build";
#endif
  GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
  GCONF.enable_major_freeze = true;
  merge_info_mgr_.tenant_id_ = DEFAULT_TENANT_ID;
}

void TestSSWindowCompactionInfo::TearDown()
{
  GCTX.startup_mode_ = origin_startup_mode_;
  GCONF.enable_major_freeze = origin_enable_major_freeze_;
}

TEST_F(TestSSWindowCompactionInfo, check_can_set_ss_window_freeze_info_matrix)
{
  struct TestCase
  {
    uint64_t frozen_scn_val_;
    uint64_t global_broadcast_scn_val_;
    uint64_t last_merged_scn_val_;
    ObZoneMergeInfo::MergeStatus merge_status_;
    ObGlobalMergeInfo::MergeMode merge_mode_;
    bool enable_major_freeze_;
    int expected_ret_;
  };
  const TestCase test_cases[] = {
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        true, OB_SUCCESS},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        true, OB_SUCCESS},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        true, OB_MAJOR_FREEZE_NOT_FINISHED},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL - 1,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        true, OB_MAJOR_FREEZE_NOT_FINISHED},
    {TARGET_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        true, OB_MAJOR_FREEZE_NOT_FINISHED},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        false, OB_MAJOR_FREEZE_NOT_ALLOW},
  };
  ObGlobalMergeInfo global_merge_info;

  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(test_cases) / sizeof(test_cases[0])); ++i) {
    const TestCase &test_case = test_cases[i];
    SCOPED_TRACE(i);
    GCONF.enable_major_freeze = test_case.enable_major_freeze_;
    build_global_merge_info(
        test_case.frozen_scn_val_,
        test_case.global_broadcast_scn_val_,
        test_case.last_merged_scn_val_,
        test_case.merge_status_,
        test_case.merge_mode_,
        global_merge_info);
    EXPECT_EQ(test_case.expected_ret_, merge_info_mgr_.check_can_set_ss_window_freeze_info_(global_merge_info));
  }

  GCONF.enable_major_freeze = true;
  build_global_merge_info(
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_TENANT,
      global_merge_info);
  const ObWindowCompactionParam invalid_param(true /*with_start_ts*/, 0);
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge_info_mgr_.set_ss_window_freeze_info(invalid_param));

  ObGlobalMergeInfo invalid_global_merge_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge_info_mgr_.check_can_set_ss_window_freeze_info_(invalid_global_merge_info));

  build_global_merge_info(
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_TENANT,
      global_merge_info);
  global_merge_info.suspend_merging_.set_val(1, false);
  EXPECT_EQ(OB_MAJOR_FREEZE_NOT_ALLOW, merge_info_mgr_.check_can_set_ss_window_freeze_info_(global_merge_info));
}

TEST_F(TestSSWindowCompactionInfo, inner_set_ss_window_freeze_info_argument_check)
{
  ObFreezeInfo freeze_info;
  ObGlobalMergeInfo invalid_global_merge_info;
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge_info_mgr_.inner_set_ss_window_freeze_info_(
      WINDOW_START_TIME_US, 1, invalid_global_merge_info, freeze_info));

  ObGlobalMergeInfo global_merge_info;
  build_global_merge_info(
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_TENANT,
      global_merge_info);
  auto *origin_schema_service = GCTX.schema_service_;
  GCTX.schema_service_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, merge_info_mgr_.inner_set_ss_window_freeze_info_(
      WINDOW_START_TIME_US, 1, global_merge_info, freeze_info));
  GCTX.schema_service_ = origin_schema_service;
}

TEST_F(TestSSWindowCompactionInfo, check_need_broadcast_matrix)
{
  struct TestCase
  {
    uint64_t frozen_scn_val_;
    uint64_t global_broadcast_scn_val_;
    uint64_t last_merged_scn_val_;
    ObZoneMergeInfo::MergeStatus merge_status_;
    ObGlobalMergeInfo::MergeMode merge_mode_;
    uint64_t pending_frozen_scn_val_;
    bool enable_major_freeze_;
    bool expected_need_broadcast_;
  };
  const TestCase test_cases[] = {
    // Test the interface directly: WINDOW mode does not block a newer SCN.
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL + 1, true, true},
    // The active window target does not need to be broadcast again.
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, true, false},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, true, true},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        TARGET_SCN_VAL, true, true},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, true, false},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        TARGET_SCN_VAL, false, false},
  };

  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(test_cases) / sizeof(test_cases[0])); ++i) {
    const TestCase &test_case = test_cases[i];
    bool need_broadcast = false;
    SCOPED_TRACE(i);
    GCONF.enable_major_freeze = test_case.enable_major_freeze_;
    set_zone_mgr_memory_global_info(
        test_case.frozen_scn_val_,
        test_case.global_broadcast_scn_val_,
        test_case.last_merged_scn_val_,
        test_case.merge_status_,
        test_case.merge_mode_);
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.check_need_broadcast(
        make_scn(test_case.pending_frozen_scn_val_), need_broadcast));
    EXPECT_EQ(test_case.expected_need_broadcast_, need_broadcast);
  }

  bool need_broadcast = false;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      zone_merge_mgr_.check_need_broadcast(SCN::invalid_scn(), need_broadcast));
}

TEST_F(TestSSWindowCompactionInfo, check_window_compaction_task_matrix)
{
  struct TestCase
  {
    uint64_t frozen_scn_val_;
    uint64_t global_broadcast_scn_val_;
    uint64_t last_merged_scn_val_;
    ObZoneMergeInfo::MergeStatus merge_status_;
    ObGlobalMergeInfo::MergeMode merge_mode_;
    int64_t request_start_time_us_;
    int expected_ret_;
    bool expected_need_update_;
  };
  const int64_t window_duration_us = 100;
  const TestCase test_cases[] = {
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US - 1, OB_MAJOR_FREEZE_NOT_FINISHED, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US + window_duration_us - 1, OB_MAJOR_FREEZE_NOT_FINISHED, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, TARGET_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US + window_duration_us - 1, OB_MAJOR_FREEZE_NOT_FINISHED, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, TARGET_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US, OB_MAJOR_FREEZE_NOT_FINISHED, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, TARGET_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US + window_duration_us, OB_SUCCESS, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US + window_duration_us, OB_SUCCESS, true},
    {BASE_SCN_VAL, BASE_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        WINDOW_START_TIME_US + window_duration_us, OB_MAJOR_FREEZE_NOT_FINISHED, false},
  };

  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(test_cases) / sizeof(test_cases[0])); ++i) {
    const TestCase &test_case = test_cases[i];
    bool need_update = false;
    ObGlobalMergeInfo global_info;
    SCOPED_TRACE(i);
    build_global_merge_info(
        test_case.frozen_scn_val_,
        test_case.global_broadcast_scn_val_,
        test_case.last_merged_scn_val_,
        test_case.merge_status_,
        test_case.merge_mode_,
        global_info);
    global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);
    EXPECT_EQ(test_case.expected_ret_, merge_info_mgr_.check_window_compaction_task(
        make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, test_case.request_start_time_us_,
        make_window_params(window_duration_us), global_info, need_update));
    EXPECT_EQ(test_case.expected_need_update_, need_update);
    EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
  }
}

TEST_F(TestSSWindowCompactionInfo, check_window_compaction_task_invalid_argument)
{
  const int64_t window_duration_us = 100;
  ObGlobalMergeInfo global_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_info);
  global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);

  const auto check_invalid_argument = [this](
      const SCN &target_scn,
      const int64_t task_start_time_us,
      const int64_t request_start_time_us,
      const int64_t test_window_duration_us,
      const ObGlobalMergeInfo &test_global_info) {
    bool need_update = true;
    EXPECT_EQ(OB_INVALID_ARGUMENT, merge_info_mgr_.check_window_compaction_task(
        target_scn, task_start_time_us, request_start_time_us,
        make_window_params(test_window_duration_us), test_global_info, need_update));
    EXPECT_FALSE(need_update);
  };

  ObGlobalMergeInfo invalid_global_info;
  check_invalid_argument(SCN::invalid_scn(), WINDOW_START_TIME_US,
      WINDOW_START_TIME_US + window_duration_us, window_duration_us, global_info);
  check_invalid_argument(make_scn(TARGET_SCN_VAL), 0,
      WINDOW_START_TIME_US + window_duration_us, window_duration_us, global_info);
  check_invalid_argument(make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US,
      0, window_duration_us, global_info);
  check_invalid_argument(make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US,
      WINDOW_START_TIME_US + window_duration_us, -1, global_info);
  check_invalid_argument(make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US,
      WINDOW_START_TIME_US + window_duration_us, window_duration_us, invalid_global_info);
  ObWindowParameters invalid_window_params = make_window_params(window_duration_us);
  invalid_window_params.is_inited_ = false;
  bool need_update = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge_info_mgr_.check_window_compaction_task(
      make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, WINDOW_START_TIME_US + window_duration_us,
      invalid_window_params, global_info, need_update));
  EXPECT_FALSE(need_update);
  EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
}

TEST_F(TestSSWindowCompactionInfo, check_window_compaction_task_base_scn_is_complete)
{
  const int64_t window_duration_us = 100;
  bool need_update = true;
  ObGlobalMergeInfo global_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_info);
  global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);

  // base_scn is valid and precedes last_merged_scn, so the task is already complete.
  EXPECT_EQ(OB_SUCCESS, merge_info_mgr_.check_window_compaction_task(
      SCN::base_scn(), WINDOW_START_TIME_US, WINDOW_START_TIME_US + window_duration_us,
      make_window_params(window_duration_us), global_info, need_update));
  EXPECT_FALSE(need_update);
  EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
}

TEST_F(TestSSWindowCompactionInfo, check_window_compaction_task_allows_cross_window_retry_with_merge_error)
{
  const int64_t window_duration_us = 100;
  bool need_update = false;
  ObGlobalMergeInfo global_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_info);
  global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);
  global_info.is_merge_error_.set_val(1, false);
  global_info.error_type_.set_val(ObZoneMergeInfo::CHECKSUM_ERROR, false);

  EXPECT_EQ(OB_SUCCESS, merge_info_mgr_.check_window_compaction_task(
      make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, WINDOW_START_TIME_US + window_duration_us,
      make_window_params(window_duration_us), global_info, need_update));
  EXPECT_TRUE(need_update);
  EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
}

TEST_F(TestSSWindowCompactionInfo, reject_disabled_or_suspended_ss_window_retry)
{
  const int64_t window_duration_us = 100;
  ObGlobalMergeInfo global_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_info);
  global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);

  bool need_update = true;
  global_info.suspend_merging_.set_val(1, false);
  EXPECT_EQ(OB_MAJOR_FREEZE_NOT_ALLOW, merge_info_mgr_.check_window_compaction_task(
      make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, WINDOW_START_TIME_US + window_duration_us,
      make_window_params(window_duration_us), global_info, need_update));
  EXPECT_FALSE(need_update);

  need_update = true;
  global_info.suspend_merging_.set_val(0, false);
  GCONF.enable_major_freeze = false;
  EXPECT_EQ(OB_MAJOR_FREEZE_NOT_ALLOW, merge_info_mgr_.check_window_compaction_task(
      make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, WINDOW_START_TIME_US + window_duration_us,
      make_window_params(window_duration_us), global_info, need_update));
  EXPECT_FALSE(need_update);
  EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
}

TEST_F(TestSSWindowCompactionInfo, reject_stopped_ss_window_retry)
{
  ObGlobalMergeInfo global_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_info);
  global_info.merge_start_time_.set_val(WINDOW_START_TIME_US, false);

  // A stopped window must reject both unfinished and completed tasks.
  for (const bool completed : {false, true}) {
    global_info.last_merged_scn_.set_scn(make_scn(completed ? TARGET_SCN_VAL : BASE_SCN_VAL), false);
    global_info.merge_status_.set_val(
        completed ? ObZoneMergeInfo::MERGE_STATUS_IDLE : ObZoneMergeInfo::MERGE_STATUS_MERGING, false);
    for (const bool enabled : {false, true}) {
      for (const int64_t duration_us : {0L, 100L}) {
        SCOPED_TRACE(::testing::Message() << "completed=" << completed
            << ", enabled=" << enabled << ", duration_us=" << duration_us);
        ObWindowParameters params = make_window_params(duration_us);
        params.enable_window_compaction_ = enabled;
        bool need_update = true;
        const bool stopped = !enabled || 0 == duration_us;
        EXPECT_EQ(stopped ? OB_MAJOR_FREEZE_NOT_ALLOW : OB_SUCCESS,
            merge_info_mgr_.check_window_compaction_task(
                make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, WINDOW_START_TIME_US + 100,
                params, global_info, need_update));
        EXPECT_EQ(!stopped && !completed, need_update);
        EXPECT_EQ(WINDOW_START_TIME_US, global_info.merge_start_time());
      }
    }
  }
}

TEST_F(TestSSWindowCompactionInfo, reject_suspended_ss_window_compaction)
{
  set_zone_mgr_memory_global_info(
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_TENANT);
  zone_merge_mgr_.global_merge_info_.suspend_merging_.set_val(1, false);

  EXPECT_EQ(OB_MAJOR_FREEZE_NOT_ALLOW,
      zone_merge_mgr_.set_ss_window_compaction_info(make_scn(TARGET_SCN_VAL), WINDOW_START_TIME_US, 1));
}

TEST_F(TestSSWindowCompactionInfo, completed_tenant_merge_does_not_need_merge)
{
  bool need_tenant_merge = true;
  ObGlobalMergeInfo global_merge_info;
  ObMajorMergeScheduler merge_scheduler(DEFAULT_TENANT_ID);
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_TENANT,
      global_merge_info);

  ASSERT_EQ(OB_SUCCESS, merge_scheduler.check_or_update_major_merge_state(
      EXPECTED_EPOCH, global_merge_info, need_tenant_merge));
  EXPECT_FALSE(need_tenant_merge);
}

TEST_F(TestSSWindowCompactionInfo, ss_window_compaction_active_guard_matrix)
{
  struct TestCase
  {
    uint64_t frozen_scn_val_;
    uint64_t global_broadcast_scn_val_;
    uint64_t last_merged_scn_val_;
    ObZoneMergeInfo::MergeStatus merge_status_;
    ObGlobalMergeInfo::MergeMode merge_mode_;
    uint64_t target_scn_val_;
    bool expected_active_;
  };
  const TestCase test_cases[] = {
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, true},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL + 1, false},
    {TARGET_SCN_VAL + 1, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL + 1, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, TARGET_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_IDLE, ObGlobalMergeInfo::MERGE_MODE_WINDOW,
        TARGET_SCN_VAL, false},
    {TARGET_SCN_VAL, TARGET_SCN_VAL, BASE_SCN_VAL,
        ObZoneMergeInfo::MERGE_STATUS_MERGING, ObGlobalMergeInfo::MERGE_MODE_TENANT,
        TARGET_SCN_VAL, false},
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(test_cases); ++i) {
    const TestCase &test_case = test_cases[i];
    ObGlobalMergeInfo global_merge_info;
    SCOPED_TRACE(i);
    build_global_merge_info(
        test_case.frozen_scn_val_,
        test_case.global_broadcast_scn_val_,
        test_case.last_merged_scn_val_,
        test_case.merge_status_,
        test_case.merge_mode_,
        global_merge_info);
    EXPECT_EQ(test_case.expected_active_,
        global_merge_info.is_ss_window_compaction_active(make_scn(test_case.target_scn_val_)));
  }

  ObGlobalMergeInfo global_merge_info;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_MERGING,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_merge_info);
  EXPECT_FALSE(global_merge_info.is_ss_window_compaction_active(SCN::invalid_scn()));
  global_merge_info.is_merge_error_.set_val(1, false);
  EXPECT_TRUE(global_merge_info.is_ss_window_compaction_active(make_scn(TARGET_SCN_VAL)));
}

TEST_F(TestSSWindowCompactionInfo, check_and_finish_guard)
{
  ObGlobalMergeInfo global_merge_info;
  ObMajorMergeScheduler merge_scheduler(DEFAULT_TENANT_ID);
  bool finished = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge_scheduler.check_and_finish_ss_window_compaction(
      EXPECTED_EPOCH, global_merge_info, finished));
  EXPECT_FALSE(finished);

  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      global_merge_info);
  finished = true;
  EXPECT_EQ(OB_SUCCESS, merge_scheduler.check_and_finish_ss_window_compaction(
      EXPECTED_EPOCH, global_merge_info, finished));
  EXPECT_FALSE(finished);

  global_merge_info.merge_status_.set_val(ObZoneMergeInfo::MERGE_STATUS_MERGING, false);
  global_merge_info.is_merge_error_.set_val(1, false);
  finished = true;
  EXPECT_EQ(OB_SUCCESS, merge_scheduler.check_and_finish_ss_window_compaction(
      EXPECTED_EPOCH, global_merge_info, finished));
  EXPECT_FALSE(finished);
}

// SSW-C2-T002～T004：验证同一轮复用缓存，以及 target 或 epoch 变化时清理缓存。
TEST_F(TestSSWindowCompactionInfo, progress_checker_reuses_and_resets_cache)
{
  volatile bool stop = false;
  const uint8_t loop_count = 5;
  const ObTabletID tablet_id(200001);
  const ObFreezeInfo freeze_info(make_scn(TARGET_SCN_VAL), 10, 20);
  const ObFreezeInfo next_freeze_info(make_scn(TARGET_SCN_VAL + 1), 11, 21);
  ObMajorMergeProgressChecker progress_checker(DEFAULT_TENANT_ID, stop);
  ASSERT_EQ(OB_SUCCESS, progress_checker.tablet_status_map_.create(
      8, "TSSWTablet", "TSSWTablet"));
  ASSERT_EQ(OB_SUCCESS, progress_checker.table_compaction_map_.create(
      8, "TSSWTable", "TSSWTable"));

  progress_checker.freeze_info_ = freeze_info;
  progress_checker.expected_epoch_ = EXPECTED_EPOCH;
  progress_checker.first_loop_in_cur_round_ = false;
  progress_checker.loop_cnt_ = loop_count;
  progress_checker.progress_.total_table_cnt_ = 3;
  ASSERT_EQ(OB_SUCCESS, progress_checker.tablet_status_map_.set_refactored(
      tablet_id, compaction::ObTabletCompactionStatusEnum::COMPACTED));

  ASSERT_EQ(OB_SUCCESS, progress_checker.set_basic_info_if_needed(
      freeze_info, EXPECTED_EPOCH));
  EXPECT_EQ(1, progress_checker.tablet_status_map_.size());
  EXPECT_EQ(loop_count, progress_checker.loop_cnt_);
  EXPECT_EQ(3, progress_checker.progress_.total_table_cnt_);

  ASSERT_EQ(OB_SUCCESS, progress_checker.set_basic_info_if_needed(
      freeze_info, EXPECTED_EPOCH + 1));
  EXPECT_EQ(0, progress_checker.tablet_status_map_.size());
  EXPECT_TRUE(progress_checker.first_loop_in_cur_round_);
  EXPECT_EQ(0, progress_checker.loop_cnt_);
  EXPECT_EQ(freeze_info, progress_checker.freeze_info_);
  EXPECT_EQ(EXPECTED_EPOCH + 1, progress_checker.expected_epoch_);

  progress_checker.first_loop_in_cur_round_ = false;
  ASSERT_EQ(OB_SUCCESS, progress_checker.tablet_status_map_.set_refactored(
      tablet_id, compaction::ObTabletCompactionStatusEnum::COMPACTED));
  ASSERT_EQ(OB_SUCCESS, progress_checker.set_basic_info_if_needed(
      next_freeze_info, EXPECTED_EPOCH + 1));
  EXPECT_EQ(0, progress_checker.tablet_status_map_.size());
  EXPECT_TRUE(progress_checker.first_loop_in_cur_round_);
  EXPECT_EQ(next_freeze_info, progress_checker.freeze_info_);

  EXPECT_EQ(OB_INVALID_ARGUMENT, progress_checker.set_basic_info_if_needed(
      ObFreezeInfo(), EXPECTED_EPOCH));
  EXPECT_EQ(OB_INVALID_ARGUMENT, progress_checker.set_basic_info_if_needed(
      freeze_info, -1));
  EXPECT_EQ(OB_SUCCESS, progress_checker.tablet_status_map_.destroy());
  EXPECT_EQ(OB_SUCCESS, progress_checker.table_compaction_map_.destroy());
}

// SSW-C2-T006：停止信号置位时，批量补 report_scn 必须明确返回取消。
TEST_F(TestSSWindowCompactionInfo, batch_update_report_scn_returns_canceled_when_stopped)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t original_cluster_version = cluster_version.get_cluster_version();
  volatile bool stop = true;
  cluster_version.update_cluster_version(DATA_VERSION_4_1_0_0);
  cluster_version.update_data_version(DATA_VERSION_4_1_0_0);

  EXPECT_EQ(OB_CANCELED, ObTabletMetaTableCompactionOperator::batch_update_report_scn(
      DEFAULT_TENANT_ID,
      TARGET_SCN_VAL,
      ObTabletReplica::SCN_STATUS_ERROR,
      stop,
      EXPECTED_EPOCH));

  cluster_version.update_data_version(0);
  cluster_version.update_cluster_version(original_cluster_version);
}

TEST_F(TestSSWindowCompactionInfo, zone_manager_rejects_inactive_or_error_state)
{
  ObZoneMergeManager zone_merge_mgr;
  zone_merge_mgr.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_mgr.is_inited_ = true;
  zone_merge_mgr.is_loaded_ = true;
  zone_merge_mgr.shadow_.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_mgr.shadow_.is_inited_ = true;
  zone_merge_mgr.shadow_.is_loaded_ = true;
  build_global_merge_info(
      TARGET_SCN_VAL,
      TARGET_SCN_VAL,
      BASE_SCN_VAL,
      ObZoneMergeInfo::MERGE_STATUS_IDLE,
      ObGlobalMergeInfo::MERGE_MODE_WINDOW,
      zone_merge_mgr.global_merge_info_);
  EXPECT_EQ(OB_STATE_NOT_MATCH, zone_merge_mgr.finish_ss_window_compaction(
      EXPECTED_EPOCH, make_scn(TARGET_SCN_VAL)));

  zone_merge_mgr.global_merge_info_.merge_status_.set_val(
      ObZoneMergeInfo::MERGE_STATUS_MERGING, false);
  zone_merge_mgr.global_merge_info_.is_merge_error_.set_val(1, false);
  EXPECT_EQ(OB_STATE_NOT_MATCH, zone_merge_mgr.finish_ss_window_compaction(
      EXPECTED_EPOCH, make_scn(TARGET_SCN_VAL)));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
