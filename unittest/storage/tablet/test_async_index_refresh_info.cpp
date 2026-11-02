/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "storage/tablet/ob_async_index_refresh_info.h"
#include "storage/tablet/ob_async_index_refresh_info_replay_executor.h"
#include "storage/tablet/ob_tablet_mds_data_cache.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::common;

class LegacyAsyncIndexRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  LegacyAsyncIndexRefreshInfo()
      : refresh_snapshot_versions_(),
        has_async_index_(false)
  {}

  ObSEArray<int64_t, 8> refresh_snapshot_versions_;
  bool has_async_index_;
};

OB_SERIALIZE_MEMBER(LegacyAsyncIndexRefreshInfo,
                    refresh_snapshot_versions_,
                    has_async_index_);

class TestAsyncIndexRefreshInfo : public ::testing::Test
{
public:
  TestAsyncIndexRefreshInfo() {}
  virtual ~TestAsyncIndexRefreshInfo() {}
};

// ----------------------------------------------------------------
// is_valid
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, merge_floor_pin_preserves_checkpoints_and_metadata)
{
  ObTabletAsyncIndexRefreshInfo info;
  ObTabletAsyncIndexRefreshInfo pin;
  info.set_has_async_index(true);
  info.set_published_version(123);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(250));
  ASSERT_EQ(OB_SUCCESS, pin.append_refresh_snapshot(150));
  ASSERT_EQ(OB_SUCCESS, info.merge_floor_pin(pin));
  ASSERT_EQ(3, info.get_refresh_snapshots().count());
  EXPECT_EQ(150, info.get_refresh_snapshots().at(0));
  EXPECT_EQ(200, info.get_refresh_snapshots().at(1));
  EXPECT_EQ(250, info.get_refresh_snapshots().at(2));
  EXPECT_TRUE(info.has_async_index());
  EXPECT_EQ(123, info.get_published_version());
  ASSERT_EQ(OB_SUCCESS, info.merge_floor_pin(pin));
  EXPECT_EQ(3, info.get_refresh_snapshots().count());
  int64_t next = 0;
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(200, next));
  EXPECT_EQ(250, next);
}

TEST_F(TestAsyncIndexRefreshInfo, initial_build_pin_survives_commit_and_full_checkpoints)
{
  ObTabletAsyncIndexRefreshInfo info;
  ObTabletAsyncIndexRefreshInfo pin;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(201));
  info.set_has_async_index(true);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  info.on_commit(commit_version, commit_scn);
  ASSERT_EQ(100, info.get_refresh_snapshots().at(0));
  for (int64_t i = 1; i < MAX_INC_MAJOR_SSTABLE_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200 + i));
  }
  ASSERT_EQ(OB_SUCCESS, pin.append_refresh_snapshot(100));
  EXPECT_FALSE(info.need_apply_floor_pin(pin));
  ASSERT_EQ(OB_SUCCESS, info.merge_floor_pin(pin));
  ASSERT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, info.get_refresh_snapshots().count());
  EXPECT_EQ(100, info.get_refresh_snapshots().at(0));
  for (int64_t i = 1; i < MAX_INC_MAJOR_SSTABLE_CNT; ++i) {
    EXPECT_EQ(200 + i, info.get_refresh_snapshots().at(i));
  }
  EXPECT_TRUE(info.has_async_index());
  EXPECT_FALSE(info.has_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, merge_floor_pin_empty_and_capacity)
{
  ObTabletAsyncIndexRefreshInfo info;
  ObTabletAsyncIndexRefreshInfo pin;
  EXPECT_EQ(OB_INVALID_ARGUMENT, info.merge_floor_pin(pin));
  ASSERT_EQ(OB_SUCCESS, pin.append_refresh_snapshot(150));
  ASSERT_EQ(OB_SUCCESS, info.merge_floor_pin(pin));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  EXPECT_EQ(150, info.get_refresh_snapshots().at(0));
  info.reset();
  for (int64_t i = 0; i < MAX_INC_MAJOR_SSTABLE_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200 + i));
  }
  EXPECT_EQ(OB_SIZE_OVERFLOW, info.merge_floor_pin(pin));
  EXPECT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, info.get_refresh_snapshots().count());
  EXPECT_EQ(200, info.get_refresh_snapshots().at(0));
  pin.reset();
  ASSERT_EQ(OB_SUCCESS, pin.append_refresh_snapshot(250));
  EXPECT_EQ(OB_SUCCESS, info.merge_floor_pin(pin));
  EXPECT_EQ(MAX_INC_MAJOR_SSTABLE_CNT, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_empty)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_single)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_strictly_increasing)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  ASSERT_TRUE(info.is_valid());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_equal_elements)
{
  // Manually push equal elements to bypass append_refresh_snapshot validation
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(200));
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(200));
  ASSERT_FALSE(info.is_valid());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_non_monotone)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(100));
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(300));
  ASSERT_EQ(OB_SUCCESS, info.refresh_snapshot_versions_.push_back(200));
  ASSERT_FALSE(info.is_valid());
}

TEST_F(TestAsyncIndexRefreshInfo, published_version_value_semantics)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION,
            info.get_published_version());
  ASSERT_FALSE(info.has_published_version());

  info.set_published_version(0);
  ASSERT_TRUE(info.has_published_version());
  ASSERT_TRUE(info.is_valid());

  info.set_published_version(100);
  ASSERT_TRUE(info.has_published_version());
  ASSERT_TRUE(info.is_valid());

  info.set_published_version(
      ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);
  ASSERT_TRUE(info.is_valid());
  ASSERT_TRUE(info.need_fill_commit_version());
  ASSERT_FALSE(info.has_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, initial_commit_pin_matches_index_publication)
{
  ObTabletAsyncIndexRefreshInfo data_info;
  ObTabletAsyncIndexRefreshInfo index_info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  data_info.set_has_async_index(true);
  data_info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, data_info.init_pending_refresh_snapshot());
  index_info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);

  data_info.on_commit(commit_version, commit_scn);
  index_info.on_commit(commit_version, commit_scn);
  ASSERT_TRUE(data_info.is_valid_for_refresh());
  ASSERT_TRUE(data_info.has_async_index());
  ASSERT_FALSE(data_info.has_published_version());
  ASSERT_EQ(1, data_info.get_refresh_snapshots().count());
  EXPECT_EQ(300, data_info.get_refresh_snapshots().at(0));
  EXPECT_EQ(300, index_info.get_published_version());

  // The data retention point and index publication resolve to the same C, not commit SCN.
  // Later DML advances the refresh checkpoint from C to T.
  int64_t next = 0;
  EXPECT_EQ(OB_ITER_END, data_info.get_next_refresh_snapshot(300, next));
  ASSERT_EQ(OB_SUCCESS, data_info.append_refresh_snapshot(500));
  ASSERT_EQ(OB_SUCCESS, data_info.get_next_refresh_snapshot(300, next));
  EXPECT_EQ(500, next);
  ASSERT_EQ(OB_SUCCESS, data_info.cleanup_retired_snapshots(500));
  ASSERT_EQ(1, data_info.get_refresh_snapshots().count());
  EXPECT_EQ(500, data_info.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexRefreshInfo, create_bootstrap_uses_commit_version)
{
  ObTabletAsyncIndexRefreshInfo info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  info.set_published_version(
      ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);

  info.on_commit(commit_version, commit_scn);
  ASSERT_FALSE(info.need_fill_commit_version());
  ASSERT_TRUE(info.has_published_version());
  ASSERT_EQ(300, info.get_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_for_refresh_empty)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_TRUE(info.is_valid());
  ASSERT_TRUE(info.is_valid_for_refresh());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_for_refresh_rejects_pending)
{
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  ASSERT_TRUE(info.is_valid());
  ASSERT_FALSE(info.is_valid_for_refresh());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_for_refresh_multi_element)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_TRUE(info.is_valid_for_refresh());
}

TEST_F(TestAsyncIndexRefreshInfo, is_valid_for_refresh_after_membership_commit)
{
  ObTabletAsyncIndexRefreshInfo info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  info.set_has_async_index(true);
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  info.on_commit(commit_version, commit_scn);
  ASSERT_TRUE(info.is_valid());
  ASSERT_TRUE(info.is_valid_for_refresh());
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_refresh_snapshot_rejects_pending)
{
  ObTabletAsyncIndexRefreshInfo info;
  int64_t next = 0;
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  ASSERT_EQ(OB_ERR_UNEXPECTED, info.get_next_refresh_snapshot(0, next));
}

TEST_F(TestAsyncIndexRefreshInfo, init_pending_refresh_snapshot_no_op_without_use_commit)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  ASSERT_EQ(0, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, init_pending_refresh_snapshot)
{
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  ASSERT_TRUE(info.is_valid());
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_TRUE(ObTabletAsyncIndexRefreshInfo::is_pending_refresh_snapshot(info.get_refresh_snapshots().at(0)));
  ASSERT_EQ(OB_ERR_UNEXPECTED, info.init_pending_refresh_snapshot());
}

TEST_F(TestAsyncIndexRefreshInfo, init_pending_refresh_snapshot_rejects_nonempty)
{
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_ERR_UNEXPECTED, info.init_pending_refresh_snapshot());
}

TEST_F(TestAsyncIndexRefreshInfo, membership_auto_pins_gc_snapshot_on_commit)
{
  ObTabletAsyncIndexRefreshInfo info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  info.set_has_async_index(true);
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.init_pending_refresh_snapshot());
  info.on_commit(commit_version, commit_scn);
  ASSERT_FALSE(info.need_fill_refresh_snapshot_on_commit());
  ASSERT_FALSE(info.has_published_version());
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(300, info.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexRefreshInfo, membership_auto_pin_skips_when_snapshot_exists)
{
  ObTabletAsyncIndexRefreshInfo info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  info.set_has_async_index(true);
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  info.on_commit(commit_version, commit_scn);
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(100, info.get_refresh_snapshots().at(0));
  ASSERT_FALSE(info.has_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, explicit_published_version_is_not_rewritten)
{
  ObTabletAsyncIndexRefreshInfo info;
  share::SCN commit_version;
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_version.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(400));
  info.set_published_version(200);
  info.on_commit(commit_version, commit_scn);
  ASSERT_EQ(200, info.get_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, replay_committed_tablet_allowed)
{
  ObTabletCreateDeleteMdsUserData tablet_status;
  tablet_status.tablet_status_ = ObTabletStatus::NORMAL;
  const mds::MdsWriter tablet_writer(transaction::ObTransID(100));
  const mds::MdsWriter refresh_writer(transaction::ObTransID(200));
  ASSERT_EQ(OB_SUCCESS,
      ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
          tablet_status,
          tablet_writer,
          mds::TwoPhaseCommitState::ON_COMMIT,
          refresh_writer));
}

TEST_F(TestAsyncIndexRefreshInfo, replay_same_transaction_create_allowed)
{
  ObTabletCreateDeleteMdsUserData tablet_status;
  tablet_status.tablet_status_ = ObTabletStatus::NORMAL;
  tablet_status.create_commit_version_ =
      transaction::ObTransVersion::INVALID_TRANS_VERSION;
  const mds::MdsWriter writer(transaction::ObTransID(100));
  ASSERT_EQ(OB_SUCCESS,
      ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
          tablet_status,
          writer,
          mds::TwoPhaseCommitState::BEFORE_PREPARE,
          writer));
}

TEST_F(TestAsyncIndexRefreshInfo, replay_other_transaction_create_retries)
{
  ObTabletCreateDeleteMdsUserData tablet_status;
  tablet_status.tablet_status_ = ObTabletStatus::NORMAL;
  tablet_status.create_commit_version_ =
      transaction::ObTransVersion::INVALID_TRANS_VERSION;
  const mds::MdsWriter tablet_writer(transaction::ObTransID(100));
  const mds::MdsWriter refresh_writer(transaction::ObTransID(200));
  ASSERT_EQ(OB_EAGAIN,
      ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
          tablet_status,
          tablet_writer,
          mds::TwoPhaseCommitState::BEFORE_PREPARE,
          refresh_writer));
}

TEST_F(TestAsyncIndexRefreshInfo, replay_transfer_and_split_never_bypass_status)
{
  ObTabletCreateDeleteMdsUserData tablet_status;
  const mds::MdsWriter writer(transaction::ObTransID(100));
  tablet_status.tablet_status_ = ObTabletStatus::TRANSFER_IN;
  ASSERT_EQ(OB_EAGAIN,
      ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
          tablet_status,
          writer,
          mds::TwoPhaseCommitState::BEFORE_PREPARE,
          writer));
  tablet_status.tablet_status_ = ObTabletStatus::SPLIT_DST;
  ASSERT_EQ(OB_EAGAIN,
      ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
          tablet_status,
          writer,
          mds::TwoPhaseCommitState::BEFORE_PREPARE,
          writer));
}

// ----------------------------------------------------------------
// append_refresh_snapshot
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, append_normal)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(100, info.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexRefreshInfo, append_strictly_greater)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(2, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, append_equal_rejected)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(100));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, append_smaller_rejected)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(100));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, append_zero_rejected)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(0));
  ASSERT_EQ(0, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, append_negative_rejected)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.append_refresh_snapshot(-1));
  ASSERT_EQ(0, info.get_refresh_snapshots().count());
}

TEST_F(TestAsyncIndexRefreshInfo, append_overflow)
{
  ObTabletAsyncIndexRefreshInfo info;
  const int64_t limit = MAX_INC_MAJOR_SSTABLE_CNT;
  for (int64_t i = 1; i <= limit; ++i) {
    ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(i * 100));
  }
  ASSERT_EQ(limit, info.get_refresh_snapshots().count());
  ASSERT_EQ(OB_SIZE_OVERFLOW, info.append_refresh_snapshot((limit + 1) * 100));
  ASSERT_EQ(limit, info.get_refresh_snapshots().count());
}

// ----------------------------------------------------------------
// cleanup_retired_snapshots
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, cleanup_invalid_arg)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.cleanup_retired_snapshots(0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.cleanup_retired_snapshots(-1));
}

TEST_F(TestAsyncIndexRefreshInfo, cleanup_keeps_min_publish)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(300));
  ASSERT_EQ(1, info.get_refresh_snapshots().count());
  ASSERT_EQ(300, info.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexRefreshInfo, cleanup_partial_prefix)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(150));
  ASSERT_EQ(3, info.get_refresh_snapshots().count());
  ASSERT_EQ(150, info.get_refresh_snapshots().at(0));
  ASSERT_EQ(200, info.get_refresh_snapshots().at(1));
  ASSERT_EQ(300, info.get_refresh_snapshots().at(2));
}

TEST_F(TestAsyncIndexRefreshInfo, cleanup_inserts_min_before_first)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(50));
  ASSERT_EQ(3, info.get_refresh_snapshots().count());
  ASSERT_EQ(50, info.get_refresh_snapshots().at(0));
}

TEST_F(TestAsyncIndexRefreshInfo, cleanup_empty_array)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.cleanup_retired_snapshots(100));
  ASSERT_EQ(0, info.get_refresh_snapshots().count());
}

// ----------------------------------------------------------------
// get_next_refresh_snapshot
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, get_next_empty_array)
{
  ObTabletAsyncIndexRefreshInfo info;
  int64_t next = 0;
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(0, next));
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_last_below_all)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  int64_t next = 0;
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(50, next));
  ASSERT_EQ(100, next);
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_last_equals_first)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  int64_t next = 0;
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(100, next));
  ASSERT_EQ(200, next);
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_last_between_elements)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  int64_t next = 0;
  ASSERT_EQ(OB_SUCCESS, info.get_next_refresh_snapshot(150, next));
  ASSERT_EQ(200, next);
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_last_equals_last)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  int64_t next = 0;
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(300, next));
}

TEST_F(TestAsyncIndexRefreshInfo, get_next_last_above_all)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));
  int64_t next = 0;
  ASSERT_EQ(OB_ITER_END, info.get_next_refresh_snapshot(400, next));
}

// ----------------------------------------------------------------
// serialize / deserialize round-trip
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, serialize_empty)
{
  ObTabletAsyncIndexRefreshInfo info;
  const int64_t BUF_LEN = 512;
  char buf[BUF_LEN];
  int64_t wpos = 0;
  ASSERT_EQ(OB_SUCCESS, info.serialize(buf, BUF_LEN, wpos));
  ASSERT_EQ(wpos, info.get_serialize_size());

  ObTabletAsyncIndexRefreshInfo info2;
  int64_t rpos = 0;
  ASSERT_EQ(OB_SUCCESS, info2.deserialize(buf, wpos, rpos));
  ASSERT_EQ(0, info2.get_refresh_snapshots().count());
  ASSERT_FALSE(info2.has_async_index());
}

TEST_F(TestAsyncIndexRefreshInfo, serialize_with_data)
{
  ObTabletAsyncIndexRefreshInfo info;
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(200));
  ASSERT_EQ(OB_SUCCESS, info.append_refresh_snapshot(300));

  const int64_t BUF_LEN = 512;
  char buf[BUF_LEN];
  int64_t wpos = 0;
  ASSERT_EQ(OB_SUCCESS, info.serialize(buf, BUF_LEN, wpos));
  ASSERT_EQ(wpos, info.get_serialize_size());

  ObTabletAsyncIndexRefreshInfo info2;
  int64_t rpos = 0;
  ASSERT_EQ(OB_SUCCESS, info2.deserialize(buf, wpos, rpos));
  ASSERT_EQ(rpos, wpos);
  ASSERT_TRUE(info2.is_valid());
  ASSERT_EQ(3, info2.get_refresh_snapshots().count());
  ASSERT_EQ(100, info2.get_refresh_snapshots().at(0));
  ASSERT_EQ(200, info2.get_refresh_snapshots().at(1));
  ASSERT_EQ(300, info2.get_refresh_snapshots().at(2));
}

TEST_F(TestAsyncIndexRefreshInfo, serialize_membership_round_trip)
{
  for (int64_t value = 0; value <= 1; ++value) {
    ObTabletAsyncIndexRefreshInfo info;
    info.set_has_async_index(1 == value);

    char buf[512];
    int64_t wpos = 0;
    ASSERT_EQ(OB_SUCCESS, info.serialize(buf, sizeof(buf), wpos));

    ObTabletAsyncIndexRefreshInfo decoded;
    int64_t rpos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, wpos, rpos));
    ASSERT_EQ(wpos, rpos);
    ASSERT_TRUE(decoded.is_valid());
    ASSERT_EQ(1 == value, decoded.has_async_index());
  }
}

TEST_F(TestAsyncIndexRefreshInfo, serialize_published_version_round_trip)
{
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(200);

  char buf[512];
  int64_t wpos = 0;
  ASSERT_EQ(OB_SUCCESS, info.serialize(buf, sizeof(buf), wpos));

  ObTabletAsyncIndexRefreshInfo decoded;
  int64_t rpos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, wpos, rpos));
  ASSERT_EQ(wpos, rpos);
  ASSERT_EQ(200, decoded.get_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, deserialize_legacy_value)
{
  LegacyAsyncIndexRefreshInfo legacy;
  ASSERT_EQ(OB_SUCCESS, legacy.refresh_snapshot_versions_.push_back(100));
  legacy.has_async_index_ = true;

  char buf[512];
  int64_t wpos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(buf, sizeof(buf), wpos));

  ObTabletAsyncIndexRefreshInfo decoded;
  int64_t rpos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, wpos, rpos));
  ASSERT_EQ(wpos, rpos);
  ASSERT_TRUE(decoded.has_async_index());
  ASSERT_EQ(ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION,
            decoded.get_published_version());
}

// ----------------------------------------------------------------
// assign
// ----------------------------------------------------------------

TEST_F(TestAsyncIndexRefreshInfo, assign_deep_copy)
{
  ObTabletAsyncIndexRefreshInfo src;
  ASSERT_EQ(OB_SUCCESS, src.append_refresh_snapshot(100));
  ASSERT_EQ(OB_SUCCESS, src.append_refresh_snapshot(200));
  src.set_has_async_index(true);
  src.set_published_version(300);

  ObTabletAsyncIndexRefreshInfo dst;
  ASSERT_EQ(OB_SUCCESS, dst.assign(src));
  ASSERT_EQ(2, dst.get_refresh_snapshots().count());
  ASSERT_TRUE(dst.has_async_index());
  ASSERT_EQ(300, dst.get_published_version());

  // Mutate src; dst should not be affected
  src.reset();
  ASSERT_EQ(0, src.get_refresh_snapshots().count());
  ASSERT_EQ(2, dst.get_refresh_snapshots().count());
  ASSERT_EQ(100, dst.get_refresh_snapshots().at(0));
  ASSERT_EQ(200, dst.get_refresh_snapshots().at(1));
  ASSERT_TRUE(dst.has_async_index());
  ASSERT_EQ(300, dst.get_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, membership_cache_value_lifecycle)
{
  ObAsyncIndexMembershipCache cache;
  ASSERT_EQ(0, cache.get_invalidation_count());
  cache.reset();
  const int64_t before_read = cache.get_invalidation_count();
  cache.reset(); // A second notification must invalidate an already pending refill.
  ASSERT_NE(before_read, cache.get_invalidation_count());
  ASSERT_FALSE(cache.is_valid());
  ASSERT_FALSE(cache.has_async_index());
  ASSERT_FALSE(cache.has_published_version());

  cache.set_value(true, 0);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_TRUE(cache.has_async_index());
  ASSERT_TRUE(cache.has_published_version());
  ASSERT_EQ(0, cache.get_published_version());

  cache.set_value(false, 200);
  ASSERT_TRUE(cache.is_valid());
  ASSERT_FALSE(cache.has_async_index());
  ASSERT_EQ(200, cache.get_published_version());

  cache.reset();
  ASSERT_FALSE(cache.is_valid());
  ASSERT_FALSE(cache.has_async_index());
  ASSERT_FALSE(cache.has_published_version());
}

TEST_F(TestAsyncIndexRefreshInfo, membership_cache_copy_preserves_value)
{
  ObAsyncIndexMembershipCache source;
  ObAsyncIndexMembershipCache destination;
  source.reset();
  source.set_value(true, 200);
  destination = source;
  ASSERT_EQ(source.get_invalidation_count(), destination.get_invalidation_count());

  source.reset();
  ASSERT_NE(source.get_invalidation_count(), destination.get_invalidation_count());
  ASSERT_FALSE(source.is_valid());
  ASSERT_TRUE(destination.is_valid());
  ASSERT_TRUE(destination.has_async_index());
  ASSERT_EQ(200, destination.get_published_version());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
