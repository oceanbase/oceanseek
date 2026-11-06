/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "share/ob_cluster_version.h"
#include "storage/ls/ob_ls_meta.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;
using namespace palf;

struct CheckpointSlogState
{
  CheckpointSlogState() : result_(OB_SUCCESS), calls_(0) {}
  int result_;
  int64_t calls_;
  LSN lsn_;
  SCN scn_;
};

class CheckpointSlogWriter
{
public:
  explicit CheckpointSlogWriter(CheckpointSlogState &state) : state_(&state) {}
  int operator()(const ObLSMeta &meta) const
  {
    ++state_->calls_;
    state_->lsn_ = meta.get_clog_base_lsn();
    state_->scn_ = meta.get_clog_checkpoint_scn();
    return state_->result_;
  }
private:
  CheckpointSlogState *state_;
};

class CheckpointSlogGuard : public ObLSMeta
{
public:
  explicit CheckpointSlogGuard(CheckpointSlogState &state)
    : old_writer_(write_slog_, function::DefaultFunctionAllocator::get_default_allocator())
  {
    write_slog_ = CheckpointSlogWriter(state);
  }
  ~CheckpointSlogGuard() override { write_slog_ = old_writer_; }
private:
  WriteSlog old_writer_;
  DISALLOW_COPY_AND_ASSIGN(CheckpointSlogGuard);
};

class TestLSMetaCheckpoint : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObMajorMVMergeInfo mv_info;
    ASSERT_EQ(OB_SUCCESS, mv_info.init(SCN::base_scn(), SCN::base_scn(), SCN::base_scn()));
    ASSERT_EQ(OB_SUCCESS, meta_.init(1002, ObLSID(1001),
        ObMigrationStatus::OB_MIGRATION_STATUS_NONE,
        ObLSRestoreStatus(ObLSRestoreStatus::NONE), SCN::base_scn(), mv_info,
        ObLSStoreFormat(OB_LS_STORE_NORMAL), DATA_VERSION_4_6_3_0, REPLICA_TYPE_FULL));
    ASSERT_EQ(OB_SUCCESS, meta_.set_start_work_state());
    ASSERT_TRUE(meta_.is_valid());
  }
  ObLSMeta meta_;
};

TEST_F(TestLSMetaCheckpoint, failed_slog_preserves_both_recovery_fields)
{
  // A failed durable write must preserve both in-memory recovery coordinates.
  CheckpointSlogState state;
  state.result_ = OB_IO_ERROR;
  CheckpointSlogGuard guard(state);
  const LSN old_lsn = meta_.get_clog_base_lsn();
  const SCN old_scn = meta_.get_clog_checkpoint_scn();
  const LSN next_lsn(old_lsn.val_ + 8192);
  const SCN next_scn = SCN::plus(old_scn, 100);
  ASSERT_EQ(OB_IO_ERROR, meta_.set_clog_checkpoint(next_lsn, next_scn, true));
  ASSERT_EQ(1, state.calls_);
  ASSERT_EQ(next_lsn, state.lsn_);
  ASSERT_EQ(next_scn, state.scn_);
  ASSERT_EQ(old_lsn, meta_.get_clog_base_lsn());
  ASSERT_EQ(old_scn, meta_.get_clog_checkpoint_scn());
}

TEST_F(TestLSMetaCheckpoint, successful_slog_advances_both_recovery_fields)
{
  // Successful persistence publishes both fields from the snapshot passed to SLOG.
  CheckpointSlogState state;
  CheckpointSlogGuard guard(state);
  const LSN next_lsn(meta_.get_clog_base_lsn().val_ + 8192);
  const SCN next_scn = SCN::plus(meta_.get_clog_checkpoint_scn(), 100);
  ASSERT_EQ(OB_SUCCESS, meta_.set_clog_checkpoint(next_lsn, next_scn, true));
  ASSERT_EQ(1, state.calls_);
  ASSERT_EQ(next_lsn, state.lsn_);
  ASSERT_EQ(next_scn, state.scn_);
  ASSERT_EQ(next_lsn, meta_.get_clog_base_lsn());
  ASSERT_EQ(next_scn, meta_.get_clog_checkpoint_scn());
}

TEST_F(TestLSMetaCheckpoint, memory_only_update_does_not_invoke_slog)
{
  // The explicit memory-only path succeeds without invoking a failing SLOG writer.
  CheckpointSlogState state;
  state.result_ = OB_IO_ERROR;
  CheckpointSlogGuard guard(state);
  const LSN next_lsn(meta_.get_clog_base_lsn().val_ + 8192);
  const SCN next_scn = SCN::plus(meta_.get_clog_checkpoint_scn(), 100);
  ASSERT_EQ(OB_SUCCESS, meta_.set_clog_checkpoint(next_lsn, next_scn, false));
  ASSERT_EQ(0, state.calls_);
  ASSERT_EQ(next_lsn, meta_.get_clog_base_lsn());
  ASSERT_EQ(next_scn, meta_.get_clog_checkpoint_scn());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
