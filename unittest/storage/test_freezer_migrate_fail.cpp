/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "logservice/replayservice/ob_log_replay_service.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "share/rc/ob_tenant_base.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;

class TestFreezerMigrateFail : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    OB_LOGGER.set_file_name("test_freezer_migrate_fail.log");
    OB_LOGGER.set_log_level("WARN");
  }

protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
    ls_.reset(new ObLS());
    // Keep the LS services uninitialized. Exercise the real persisted-state
    // checks and freeze loops without log I/O or a background GC service.
    ls_->ls_meta_.tenant_id_ = 1002;
    ls_->ls_meta_.ls_id_ = ObLSID(1001);
    ls_->ls_meta_.gc_state_ = logservice::LSGCState::NORMAL;
    ls_->ls_meta_.restore_status_ = ObLSRestoreStatus::NONE;
    ls_->ls_meta_.rebuild_seq_ = 0;
    set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE);
    ASSERT_TRUE(ls_->ls_meta_.is_valid());
  }

  void TearDown() override
  {
    ls_->log_handler_.is_inited_ = false;
    ls_->log_handler_.replay_service_ = nullptr;
    ls_->running_state_.state_ = ObLSRunningState::State::LS_INIT;
    ls_.reset();
    ObClockGenerator::destroy();
  }

  void set_status(const ObMigrationStatus status)
  {
    ObLSMeta::ObReentrantWLockGuard guard(ls_->ls_meta_.rw_lock_);
    ls_->ls_meta_.migration_status_ = status;
  }

  std::unique_ptr<ObLS> ls_;
};

TEST_F(TestFreezerMigrateFail, normal_and_in_progress_migration_can_freeze)
{
  const ObMigrationStatus statuses[] = {
      ObMigrationStatus::OB_MIGRATION_STATUS_NONE,
      ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE,
      ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE_HOLD};
  for (const auto status : statuses) {
    set_status(status);
    ASSERT_EQ(OB_SUCCESS, ls_->check_migration_status_for_freeze());
    ASSERT_EQ(OB_SUCCESS, ls_->ls_freezer_.loop_set_freeze_flag(0));
    ASSERT_TRUE(ls_->ls_freezer_.is_freeze(ls_->ls_freezer_.freeze_flag_));
    ls_->ls_freezer_.unset_freeze_();
  }
}

TEST_F(TestFreezerMigrateFail, failed_migration_cannot_start_freeze)
{
  const ObMigrationStatus statuses[] = {
      ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE_FAIL};
  for (const auto status : statuses) {
    set_status(status);
    ASSERT_EQ(OB_CANCELED, ls_->check_migration_status_for_freeze());
    ASSERT_EQ(OB_CANCELED, ls_->ls_freezer_.loop_set_freeze_flag(10_s));
    ASSERT_FALSE(ls_->ls_freezer_.is_freeze(ls_->ls_freezer_.freeze_flag_));
  }
}

TEST_F(TestFreezerMigrateFail, unreadable_migration_status_prevents_freeze_until_status_recovers)
{
  ObFreezer &freezer = ls_->ls_freezer_;
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MAX);
  EXPECT_EQ(OB_ERR_UNEXPECTED, freezer.loop_set_freeze_flag(0));
  EXPECT_FALSE(freezer.is_freeze(freezer.get_freeze_flag()));
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_NONE);
  ASSERT_EQ(OB_SUCCESS, freezer.loop_set_freeze_flag(0));
  EXPECT_TRUE(freezer.is_freeze(freezer.get_freeze_flag()));
  freezer.unset_freeze_();
}

TEST_F(TestFreezerMigrateFail, failure_interrupts_flag_wait_and_releases_gc_write_lock)
{
  ObFreezer &freezer = ls_->ls_freezer_;
  ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag_without_inc_freeze_clock());
  std::promise<void> reader_locked;
  std::promise<void> writer_started;
  auto reader_ready = reader_locked.get_future();
  auto writer_ready = writer_started.get_future();
  auto reader = std::async(std::launch::async, [&]() {
    ObLSLockGuard guard(ls_.get(), true /* read lock */);
    reader_locked.set_value();
    return freezer.loop_set_freeze_flag(10_s);
  });
  reader_ready.wait();
  auto writer = std::async(std::launch::async, [&]() {
    writer_started.set_value();
    ObLSLockGuard guard(ls_.get());
    return guard.locked();
  });
  writer_ready.wait();
  EXPECT_EQ(std::future_status::timeout, writer.wait_for(std::chrono::milliseconds(20)));
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  const auto reader_state = reader.wait_for(std::chrono::seconds(1));
  EXPECT_EQ(std::future_status::ready, reader_state);
  if (std::future_status::ready != reader_state) {
    // Bound failure of the regression itself; let the old implementation exit.
    freezer.unset_freeze_();
  }
  EXPECT_EQ(OB_CANCELED, reader.get());
  EXPECT_EQ(std::future_status::ready, writer.wait_for(std::chrono::seconds(1)));
  EXPECT_TRUE(writer.get());
  // A waiter must not release the flag belonging to the original freeze.
  EXPECT_TRUE(freezer.is_freeze(freezer.freeze_flag_));
  freezer.unset_freeze_();
}

TEST_F(TestFreezerMigrateFail, failed_migration_rejects_admission_before_ls_lock_or_async_queue)
{
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  ObSEArray<ObTabletID, 1> tablets;
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(200001)));
  std::future<int> tablet_task;
  std::future<int> ls_task;
  {
    ObLSLockGuard gc_lock(ls_.get());
    const int64_t deadline = ObClockGenerator::getClock() + 1_s;
    tablet_task = std::async(std::launch::async, [&, deadline]() {
      return ls_->tablet_freeze_task(0, tablets, false, true, deadline, 0);
    });
    ls_task = std::async(std::launch::async, [&, deadline]() {
      return ls_->logstream_freeze_task(0, deadline);
    });
    EXPECT_EQ(std::future_status::ready, tablet_task.wait_for(std::chrono::milliseconds(100)));
    EXPECT_EQ(std::future_status::ready, ls_task.wait_for(std::chrono::milliseconds(100)));
  }
  EXPECT_EQ(OB_CANCELED, tablet_task.get());
  EXPECT_EQ(OB_CANCELED, ls_task.get());
  EXPECT_EQ(OB_CANCELED, ls_->tablet_freeze(0, tablets, false, 0, false,
      ObFreezeSourceFlag::MAJOR_FREEZE));
  EXPECT_TRUE(ls_->ls_freezer_.is_async_freeze_tablets_empty());
}

TEST_F(TestFreezerMigrateFail, disabled_replay_finishes_owned_freeze_with_error)
{
  ls_->running_state_.state_ = ObLSRunningState::State::LS_RUNNING;
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  ObFreezer &freezer = ls_->ls_freezer_;
  ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag_without_inc_freeze_clock());
  ObArenaAllocator allocator;
  void *buffer = allocator.alloc(sizeof(memtable::ObMemtable));
  ASSERT_NE(nullptr, buffer);
  auto *memtable = new (buffer) memtable::ObMemtable();
  memtable->key_.table_type_ = ObITable::DATA_MEMTABLE;
  memtable->key_.tablet_id_ = LS_MDS_TABLET;
  ObTableHandleV2 handle;
  handle.table_ = memtable;
  handle.table_type_ = ObITable::DATA_MEMTABLE;
  handle.allocator_ = &allocator;
  memtable->inc_ref();
  ObSEArray<ObTableHandleV2, 1> handles;
  ObSEArray<ObTabletID, 1> failed_tablets;
  ASSERT_EQ(OB_SUCCESS, handles.push_back(handle));
  // The uninitialized log handler reports replay disabled, as after offline.
  ASSERT_EQ(OB_NOT_RUNNING, freezer.wait_tablet_freeze_finish(handles, failed_tablets));
  ASSERT_FALSE(freezer.is_freeze(freezer.freeze_flag_));
  ASSERT_EQ(1, failed_tablets.count());
  ASSERT_EQ(LS_MDS_TABLET, failed_tablets.at(0));
  // Failure discovered during completion is rejected at the next task's entry.
  EXPECT_EQ(OB_CANCELED, ls_->tablet_freeze_task(0, failed_tablets, false, true,
      ObClockGenerator::getClock() + 1_s, 0));
}

class WaitingMemtable : public memtable::ObMemtable
{
public:
  bool ready_for_flush() override
  {
    if (!entered_.exchange(true)) {
      waiting_.set_value();
    }
    return false;
  }
  std::atomic<bool> entered_{false};
  std::promise<void> waiting_;
};

TEST_F(TestFreezerMigrateFail, gc_disables_replay_after_six_freeze_readers_exit)
{
  logservice::ObLogReplayService replay_service;
  ASSERT_EQ(OB_SUCCESS, replay_service.replay_status_map_.init("FreezeTest", OB_SERVER_TENANT_ID));
  void *status_buffer = share::mtl_malloc(sizeof(logservice::ObReplayStatus), "FreezeTest");
  ASSERT_NE(nullptr, status_buffer);
  auto *status = new (status_buffer) logservice::ObReplayStatus();
  status->ls_id_ = ls_->get_ls_id();
  status->is_enabled_ = true;
  status->inc_ref();
  ASSERT_EQ(OB_SUCCESS, replay_service.replay_status_map_.insert_and_get(palf::LSKey(1001), status));
  replay_service.replay_status_map_.revert(status);
  replay_service.is_inited_ = true;
  ls_->log_handler_.is_inited_ = true;
  // Role lookup remains unavailable without PALF; replay uses the real service.
  ls_->log_handler_.is_in_stop_state_ = true;
  ls_->log_handler_.id_ = ls_->get_ls_id().id();
  ls_->log_handler_.replay_service_ = &replay_service;
  ls_->running_state_.state_ = ObLSRunningState::State::LS_RUNNING;
  ASSERT_TRUE(ls_->log_handler_.is_replay_enabled());

  ObFreezer &freezer = ls_->ls_freezer_;
  ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag_without_inc_freeze_clock());
  ObArenaAllocator allocator;
  void *buffer = allocator.alloc(sizeof(WaitingMemtable));
  ASSERT_NE(nullptr, buffer);
  auto *memtable = new (buffer) WaitingMemtable();
  memtable->key_.table_type_ = ObITable::DATA_MEMTABLE;
  memtable->key_.tablet_id_ = LS_MDS_TABLET;
  auto waiting = memtable->waiting_.get_future();
  ObTableHandleV2 handle;
  handle.table_ = memtable;
  handle.table_type_ = ObITable::DATA_MEMTABLE;
  handle.allocator_ = &allocator;
  memtable->inc_ref();
  ObSEArray<ObTableHandleV2, 1> handles;
  ObSEArray<ObTabletID, 1> failed_tablets;
  ASSERT_EQ(OB_SUCCESS, handles.push_back(handle));
  auto owner = std::async(std::launch::async, [&]() {
    return freezer.wait_tablet_freeze_finish(handles, failed_tablets);
  });
  EXPECT_EQ(std::future_status::ready, waiting.wait_for(std::chrono::seconds(1)));

  std::vector<std::future<int>> readers;
  for (int i = 0; i < 6; ++i) {
    std::promise<void> locked;
    auto ready = locked.get_future();
    readers.push_back(std::async(std::launch::async, [&, locked = std::move(locked)]() mutable {
      ObLSLockGuard guard(ls_.get(), true);
      locked.set_value();
      return freezer.loop_set_freeze_flag(1_s);
    }));
    ready.wait();
  }
  std::promise<void> allow_offline;
  auto allowed = allow_offline.get_future();
  std::promise<void> writer_locked;
  auto writer_ready = writer_locked.get_future();
  auto gc = std::async(std::launch::async, [&]() {
    ObLSLockGuard guard(ls_.get());
    writer_locked.set_value();
    allowed.wait();
    return ls_->log_handler_.disable_replay();
  });
  EXPECT_EQ(std::future_status::timeout, writer_ready.wait_for(std::chrono::milliseconds(20)));
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  for (auto &reader : readers) {
    EXPECT_EQ(OB_CANCELED, reader.get());
  }
  EXPECT_EQ(std::future_status::ready, writer_ready.wait_for(std::chrono::seconds(1)));
  EXPECT_EQ(std::future_status::timeout, owner.wait_for(std::chrono::milliseconds(20)));
  EXPECT_TRUE(freezer.is_freeze(ATOMIC_LOAD(&freezer.freeze_flag_)));
  allow_offline.set_value();
  EXPECT_EQ(OB_SUCCESS, gc.get());
  EXPECT_EQ(std::future_status::ready, owner.wait_for(std::chrono::seconds(1)));
  EXPECT_EQ(OB_NOT_RUNNING, owner.get());
  EXPECT_FALSE(freezer.is_freeze(freezer.freeze_flag_));
  EXPECT_EQ(1, failed_tablets.count());
  ls_->log_handler_.is_inited_ = false;
  ls_->log_handler_.replay_service_ = nullptr;
}

TEST_F(TestFreezerMigrateFail, migration_failure_keeps_snapshot_until_checkpoint_finishes)
{
  ObFreezer &freezer = ls_->ls_freezer_;
  ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag_without_inc_freeze_clock());
  freezer.freeze_snapshot_version_ = SCN::min_scn();
  ls_->data_checkpoint_.set_ls_freeze_finished_(false);
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  auto owner = std::async(std::launch::async, [&]() { return freezer.wait_ls_freeze_finish(); });
  EXPECT_EQ(std::future_status::timeout, owner.wait_for(std::chrono::milliseconds(50)));
  EXPECT_TRUE(freezer.is_freeze(ATOMIC_LOAD(&freezer.freeze_flag_)));
  EXPECT_EQ(SCN::min_scn(), freezer.get_freeze_snapshot_version());
  ls_->data_checkpoint_.set_ls_freeze_finished_(true);
  EXPECT_EQ(OB_SUCCESS, owner.get());
  EXPECT_FALSE(freezer.is_freeze(freezer.freeze_flag_));
  EXPECT_EQ(SCN::invalid_scn(), freezer.get_freeze_snapshot_version());
}

TEST_F(TestFreezerMigrateFail, canceled_checkpoint_submission_keeps_published_snapshot)
{
  ObFreezer &freezer = ls_->ls_freezer_;
  const ObMigrationStatus statuses[] = {
      ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL,
      ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE_FAIL};
  ls_->running_state_.state_ = ObLSRunningState::State::LS_RUNNING;
  freezer.online();
  for (const auto status : statuses) {
    set_status(status);
    ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag());
    const uint32_t freeze_clock = freezer.get_freeze_clock();
    freezer.freeze_snapshot_version_ = SCN::min_scn();
    freezer.set_need_resubmit_log(true);
    ObLSLockGuard guard(ls_.get(), true);
    EXPECT_EQ(OB_CANCELED, freezer.submit_checkpoint_task());
    EXPECT_FALSE(freezer.is_freeze(freezer.get_freeze_flag()));
    EXPECT_FALSE(freezer.need_resubmit_log());
    EXPECT_EQ(freeze_clock, freezer.get_freeze_clock());
    EXPECT_EQ(SCN::min_scn(), freezer.get_freeze_snapshot_version());
    EXPECT_TRUE(ls_->data_checkpoint_.ls_freeze_finished());
    // A memtable that observed the published clock can still consume its snapshot.
    memtable::ObMemtable memtable;
    memtable.freezer_ = &freezer;
    memtable.is_inited_ = true;
    EXPECT_EQ(OB_SUCCESS, memtable.resolve_snapshot_version_());
    EXPECT_EQ(SCN::min_scn(), memtable.snapshot_version_);
    memtable.is_inited_ = false;
  }
}

TEST_F(TestFreezerMigrateFail, checkpoint_submission_rejects_offline_and_stopped_ls)
{
  ObFreezer &freezer = ls_->ls_freezer_;
  const int64_t states[] = {ObLSRunningState::State::LS_OFFLINED,
                           ObLSRunningState::State::LS_STOPPED,
                           ObLSRunningState::State::LS_RUNNING};
  for (const auto state : states) {
    ls_->running_state_.state_ = state;
    freezer.enable_ = ObLSRunningState::State::LS_RUNNING != state;
    ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag());
    freezer.freeze_snapshot_version_ = SCN::min_scn();
    ObLSLockGuard guard(ls_.get(), true);
    EXPECT_EQ(OB_NOT_RUNNING, freezer.submit_checkpoint_task());
    EXPECT_FALSE(freezer.is_freeze(freezer.get_freeze_flag()));
    EXPECT_EQ(SCN::min_scn(), freezer.get_freeze_snapshot_version());
    EXPECT_TRUE(ls_->data_checkpoint_.ls_freeze_finished());
  }
}

TEST_F(TestFreezerMigrateFail, queue_pressure_retry_exits_on_migration_failure_and_unblocks_gc)
{
  ObTenantBase tenant(1002);
  checkpoint::ObCheckPointService checkpoint_service;
  tenant.set(&checkpoint_service);
  // No available task slots: exercise the real add_task -> OB_EAGAIN path.
  ASSERT_EQ(-1, checkpoint_service.freeze_thread_.available_index_);
  ObFreezer &freezer = ls_->ls_freezer_;
  ls_->running_state_.state_ = ObLSRunningState::State::LS_RUNNING;
  freezer.online();
  ASSERT_EQ(OB_SUCCESS, freezer.set_freeze_flag());
  freezer.freeze_snapshot_version_ = SCN::min_scn();
  std::promise<void> queue_full;
  auto retry_ready = queue_full.get_future();
  auto submitter = std::async(std::launch::async, [&]() {
    ObTenantEnv::set_tenant(&tenant);
    ObLSLockGuard guard(ls_.get(), true);
    EXPECT_EQ(OB_EAGAIN, ls_->data_checkpoint_.ls_freeze(SCN::max_scn()));
    queue_full.set_value();
    const int ret = freezer.submit_checkpoint_task();
    ObTenantEnv::set_tenant(nullptr);
    return ret;
  });
  retry_ready.wait();
  auto gc = std::async(std::launch::async, [&]() {
    ObLSLockGuard guard(ls_.get());
    return guard.locked();
  });
  EXPECT_EQ(std::future_status::timeout, submitter.wait_for(std::chrono::milliseconds(20)));
  EXPECT_EQ(std::future_status::timeout, gc.wait_for(std::chrono::milliseconds(20)));
  set_status(ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL);
  EXPECT_EQ(std::future_status::ready, submitter.wait_for(std::chrono::seconds(1)));
  EXPECT_EQ(OB_CANCELED, submitter.get());
  EXPECT_EQ(std::future_status::ready, gc.wait_for(std::chrono::seconds(1)));
  EXPECT_TRUE(gc.get());
  EXPECT_TRUE(ls_->data_checkpoint_.ls_freeze_finished());
  EXPECT_FALSE(freezer.is_freeze(freezer.get_freeze_flag()));
  EXPECT_EQ(SCN::min_scn(), freezer.get_freeze_snapshot_version());
}

} // namespace unittest
} // namespace oceanbase
