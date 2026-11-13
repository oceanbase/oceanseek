// owner: miaoyixuan.myx
// owner group: storage
/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <memory>

#define private public
#define protected public
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace storage
{
// Inject getter failures and replace storage I/O; both admission functions stay real.
struct ReplayReadDependencies
{
  int scan_count_ = 0;
  int fetch_count_ = 0;
  int restore_status_ret_ = OB_SUCCESS;
  int migration_status_ret_ = OB_SUCCESS;
  int restore_status_count_ = 0;
  int migration_status_count_ = 0;
};
static ReplayReadDependencies deps;

// Successful getter calls expose the valid fixture meta. Failures are controlled
// independently so migration can fail after restore has successfully returned NONE.
int ObLSMeta::get_restore_status(ObLSRestoreStatus &restore_status) const
{
  ++deps.restore_status_count_;
  if (OB_SUCCESS == deps.restore_status_ret_) {
    restore_status = restore_status_;
  }
  return deps.restore_status_ret_;
}

int ObLSMeta::get_migration_status(ObMigrationStatus &migration_status) const
{
  ++deps.migration_status_count_;
  if (OB_SUCCESS == deps.migration_status_ret_) {
    migration_status = migration_status_;
  }
  return deps.migration_status_ret_;
}

int ObLSTabletService::inner_table_scan(
    ObTabletHandle &, ObTableScanIterator &, ObTableScanParam &)
{
  ++deps.scan_count_;
  return OB_ENTRY_NOT_EXIST;
}

int ObLSTabletService::prepare_scan_table_param(
    ObTableScanParam &, schema::ObMultiVersionSchemaService &)
{
  return OB_SUCCESS;
}

int ObLSTabletService::get_tablet_with_timeout(
    const ObTabletID &, ObTabletHandle &, const int64_t,
    const ObMDSGetTabletMode, const SCN &)
{
  ++deps.fetch_count_;
  return OB_ENTRY_NOT_EXIST;
}

class TestMigrationReplayRead : public ::testing::Test
{
protected:
  void SetUp() override
  {
    deps = ReplayReadDependencies();
    ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
    tenant_.reset(new ObTenantBase(1002));
    ObTenantEnv::set_tenant(tenant_.get());
    schema_service_.reset(new schema::ObMultiVersionSchemaService());
    tenant_schema_service_.reset(new schema::ObTenantSchemaService());
    tenant_schema_service_->schema_service_ = schema_service_.get();
    tenant_->set(tenant_schema_service_.get());
    // set_tenant copies module pointers into the thread-local tenant view.
    ObTenantEnv::set_tenant(tenant_.get());
    ASSERT_EQ(tenant_schema_service_.get(), MTL(schema::ObTenantSchemaService *));
    ls_.reset(new ObLS());
    ls_->ls_meta_.tenant_id_ = 1002;
    ls_->ls_meta_.ls_id_ = ObLSID(1001);
    ls_->ls_meta_.restore_status_ = ObLSRestoreStatus::NONE;
    ls_->ls_meta_.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_ADD;
    ls_->ls_meta_.replica_type_ = REPLICA_TYPE_FULL;
    ls_->ls_meta_.gc_state_ = logservice::LSGCState::NORMAL;
    ls_->ls_meta_.rebuild_seq_ = 0;
    ASSERT_TRUE(ls_->ls_meta_.is_valid());
    service_ = ls_->get_tablet_svr();
    service_->ls_ = ls_.get();
    service_->is_inited_ = true;
    service_->disable_to_read();
    ASSERT_FALSE(ls_->is_running());
    ASSERT_EQ(OB_REPLICA_NOT_READABLE, service_->check_allow_to_read());
  }

  void TearDown() override
  {
    // Only the LS state machine was exercised, without initializing storage.
    ls_->running_state_.state_ = ObLSRunningState::State::LS_INIT;
    service_->is_inited_ = false;
    service_->ls_ = nullptr;
    ls_.reset();
    tenant_schema_service_->schema_service_ = nullptr;
    tenant_schema_service_.reset();
    schema_service_.reset();
    ObTenantEnv::set_tenant(nullptr);
    tenant_.reset();
    ObClockGenerator::destroy();
    deps = ReplayReadDependencies();
  }

  void expect_read_result(
      const ObTabletID &tablet_id,
      const int expected_ret,
      const int expected_fetches,
      const int expected_scans)
  {
    SCOPED_TRACE(tablet_id.id());
    const int normal_read_ret = service_->check_allow_to_read();
    // Check the returned error independently from whether storage I/O is attempted.
    const int scans_before = deps.scan_count_;
    const int fetches_before = deps.fetch_count_;
    ObAccessService access;
    ObStoreCtxGuard ctx;
    ObTabletHandle fetched;
    EXPECT_EQ(expected_ret, access.construct_store_ctx_other_variables_(
        *ls_, tablet_id, ObTimeUtility::current_time() + 1_s, SCN::max_scn(),
        fetched, ctx, schema::ObTableType::USER_TABLE));
    EXPECT_EQ(fetches_before + expected_fetches, deps.fetch_count_);
    EXPECT_EQ(scans_before, deps.scan_count_);

    ObTablet tablet;
    tablet.tablet_meta_.tablet_id_ = tablet_id;
    ObTabletHandle handle;
    handle.obj_ = &tablet;
    ObTableScanIterator iter;
    ObTableScanParam param;
    const int scan_ret = service_->table_scan(handle, iter, param);
    handle.obj_ = nullptr;
    EXPECT_EQ(expected_ret, scan_ret);
    EXPECT_EQ(scans_before + expected_scans, deps.scan_count_);
    EXPECT_EQ(fetches_before + expected_fetches, deps.fetch_count_);
    EXPECT_EQ(normal_read_ret, service_->check_allow_to_read());
  }

  void expect_admission(const ObTabletID &tablet_id, const bool admitted)
  {
    expect_read_result(tablet_id,
        admitted ? OB_ENTRY_NOT_EXIST : OB_REPLICA_NOT_READABLE,
        admitted ? 1 : 0, admitted ? 1 : 0);
  }

  std::unique_ptr<ObTenantBase> tenant_;
  std::unique_ptr<schema::ObMultiVersionSchemaService> schema_service_;
  std::unique_ptr<schema::ObTenantSchemaService> tenant_schema_service_;
  std::unique_ptr<ObLS> ls_;
  ObLSTabletService *service_ = nullptr;
};

TEST_F(TestMigrationReplayRead, restore_migration_and_online_state_admission)
{
  const ObLSRestoreStatus::Status restore_states[] = {
      ObLSRestoreStatus::NONE, ObLSRestoreStatus::SS_RESTORE_CLOG,
      ObLSRestoreStatus::SS_RESTORE_START, ObLSRestoreStatus::SS_RESTORE_LS,
      ObLSRestoreStatus::SS_RESTORE_WAIT_LS, ObLSRestoreStatus::RESTORE_FAILED,
      ObLSRestoreStatus::CLONE_START, ObLSRestoreStatus::QUICK_RESTORE_FINISH};
  for (const ObLSRestoreStatus::Status restore_state : restore_states) {
    ls_->ls_meta_.restore_status_ = ObLSRestoreStatus(restore_state);
    SCOPED_TRACE(static_cast<int>(restore_state));
    for (int migration = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
         migration < ObMigrationStatus::OB_MIGRATION_STATUS_MAX; ++migration) {
      ls_->ls_meta_.migration_status_ = static_cast<ObMigrationStatus>(migration);
      SCOPED_TRACE(migration);
      for (int64_t running = ObLSRunningState::State::LS_INIT;
           running < ObLSRunningState::State::MAX; ++running) {
        ls_->running_state_.state_ = running;
        SCOPED_TRACE(running);
        const bool migrating = migration == ObMigrationStatus::OB_MIGRATION_STATUS_ADD
            || migration == ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE
            || migration == ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE;
        const bool admitted = restore_state == ObLSRestoreStatus::SS_RESTORE_CLOG
            || (restore_state == ObLSRestoreStatus::NONE && migrating
                && running == ObLSRunningState::State::LS_RUNNING);
        expect_admission(LS_REORG_INFO_TABLET, admitted);
        expect_admission(ObTabletID(200001), false);
        expect_admission(LS_TX_DATA_TABLET, false);
      }
    }
  }
}

TEST_F(TestMigrationReplayRead, online_and_offline_transitions_control_reorg_reads)
{
  for (const ObMigrationStatus migration : {
           ObMigrationStatus::OB_MIGRATION_STATUS_ADD,
           ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE,
           ObMigrationStatus::OB_MIGRATION_STATUS_REPLACE}) {
    SCOPED_TRACE(static_cast<int>(migration));
    ls_->ls_meta_.migration_status_ = migration;
    ls_->running_state_.state_ = ObLSRunningState::State::LS_INIT;
    expect_admission(LS_REORG_INFO_TABLET, false);
    ASSERT_EQ(OB_SUCCESS, ls_->running_state_.create_finish(ls_->get_ls_id()));
    expect_admission(LS_REORG_INFO_TABLET, false);
    ASSERT_EQ(OB_SUCCESS, ls_->running_state_.online(ls_->get_ls_id()));
    expect_admission(LS_REORG_INFO_TABLET, true);
    ASSERT_EQ(OB_SUCCESS, ls_->running_state_.pre_offline(ls_->get_ls_id()));
    expect_admission(LS_REORG_INFO_TABLET, false);
    ASSERT_EQ(OB_SUCCESS, ls_->running_state_.post_offline(ls_->get_ls_id()));
    expect_admission(LS_REORG_INFO_TABLET, false);
  }
}

TEST_F(TestMigrationReplayRead, restore_getter_error_is_propagated_without_io)
{
  ASSERT_TRUE(ls_->ls_meta_.is_valid());
  ASSERT_EQ(OB_REPLICA_NOT_READABLE, service_->check_allow_to_read());
  deps.restore_status_ret_ = OB_ERR_UNEXPECTED;

  expect_read_result(LS_REORG_INFO_TABLET, OB_ERR_UNEXPECTED, 0, 0);

  EXPECT_EQ(2, deps.restore_status_count_);
  EXPECT_EQ(0, deps.migration_status_count_);
  EXPECT_TRUE(ls_->ls_meta_.is_valid());
}

TEST_F(TestMigrationReplayRead, migration_getter_error_is_propagated_without_io)
{
  ASSERT_EQ(OB_SUCCESS, ls_->running_state_.create_finish(ls_->get_ls_id()));
  ASSERT_EQ(OB_SUCCESS, ls_->running_state_.online(ls_->get_ls_id()));
  ASSERT_TRUE(ls_->is_running());
  ASSERT_TRUE(ls_->ls_meta_.is_valid());
  ASSERT_EQ(OB_REPLICA_NOT_READABLE, service_->check_allow_to_read());
  ObLSRestoreStatus restore_status;
  ASSERT_EQ(OB_SUCCESS, ls_->get_restore_status(restore_status));
  ASSERT_TRUE(restore_status.is_none());
  deps.restore_status_count_ = 0;
  deps.migration_status_ret_ = OB_ERR_UNEXPECTED;

  expect_read_result(LS_REORG_INFO_TABLET, OB_ERR_UNEXPECTED, 0, 0);

  EXPECT_EQ(2, deps.restore_status_count_);
  EXPECT_EQ(2, deps.migration_status_count_);
  EXPECT_TRUE(ls_->ls_meta_.is_valid());
}

TEST_F(TestMigrationReplayRead, normal_read_admission_is_preserved)
{
  ls_->ls_meta_.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  ls_->running_state_.state_ = ObLSRunningState::State::LS_RUNNING;
  service_->enable_to_read();
  ASSERT_EQ(OB_SUCCESS, service_->check_allow_to_read());
  expect_admission(LS_REORG_INFO_TABLET, true);
  expect_admission(ObTabletID(200001), true);
  expect_admission(LS_TX_DATA_TABLET, true);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_migration_replay_read.log");
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
