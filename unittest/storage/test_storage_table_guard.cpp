/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/test_tablet_helper.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/ob_relative_table.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
namespace storage
{

class TestStorageTableGuard : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    saved_timeout_ = THIS_WORKER.get_timeout_ts();
    static int64_t next_ls_id = 2200;
    ls_id_ = share::ObLSID(next_ls_id++);
    ObCreateLSArg arg;
    share::schema::ObTableSchema schema;
    ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(1, ls_id_, arg));
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id_, ls_handle_, ObLSGetMod::STORAGE_MOD));
    saved_epoch_ = ls_handle_.get_ls()->get_switch_epoch();
    ASSERT_EQ(OB_SUCCESS, build_test_schema(schema, 12345));
    ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
        ls_handle_, common::ObTabletID(500000), schema, allocator_,
        ObTabletStatus::NORMAL, share::SCN::base_scn(), tablet_handle_));
    store_ctx_.ls_ = ls_handle_.get_ls();
    store_ctx_.ls_id_ = ls_id_;
    THIS_WORKER.set_timeout_ts(common::ObTimeUtility::current_time() + 5 * 1000 * 1000);
  }

  void TearDown() override
  {
    THIS_WORKER.set_timeout_ts(saved_timeout_);
    if (ls_handle_.is_valid()) {
      ATOMIC_STORE(&ls_handle_.get_ls()->switch_epoch_, saved_epoch_);
    }
    store_ctx_.reset();
    tablet_handle_.reset();
    ls_handle_.reset();
    EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->remove_ls(ls_id_));
  }

  void make_ls_offline()
  {
    // Model the epoch transition at the start of offline_(), while the old
    // tablet and transaction references still keep the LS alive.
    ATOMIC_STORE(&ls_handle_.get_ls()->switch_epoch_, saved_epoch_ & ~uint64_t(1));
  }

  void create_memtable(ObTableHandleV2 &handle)
  {
    CreateMemtableArg arg;
    arg.schema_version_ = 1;
    arg.clog_checkpoint_scn_ = share::SCN::base_scn();
    ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->create_memtable(arg));
    ObProtectedMemtableMgrHandle *mgr = nullptr;
    ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->get_protected_memtable_mgr_handle(mgr));
    ASSERT_EQ(OB_SUCCESS, mgr->get_active_memtable(handle));
  }

  common::ObArenaAllocator allocator_;
  share::ObLSID ls_id_;
  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_;
  ObStoreCtx store_ctx_;
  uint64_t saved_epoch_ = 0;
  int64_t saved_timeout_ = 0;
};

TEST_F(TestStorageTableGuard, offline_without_memtable)
{
  ObRelativeTable relative_table;
  make_ls_offline();
  ObStorageTableGuard guard(tablet_handle_.get_obj(), store_ctx_, false);
  bool need_refresh = true;
  bool pinned = true;
  ASSERT_EQ(OB_LS_OFFLINE, guard.need_to_refresh_table(
      *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
  ASSERT_FALSE(need_refresh);
  ASSERT_FALSE(pinned);
  // The write must return the original error, not success or a later timeout.
  ASSERT_EQ(OB_LS_OFFLINE, guard.refresh_and_protect_memtable_for_write(relative_table));
}

TEST_F(TestStorageTableGuard, offline_with_frozen_memtable)
{
  ObTableHandleV2 handle;
  create_memtable(handle);
  ASSERT_FALSE(HasFatalFailure());
  memtable::ObMemtable *mt = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_data_memtable(mt));
  mt->set_is_tablet_freeze();
  ASSERT_FALSE(mt->is_active_memtable());
  ASSERT_EQ(0, mt->get_write_ref());
  ObRelativeTable relative_table;
  ASSERT_EQ(OB_SUCCESS, relative_table.tablet_iter_.table_iter()->add_table(mt));
  make_ls_offline();
  ObStorageTableGuard guard(tablet_handle_.get_obj(), store_ctx_, false);
  bool need_refresh = true;
  bool pinned = false;
  ASSERT_EQ(OB_LS_OFFLINE, guard.need_to_refresh_table(
      *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
  ASSERT_FALSE(need_refresh);
  ASSERT_EQ(OB_LS_OFFLINE, guard.refresh_and_protect_memtable_for_write(relative_table));
  ASSERT_EQ(0, mt->get_write_ref());
}

TEST_F(TestStorageTableGuard, active_memtable_write_reference)
{
  ObTableHandleV2 handle;
  create_memtable(handle);
  ASSERT_FALSE(HasFatalFailure());
  memtable::ObMemtable *mt = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_data_memtable(mt));
  ObRelativeTable relative_table;
  ASSERT_EQ(OB_SUCCESS, relative_table.tablet_iter_.table_iter()->add_table(mt));
  const int64_t old_write_ref = mt->get_write_ref();
  {
    ObStorageTableGuard guard(tablet_handle_.get_obj(), store_ctx_, false);
    ASSERT_EQ(OB_SUCCESS, guard.refresh_and_protect_memtable_for_write(relative_table));
    ASSERT_EQ(old_write_ref + 1, mt->get_write_ref());
  }
  ASSERT_EQ(old_write_ref, mt->get_write_ref());
}

TEST_F(TestStorageTableGuard, create_eagain_keeps_refresh_decision_and_timeout)
{
  // A committed, non-writable tablet status rejects memtable creation with
  // OB_EAGAIN. HA data completeness does not gate tablet status MDS reads.
  const common::ObTabletID tablet_id(500001);
  share::schema::ObTableSchema schema;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(schema, 12345));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
      ls_handle_, tablet_id, schema, allocator_,
      ObTabletStatus::DELETED, share::SCN::base_scn(), tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_FALSE(tablet->is_empty_shell());
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_state;
  share::SCN trans_version;
  ASSERT_EQ(OB_SUCCESS, tablet->ObITabletMdsInterface::get_latest_tablet_status(
      user_data, writer, trans_state, trans_version));
  ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_state);
  ASSERT_EQ(ObTabletStatus::DELETED, user_data.tablet_status_);
  CreateMemtableArg arg;
  ASSERT_EQ(OB_EAGAIN, ls_handle_.get_ls()->get_tablet_svr()->create_memtable(tablet_id, arg));

  ObRelativeTable relative_table;
  ObStorageTableGuard guard(tablet, store_ctx_, false);
  bool need_refresh = false;
  bool pinned = false;
  ASSERT_EQ(OB_SUCCESS, guard.need_to_refresh_table(
      *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
  ASSERT_TRUE(need_refresh);
  ASSERT_FALSE(pinned);
  // Check the refresh decision and the expired-deadline exit. This does not
  // exercise a successful refresh or multiple iterations of the public loop.
  THIS_WORKER.set_timeout_ts(common::ObTimeUtility::current_time() - 1);
  ASSERT_EQ(OB_TIMEOUT, guard.refresh_and_protect_memtable_for_write(relative_table));
}

TEST_F(TestStorageTableGuard, other_create_error_keeps_refresh_retry)
{
  ObRelativeTable relative_table;
  ObStorageTableGuard guard(tablet_handle_.get_obj(), store_ctx_, false);
  ObLSTabletService *tablet_service = ls_handle_.get_ls()->get_tablet_svr();
  const bool saved_inited = tablet_service->is_inited_;
  tablet_service->is_inited_ = false;
  CreateMemtableArg arg;
  EXPECT_EQ(OB_NOT_INIT, tablet_service->create_memtable(common::ObTabletID(500000), arg));
  bool need_refresh = false;
  bool pinned = false;
  // A non-EAGAIN creation error must retain the original refresh decision.
  EXPECT_EQ(OB_SUCCESS, guard.need_to_refresh_table(
      *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
  EXPECT_TRUE(need_refresh);
  THIS_WORKER.set_timeout_ts(common::ObTimeUtility::current_time() - 1);
  EXPECT_EQ(OB_TIMEOUT, guard.refresh_and_protect_memtable_for_write(relative_table));
  tablet_service->is_inited_ = saved_inited;
}

TEST_F(TestStorageTableGuard, other_frozen_memtable_error_keeps_refresh_retry)
{
  ObTableHandleV2 handle;
  create_memtable(handle);
  ASSERT_FALSE(HasFatalFailure());
  memtable::ObMemtable *mt = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_data_memtable(mt));
  mt->set_is_tablet_freeze();
  ObRelativeTable relative_table;
  ASSERT_EQ(OB_SUCCESS, relative_table.tablet_iter_.table_iter()->add_table(mt));
  ObStorageTableGuard guard(tablet_handle_.get_obj(), store_ctx_, false);
  ObLSTabletService *tablet_service = ls_handle_.get_ls()->get_tablet_svr();
  const bool saved_inited = tablet_service->is_inited_;
  tablet_service->is_inited_ = false;
  bool need_refresh = false;
  bool pinned = false;
  EXPECT_EQ(OB_NOT_INIT, guard.check_freeze_to_inc_write_ref(mt, need_refresh, pinned));
  EXPECT_EQ(OB_SUCCESS, guard.need_to_refresh_table(
      *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
  EXPECT_TRUE(need_refresh);
  THIS_WORKER.set_timeout_ts(common::ObTimeUtility::current_time() - 1);
  EXPECT_EQ(OB_TIMEOUT, guard.refresh_and_protect_memtable_for_write(relative_table));
  EXPECT_EQ(0, mt->get_write_ref());
  tablet_service->is_inited_ = saved_inited;
  mt->clear_is_tablet_freeze();
}

TEST_F(TestStorageTableGuard, pinned_frozen_mds_returns_eagain)
{
  ObTabletHandle mds_tablet;
  ASSERT_EQ(OB_SUCCESS, ls_handle_.get_ls()->get_tablet_svr()->get_tablet(
      common::LS_MDS_TABLET, mds_tablet));
  ObProtectedMemtableMgrHandle *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mds_tablet.get_obj()->get_protected_memtable_mgr_handle(mgr));
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, mgr->get_active_memtable(handle));
  memtable::ObMemtable *mt = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_data_memtable(mt));
  ObRelativeTable relative_table;
  ASSERT_EQ(OB_SUCCESS, relative_table.tablet_iter_.table_iter()->add_table(mt));
  mt->set_is_tablet_freeze();
  mt->inc_mds_pin_cnt();
  {
    ObStorageTableGuard guard(mds_tablet.get_obj(), store_ctx_, false);
    bool need_refresh = false;
    bool pinned = false;
    EXPECT_EQ(OB_SUCCESS, guard.need_to_refresh_table(
        *relative_table.tablet_iter_.table_iter(), need_refresh, pinned));
    EXPECT_TRUE(need_refresh);
    EXPECT_TRUE(pinned);
    EXPECT_EQ(OB_EAGAIN, guard.refresh_and_protect_memtable_for_write(relative_table));
  }
  mt->dec_mds_pin_cnt();
  mt->clear_is_tablet_freeze();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_storage_table_guard.log", true);
  OB_LOGGER.set_log_level("INFO");
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
