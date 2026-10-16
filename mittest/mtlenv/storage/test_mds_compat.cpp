// owner: gaishun.gs
// owner group: storage

/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define protected public
#define private public

#include "mtlenv/storage/medium_info_helper.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/tx_storage/ob_ls_service.h"


#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
namespace {
bool ut_has_mds_memtable(const ObTablet *tablet)
{
  ObSEArray<ObITable *, 8> memtables;
  return OB_NOT_NULL(tablet)
      && OB_SUCCESS == tablet->inner_get_memtables(memtables)
      && memtables.count() > 0;
}
} // namespace
class TestMdsCompat : public::testing::Test
{
public:
  TestMdsCompat() = default;
  virtual ~TestMdsCompat() = default;
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_ls_mds_tablet(ObTabletHandle &tablet_handle);
  static int wait_for_mds_table_flush(const common::ObTabletID &tablet_id);
  static int wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id);
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;
public:
  common::ObArenaAllocator allocator_;
};

const share::ObLSID TestMdsCompat::LS_ID(1234);

void TestMdsCompat::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerStorageMetaService::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMdsCompat::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMdsCompat::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMdsCompat::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestMdsCompat::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      ObTabletStatus::MAX, share::SCN::invalid_scn(), tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret));
  }

  return ret;
}

int TestMdsCompat::get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

int TestMdsCompat::get_ls_mds_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_MDS_TABLET, tablet_handle))) {
    LOG_WARN("failed to get ls mds tablet", K(ret));
  }
  return ret;
}

int TestMdsCompat::wait_for_mds_table_flush(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tablet_id);

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  ret = TestMdsCompat::get_ls_mds_tablet(tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  do {
    ret = TestMdsCompat::get_ls_mds_tablet(tablet_handle);
    EXPECT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    EXPECT_NE(nullptr, tablet);
    if (ut_has_mds_memtable(tablet)) {
      break;
    }
    ::ob_usleep(100_ms);
    ++times;
  } while (times < 50);

  ret = TestMdsCompat::get_ls_mds_tablet(tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);
  EXPECT_TRUE(ut_has_mds_memtable(tablet));

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}

int TestMdsCompat::wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id)
{
  UNUSED(tablet_id);
  for (int times = 0; times < 60; ++times) {
    ::ob_usleep(100_ms);
  }
  return OB_SUCCESS;
}

TEST_F(TestMdsCompat, migration_param)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 50);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);
  // write data to mds table via replay (LS aggregate MDS)
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(100);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(777)));
  }

  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(200);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 220);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 200);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(888)));
  }

  {
    ObLSHandle ls_handle;
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 280);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_ddl_info_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(1000)));
  }

  {
    ObLSHandle ls_handle;
    share::ObTabletAutoincSeq user_data;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 320);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_dummy_key_mds_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(1200)));
  }

  // flush LS aggregate MDS (LS_MDS_TABLET)
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestMdsCompat::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ObMigrationTabletParam param;
  ret = tablet->build_migration_tablet_param(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(param.is_valid());

  const int64_t serialize_size = param.get_serialize_size();
  char *const ser_buf = static_cast<char *>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t ser_pos = 0;
  ret = param.serialize(ser_buf, serialize_size, ser_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(serialize_size, ser_pos);

  ObMigrationTabletParam param2;
  ser_pos = 0;
  ret = param2.deserialize(ser_buf, serialize_size, ser_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(serialize_size, ser_pos);
  ASSERT_TRUE(param2.is_valid());
}

TEST_F(TestMdsCompat, compat)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 80);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(100);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(777)));
  }

}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_compat.log*");
  OB_LOGGER.set_file_name("test_mds_compat.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
