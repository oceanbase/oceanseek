// owner: gaishun.gs
// owner group: storage

/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/compaction/ob_schedule_status_cache.h"

using namespace oceanbase::common;

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
class TestTabletStatusCache : public::testing::Test
{
public:
  TestTabletStatusCache();
  virtual ~TestTabletStatusCache() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      const ObTabletStatus::Status &tablet_status = ObTabletStatus::NORMAL,
      const share::SCN &create_commit_scn = share::SCN::min_scn());
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;

  common::ObArenaAllocator allocator_;
};

TestTabletStatusCache::TestTabletStatusCache()
  : allocator_()
{
}

const share::ObLSID TestTabletStatusCache::LS_ID(1001);

void TestTabletStatusCache::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  SERVER_STORAGE_META_SERVICE.is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletStatusCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

void TestTabletStatusCache::SetUp()
{
}

void TestTabletStatusCache::TearDown()
{
}

int TestTabletStatusCache::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestTabletStatusCache::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestTabletStatusCache::create_tablet(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    const ObTabletStatus::Status &tablet_status,
    const share::SCN &create_commit_scn)
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
      tablet_status, create_commit_scn, tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret), K(LS_ID), K(tablet_id), K(create_commit_scn));
  }

  return ret;
}

TEST_F(TestTabletStatusCache, transfer_memtable_flush_status)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
  const ObTabletStatus::Status statuses[] = {
      ObTabletStatus::NORMAL,
      ObTabletStatus::TRANSFER_OUT,
      ObTabletStatus::TRANSFER_OUT_DELETED,
      ObTabletStatus::TRANSFER_IN,
      ObTabletStatus::DELETED};
  const int64_t base_tablet_id = ObTimeUtility::fast_current_time() % 10000000000000;
  for (int64_t i = 0; i < ARRAYSIZEOF(statuses); ++i) {
    SCOPED_TRACE(i);
    const ObTabletID tablet_id(base_tablet_id + i);
    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle, statuses[i]));
    memtable::ObMemtable memtable;
    memtable.ls_handle_ = ls_handle;
    memtable.ls_id_ = LS_ID;
    memtable.key_.table_type_ = ObITable::DATA_MEMTABLE;
    memtable.key_.tablet_id_ = tablet_id;
    memtable.key_.scn_range_.start_scn_.convert_for_tx(100);
    memtable.key_.scn_range_.end_scn_.convert_for_tx(100);
    const bool transfer_out = ObTabletStatus::TRANSFER_OUT == statuses[i]
        || ObTabletStatus::TRANSFER_OUT_DELETED == statuses[i];
    bool is_transfer_out = false;
    EXPECT_EQ(OB_SUCCESS, memtable.is_transfer_out_(is_transfer_out));
    EXPECT_EQ(transfer_out, is_transfer_out);
    ls_handle.get_ls()->disable_flush();
    EXPECT_EQ(transfer_out ? OB_SUCCESS : OB_EAGAIN, memtable.flush(LS_ID));
    ls_handle.get_ls()->enable_flush();
  }
  // Propagate the tablet lookup error without bypassing backpressure.
  memtable::ObMemtable missing_memtable;
  missing_memtable.ls_handle_ = ls_handle;
  missing_memtable.key_.tablet_id_ = ObTabletID(base_tablet_id + ARRAYSIZEOF(statuses));
  bool is_transfer_out = true;
  EXPECT_EQ(OB_TABLET_NOT_EXIST, missing_memtable.is_transfer_out_(is_transfer_out));
  EXPECT_FALSE(is_transfer_out);
  ls_handle.get_ls()->disable_flush();
  EXPECT_EQ(OB_TABLET_NOT_EXIST, missing_memtable.flush(LS_ID));
  ls_handle.get_ls()->enable_flush();
}

TEST_F(TestTabletStatusCache, weak_read)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  // create commit scn: 50
  share::SCN min_scn;
  min_scn.set_min();
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::NORMAL, share::SCN::plus(min_scn, 50));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, snapshot version is smaller than create commit version, return OB_SNAPSHOT_DISCARDED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 20/*snapshot*/);
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, but snapshot is not max scn, not supported
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 20/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_READABLE_COMMITED, snapshot version is bigger than create commit version, return OB_SUCCESS
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 60/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_TRUE(tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, fork_dst_cannot_schedule_new_medium_round)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  share::SCN min_scn;
  min_scn.set_min();
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::NORMAL, share::SCN::plus(min_scn, 50));
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObTabletCreateDeleteMdsUserData start_fork_dst;
  start_fork_dst.tablet_status_ = ObTabletStatus::FORK_DST;
  start_fork_dst.data_type_ = ObTabletMdsUserDataType::START_FORK_DST;
  mds::MdsCtx start_ctx{mds::MdsWriter(transaction::ObTransID(9012345))};
  ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(9012345), start_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet->set_tablet_status(start_fork_dst, start_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ut_finish_mds_tx_commit(start_ctx, ut_mds_write_env().mem_ctx, share::SCN::plus(min_scn, 100));
  ASSERT_EQ(OB_SUCCESS, ret);

  compaction::ObTenantStatusCache tenant_status;
  compaction::ObTabletStatusCache schedule_status(tenant_status);
  schedule_status.inner_init_could_schedule_new_round(LS_ID, *tablet, true, true);
  ASSERT_EQ(compaction::ObTabletStatusCache::DURING_FORK, schedule_status.get_new_round_state());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
  UtMdsTxScope finish_scope(*ls_handle.get_ls());
  ASSERT_EQ(OB_SUCCESS, finish_scope.begin(tablet_id));
  mds::MdsCtx finish_ctx{mds::MdsWriter(finish_scope.tx_desc()->tx_id_)};
  finish_scope.bind_to_mds_ctx(finish_ctx);
  ObTabletCreateDeleteMdsUserData finish_fork_dst;
  finish_fork_dst.tablet_status_ = ObTabletStatus::NORMAL;
  finish_fork_dst.data_type_ = ObTabletMdsUserDataType::FINISH_FORK_DST;
  ret = tablet->set_tablet_status(finish_fork_dst, finish_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);

  schedule_status.inner_init_could_schedule_new_round(LS_ID, *tablet, true, true);
  ASSERT_EQ(compaction::ObTabletStatusCache::DURING_FORK, schedule_status.get_new_round_state());

  ret = ut_finish_mds_tx_commit(finish_ctx, *finish_scope.mem_ctx(), share::SCN::plus(min_scn, 150));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schedule_status.init_for_major(*ls_handle.get_ls(), 2, *tablet, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, schedule_status.medium_list());
  ASSERT_EQ(compaction::ObTabletStatusCache::CAN_SCHEDULE_NEW_ROUND, schedule_status.get_new_round_state());
}

TEST_F(TestTabletStatusCache, get_transfer_out_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  share::SCN min_scn;
  min_scn.set_min();
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;

  mds::MdsCtx ctx1{mds::MdsWriter(transaction::ObTransID(123))};
  ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(123), ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet->set(user_data, ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, state is INIT, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet->tablet_status_cache_.is_valid());

  // mode is READ_READABLE_COMMITED, state is INIT
  // snapshot is not MAX, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(tablet->tablet_status_cache_.is_valid());

  // mode is READ_READABLE_COMMITED, state is INIT
  // snapshot is not MAX, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 110/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(tablet->tablet_status_cache_.is_valid());

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }
  const share::SCN redo_scn = share::SCN::plus(min_scn, 20);
  const share::SCN prepare_version = share::SCN::plus(min_scn, 30);
  UtMdsWriteEnv &env = ut_mds_write_env();
  ctx1.on_redo(redo_scn);
  ut_advance_mds_tx_before_prepare(ctx1, env.mem_ctx, redo_scn);
  ut_advance_mds_tx_on_prepare(ctx1, env.mem_ctx, prepare_version);

  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, state is ON_PREPARE, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get TRANSFER_OUT status tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  tablet_handle.reset();
  // read snapshot: 80. not max scn, return OB_NOT_SUPPORTED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // read snapshot: 80. less than transfer scn
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // check tablet status cache
  {
    SpinRLockGuard guard(tablet->mds_cache_lock_);
    const ObTabletStatusCache &tablet_status_cache = tablet->tablet_status_cache_;

    // we won't update tablet status cache if current status is TRANSFER_OUT,
    // so here the cache remains invalid
    ASSERT_EQ(ObTabletStatus::MAX, tablet_status_cache.tablet_status_);
  }

  // let transaction commit
  // commit scn: 120
  share::SCN commit_scn = share::SCN::plus(min_scn, 120);
  ret = ut_finish_mds_tx_commit_after_redo(ctx1, ut_mds_write_env().mem_ctx, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, read snapshot is max scn, greater than transfer scn 100, not allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  tablet_handle.reset();
  // mode is READ_READABLE_COMMITED, read snapshot 80 less than transfer scn 100, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, snapshot is not max scn, return OB_NOT_SUPPORTED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, 80/*snapshot*/);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  // Open a REAL ObPartTransCtx so reads on the uncommitted row don't trip
  // OB_TRANS_CTX_NOT_EXIST when the tx_state resolver looks up the tx.
  ObLSHandle ls_handle_for_tx;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle_for_tx, ObLSGetMod::STORAGE_MOD));
  UtMdsTxScope ctx2_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx2_scope.begin(tablet_id));
  mds::MdsCtx ctx2{mds::MdsWriter(ctx2_scope.tx_desc()->tx_id_)};
  ctx2_scope.bind_to_mds_ctx(ctx2);
  ret = tablet->set(user_data, ctx2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // mode is READ_READABLE_COMMITED, snpashot status is transfer out, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, allow to get tablet whose snapshot status is transfer out
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // transaction commit
  commit_scn = share::SCN::plus(min_scn, 220);
  ret = ut_finish_mds_tx_commit(ctx2, *ctx2_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mode is READ_READABLE_COMMITED, snapshot status is transfer out deleted, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // mode is READ_ALL_COMMITED, snapshot status is transfer out deleted, not allow to get
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::REMOVE_TABLET;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  UtMdsTxScope ctx3_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx3_scope.begin(tablet_id));
  mds::MdsCtx ctx3{mds::MdsWriter(ctx3_scope.tx_desc()->tx_id_)};
  ctx3_scope.bind_to_mds_ctx(ctx3);
  ret = tablet->set(user_data, ctx3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // transaction commit
  commit_scn = share::SCN::plus(min_scn, 200);
  ret = ut_finish_mds_tx_commit(ctx3, *ctx3_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  const uint64_t data_version = DATA_CURRENT_VERSION;
  ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(data_version, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
}

TEST_F(TestTabletStatusCache, get_transfer_deleted)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  // delete commit scn: 200
  // begin deleted transaction
  user_data.tablet_status_ = ObTabletStatus::DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::REMOVE_TABLET;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.delete_commit_scn_ = share::SCN::plus(min_scn, 200);
  user_data.delete_commit_version_ = 200;

  mds::MdsCtx ctx{mds::MdsWriter(transaction::ObTransID(789))};
  ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(789), ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN commit_scn = share::SCN::plus(min_scn, 120);
  ret = ut_finish_mds_tx_commit(ctx, ut_mds_write_env().mem_ctx, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  const uint64_t data_version = DATA_CURRENT_VERSION;
  ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(data_version, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_transfer_out_deleted)
{
  int ret = OB_SUCCESS;
  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  ObLSHandle ls_handle;
  ret = MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  const ObTabletMapKey key(LS_ID, tablet_id);
  tablet_handle.reset();
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(tablet->tablet_status_cache_.is_valid());

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  // set transfer scn
  // create commit scn: 50
  // transfer scn: 100
  // delete commit scn: 200
  // begin transfer out deleted transaction
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 100);
  user_data.transfer_ls_id_ = share::ObLSID(1010);
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.start_transfer_commit_version_ = 200;

  mds::MdsCtx ctx{mds::MdsWriter(transaction::ObTransID(789))};
  ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(789), ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet->set(user_data, ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  share::SCN commit_scn = share::SCN::plus(min_scn, 200);
  ret = ut_finish_mds_tx_commit(ctx, ut_mds_write_env().mem_ctx, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // mock weak read
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 210/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, 90/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, INT64_MAX/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, finish_transfer_in_tx_query_between_redo_and_before_prepare)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    user_data.create_commit_scn_ = share::SCN::plus(share::SCN::min_scn(), 100);
    user_data.create_commit_version_ = 100;

    mds::MdsCtx ctx{mds::MdsWriter(transaction::ObTransID(111))};
    ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(111), ctx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
    ret = ut_finish_mds_tx_commit(ctx, ut_mds_write_env().mem_ctx, commit_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_IN;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;
    user_data.create_commit_scn_ = share::SCN::plus(share::SCN::min_scn(), 100);
    user_data.create_commit_version_ = 100;
    user_data.start_transfer_commit_version_ = 200;

    mds::MdsCtx ctx{mds::MdsWriter(transaction::ObTransID(222))};
    ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(222), ctx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ret = ut_finish_mds_tx_commit(ctx, ut_mds_write_env().mem_ctx, commit_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    // finish transfer in
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
    user_data.create_commit_scn_ = share::SCN::plus(share::SCN::min_scn(), 100);
    user_data.create_commit_version_ = 100;
    user_data.start_transfer_commit_version_ = 200;

    // Open a REAL ObPartTransCtx: reader hits this tx on_redo (no commit) and
    // must resolve its state via ObTransService.
    ObLSHandle ls_handle_for_tx;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle_for_tx, ObLSGetMod::STORAGE_MOD));
    UtMdsTxScope scope(*ls_handle_for_tx.get_ls());
    ASSERT_EQ(OB_SUCCESS, scope.begin(tablet_id));
    mds::MdsCtx ctx{mds::MdsWriter(scope.tx_desc()->tx_id_)};
    scope.bind_to_mds_ctx(ctx);
    ret = tablet->set_tablet_status(user_data, ctx);
    ASSERT_EQ(OB_SUCCESS, ret);

    ctx.on_redo(share::SCN::plus(share::SCN::min_scn(), 310));

    // get tablet
    // disable cache
    {
      SpinWLockGuard guard(tablet->mds_cache_lock_);
      tablet->tablet_status_cache_.reset();
    }
    const ObTabletMapKey key(LS_ID, tablet_id);
    ObTabletHandle handle;
    ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle, 1_s/*timeout_us*/, ObMDSGetTabletMode::READ_READABLE_COMMITED, 333);
    ASSERT_EQ(OB_SUCCESS, ret);

    // disable cache
    {
      SpinWLockGuard guard(tablet->mds_cache_lock_);
      tablet->tablet_status_cache_.reset();
    }
    ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle, 1_s/*timeout_us*/, ObMDSGetTabletMode::READ_READABLE_COMMITED, 10);
    ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
  }
}

TEST_F(TestTabletStatusCache, get_empty_result_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet without any tablet status
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
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
      ObTabletStatus::Status::MAX, share::SCN::min_scn()))) {
    LOG_WARN("failed to create tablet", K(ret), K(ls->get_ls_id()), K(tablet_id));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // test READ_WITHOUT_CHECK
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());

  // test READ_READABLE_COMMITED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_READABLE_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());

  // test READ_ALL_COMMITED
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
      ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_FALSE(tablet_handle.get_obj()->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, get_read_all_committed_tablet)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX, share::SCN::invalid_scn());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN commit_scn;

  ObLSHandle ls_handle_for_tx;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle_for_tx, ObLSGetMod::STORAGE_MOD));

  // start transfer in not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_IN;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_IN;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  UtMdsTxScope ctx3_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx3_scope.begin(tablet_id));
  mds::MdsCtx ctx3{mds::MdsWriter(ctx3_scope.tx_desc()->tx_id_)};
  ctx3_scope.bind_to_mds_ctx(ctx3);
  ret = tablet->set(user_data, ctx3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer in commited
  commit_scn = share::SCN::plus(min_scn, 300);
  ret = ut_finish_mds_tx_commit(ctx3, *ctx3_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet with larger snapshot than transfer scn
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 301/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet with smaller snapshot than transfer scn
  // notice: it is allowed because data and tx is ready in dest ls
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 100/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // get tablet with older snapshot than create commit version
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 10/*snapshot*/);
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer in not commited
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  UtMdsTxScope ctx4_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx4_scope.begin(tablet_id));
  mds::MdsCtx ctx4{mds::MdsWriter(ctx4_scope.tx_desc()->tx_id_)};
  ctx4_scope.bind_to_mds_ctx(ctx4);
  ret = tablet->set(user_data, ctx4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer in commited
  commit_scn = share::SCN::plus(min_scn, 400);
  ret = ut_finish_mds_tx_commit(ctx4, *ctx4_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer out not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  user_data.transfer_scn_ = share::SCN::plus(min_scn, 90);

  UtMdsTxScope ctx5_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx5_scope.begin(tablet_id));
  mds::MdsCtx ctx5{mds::MdsWriter(ctx5_scope.tx_desc()->tx_id_)};
  ctx5_scope.bind_to_mds_ctx(ctx5);
  ret = tablet->set(user_data, ctx5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // start transfer out commited
  commit_scn = share::SCN::plus(min_scn, 500);
  ret = ut_finish_mds_tx_commit(ctx5, *ctx5_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer out not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
  user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
  user_data.create_commit_scn_ = share::SCN::plus(min_scn, 50);
  user_data.create_commit_version_ = 50;
  UtMdsTxScope ctx6_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx6_scope.begin(tablet_id));
  mds::MdsCtx ctx6{mds::MdsWriter(ctx6_scope.tx_desc()->tx_id_)};
  ctx6_scope.bind_to_mds_ctx(ctx6);
  ret = tablet->set(user_data, ctx6);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());

  // finish transfer out commited
  commit_scn = share::SCN::plus(min_scn, 600);
  ret = ut_finish_mds_tx_commit(ctx6, *ctx6_scope.mem_ctx(), commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

TEST_F(TestTabletStatusCache, read_all_committed)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX, share::SCN::invalid_scn());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  // get tablet
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN commit_scn;

  // create not committed
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;

  mds::MdsCtx ctx1{mds::MdsWriter(transaction::ObTransID(123))};
  ret = ut_prepare_mds_write(LS_ID, *tablet, tablet_id, transaction::ObTransID(123), ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet->set(user_data, ctx1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_TABLET_NOT_EXIST, ret);

  // commit
  commit_scn = share::SCN::plus(min_scn, 100);
  ret = ut_finish_mds_tx_commit(ctx1, ut_mds_write_env().mem_ctx, commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_ALL_COMMITED, ObTransVersion::MAX_TRANS_VERSION/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTabletStatusCache, transfer_out_not_commited_read_from_src_ls)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  const ObTabletMapKey key(LS_ID, tablet_id);
  ObTabletHandle tablet_handle;
  ret = create_tablet(tablet_id, tablet_handle, ObTabletStatus::MAX, share::SCN::invalid_scn());
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // disable cache
  {
    SpinWLockGuard guard(tablet->mds_cache_lock_);
    tablet->tablet_status_cache_.reset();
  }

  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN min_scn;
  min_scn.set_min();
  share::SCN commit_scn;

  // start transfer in not commited
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.create_commit_scn_.set_min();
  user_data.create_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  user_data.transfer_scn_.set_min();

  // Open a REAL ObPartTransCtx so reads on the uncommitted row don't trip
  // OB_TRANS_CTX_NOT_EXIST when the tx_state resolver looks up the tx.
  ObLSHandle ls_handle_for_tx;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle_for_tx, ObLSGetMod::STORAGE_MOD));
  UtMdsTxScope ctx3_scope(*ls_handle_for_tx.get_ls());
  ASSERT_EQ(OB_SUCCESS, ctx3_scope.begin(tablet_id));
  mds::MdsCtx ctx3{mds::MdsWriter(ctx3_scope.tx_desc()->tx_id_)};
  ctx3_scope.bind_to_mds_ctx(ctx3);
  ret = tablet->set(user_data, ctx3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 1_s,
    ObMDSGetTabletMode::READ_READABLE_COMMITED, 10/*snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(!tablet->tablet_status_cache_.is_valid());
}

// TODO(@gaishun.gs): refactor test cases to cover all scene
/*
TEST_F(TestTabletStatusCache, transfer_src_ls_read_all_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_dst_ls_read_all_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_src_ls_read_readable_committed)
{

}

TEST_F(TestTabletStatusCache, transfer_dst_ls_read_readable_committed)
{

}
*/

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_status_cache.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tablet_status_cache.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
