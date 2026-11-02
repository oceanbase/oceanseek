/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <cstdlib>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/allocator/ob_malloc.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "share/ob_debug_sync.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;

namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::share;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y)
#define LOGI(fmt, ...) SERVER_LOG(INFO, fmt, ##__VA_ARGS__)

// ---------------------------------------------------------------
// Global test context
// ---------------------------------------------------------------
class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};
TestRunCtx R;

// ---------------------------------------------------------------
// Helper: get tablet_id + ls_id by table name, then read back info
// ---------------------------------------------------------------
static int get_tablet_and_ls(const char *table_name,
                             ObTabletID &tablet_id,
                             ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSH::select_table_tablet(R.tenant_id_, table_name, tablet_id))) {
    SERVER_LOG(WARN, "fail to get tablet_id", K(ret));
  } else if (OB_FAIL(SSH::select_table_loc(R.tenant_id_, table_name, ls_id))) {
    SERVER_LOG(WARN, "fail to get ls_id", K(ret));
  }
  return ret;
}

static int register_membership(const ObTabletID &tablet_id,
                               const ObLSID &ls_id,
                               const bool has_async_index,
                               const bool commit)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(has_async_index);

  char buf[512];
  int64_t pos = 0;
  ObMySQLTransaction trans;
  if (OB_FAIL(arg.serialize(buf, sizeof(buf), pos))) {
    LOG_WARN("failed to serialize async index membership", K(ret), K(arg));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, R.tenant_id_))) {
    LOG_WARN("failed to start transaction", K(ret), "tenant_id", R.tenant_id_);
  } else {
    observer::ObInnerSQLConnection *conn =
        static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
    ObRegisterMdsFlag flag;
    if (OB_FAIL(conn->register_multi_data_source(
            R.tenant_id_, ls_id, ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
            buf, pos, flag))) {
      LOG_WARN("failed to register async index membership", K(ret), K(tablet_id), K(ls_id));
    }
  }
  if (trans.is_started()) {
    const int end_ret = trans.end(OB_SUCC(ret) && commit);
    if (OB_SUCC(ret) && OB_SUCCESS != end_ret) {
      ret = end_ret;
      LOG_WARN("failed to end membership transaction", K(ret), K(commit));
    }
  }
  return ret;
}

static int
register_refresh_info_in_trans(const ObTabletID &tablet_id, const ObLSID &ls_id,
                               const ObTabletAsyncIndexRefreshInfo &info,
                               ObMySQLTransaction &trans) {
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  char buf[512];
  int64_t pos = 0;
  observer::ObInnerSQLConnection *conn = nullptr;
  if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_NOT_INIT;
    LOG_WARN("membership transaction is not started", K(ret), K(tablet_id),
             K(ls_id));
  } else if (OB_FAIL(arg.info_.assign(info))) {
    LOG_WARN("failed to assign async index refresh info", K(ret), K(info));
  } else if (OB_FAIL(arg.serialize(buf, sizeof(buf), pos))) {
    LOG_WARN("failed to serialize async index refresh info", K(ret), K(arg));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(
                           trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("membership transaction connection is null", K(ret), K(tablet_id),
             K(ls_id));
  } else {
    ObRegisterMdsFlag flag;
    if (OB_FAIL(conn->register_multi_data_source(
            R.tenant_id_, ls_id, ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
            buf, pos, flag))) {
      LOG_WARN("failed to register async index refresh info", K(ret),
               K(tablet_id), K(ls_id));
    }
  }
  return ret;
}

// ---------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------
class TestAsyncIndexRefreshInfoMds : public ObSimpleClusterTestBase
{
public:
  TestAsyncIndexRefreshInfoMds()
      : ObSimpleClusterTestBase("test_async_index_refresh_info_", "50G", "50G", "50G")
  {}
};

// ---------------------------------------------------------------
// Test 1: boilerplate – start observer and create tenant
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoMds, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(TestAsyncIndexRefreshInfoMds, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("async_idx_t", "20G", "20G", false, 4));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "async_idx_t"));
  ASSERT_NE(0, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("async_idx_t"));
}

// ---------------------------------------------------------------
// Test 2: basic register + commit + read-back
//
// Register ObTabletAsyncIndexRefreshInfoArg via register_multi_data_source,
// commit the transaction, then read back ObTabletAsyncIndexRefreshInfo from
// the tablet's MDS table and verify the snapshot versions.
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoMds, basic_register_and_read)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  // 1. Create a user table to get a real tablet
  EQ(0, sql_proxy.write("drop table if exists t_async_idx1", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx1(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx1", tablet_id, ls_id));
  LOGI("got tablet_id and ls_id", K(tablet_id), K(ls_id));

  // 2. Build and serialize ObTabletAsyncIndexRefreshInfoArg
  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_     = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(1000));
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(2000));
  ASSERT_TRUE(arg.is_valid());

  char buf[512];
  int64_t pos = 0;
  EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));

  // 3. Register MDS via a transaction and commit
  //    Pattern mirrors test_tx_data.cpp::retain_ctx2
  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  EQ(0, conn->register_multi_data_source(R.tenant_id_,
                                         ls_id,
                                         ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                                         buf, pos, flag));
  EQ(0, trans.end(true /* commit */));
  LOGI("MDS registered and committed", K(tablet_id), K(ls_id));

  // 4. Read back from the tablet's MDS table
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    ObTabletAsyncIndexRefreshInfo info;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_info(
        share::SCN::max_scn(), info, 1_s));

    ASSERT_TRUE(info.is_valid());
    ASSERT_TRUE(info.has_async_index());
    ASSERT_EQ(2, info.get_refresh_snapshots().count());
    ASSERT_EQ(1000, info.get_refresh_snapshots().at(0));
    ASSERT_EQ(2000, info.get_refresh_snapshots().at(1));
    LOGI("read back info ok", K(info));
  }
}

TEST_F(TestAsyncIndexRefreshInfoMds, commit_version_placeholder_is_resolved)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write(
      "drop table if exists t_async_idx_commit_version", affected_rows));
  EQ(0, sql_proxy.write(
      "create table t_async_idx_commit_version(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls(
      "t_async_idx_commit_version", tablet_id, ls_id));

  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(
      ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);
  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS,
     register_refresh_info_in_trans(tablet_id, ls_id, info, trans));
  EQ(OB_SUCCESS, trans.end(true /* commit */));

  int64_t create_published_version = 0;
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS,
       ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ObTabletAsyncIndexRefreshInfo committed_info;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_info(
        share::SCN::max_scn(), committed_info, 1_s));
    ASSERT_TRUE(committed_info.has_published_version());
    ASSERT_FALSE(committed_info.need_fill_commit_version());
    ASSERT_GT(committed_info.get_published_version(), 0);
    create_published_version = committed_info.get_published_version();

    // This test Tablet has an empty physical TableStore. Mark only its local
    // caches as an async-refresh target created at C so the production
    // watermark getter can be exercised without introducing a separate
    // async-index DDL fixture.
    tablet_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ =
        true;
    ObTabletStatusCache &tablet_status_cache =
        tablet_handle.get_obj()->tablet_status_cache_;
    ASSERT_TRUE(tablet_status_cache.is_valid());
    const ObTabletStatus cached_tablet_status =
        tablet_status_cache.get_tablet_status();
    const int64_t cached_delete_commit_version =
        tablet_status_cache.get_delete_commit_version();
    int64_t physical_published_version = 0;
    int64_t latest_published_version = 0;
    int64_t query_published_version = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_max_published_version(
        physical_published_version));
    ASSERT_LT(physical_published_version, create_published_version);

    // Logical publication is independent of TabletStatus cache population.
    tablet_status_cache.reset();
    share::SCN finite_read_snapshot;
    EQ(OB_SUCCESS,
       finite_read_snapshot.convert_for_tx(create_published_version));
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_async_index_query_published_version(
           finite_read_snapshot, query_published_version));
    ASSERT_EQ(create_published_version, query_published_version);

    // Bound publication by the SQL snapshot, including when the publishing
    // transaction committed later than that SQL snapshot.
    tablet_status_cache.set_value(
        cached_tablet_status, create_published_version,
        cached_delete_commit_version);
    share::SCN before_create_snapshot;
    EQ(OB_SUCCESS,
       before_create_snapshot.convert_for_tx(create_published_version - 1));
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_async_index_query_published_version(
           before_create_snapshot, query_published_version));
    ASSERT_EQ(create_published_version - 1, query_published_version);
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_async_index_query_published_version(
           finite_read_snapshot, query_published_version));
    ASSERT_EQ(create_published_version, query_published_version);

    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_published_version(
        latest_published_version));
    ASSERT_EQ(create_published_version, latest_published_version);

    // The original CREATE publication is identifiable by both its value and
    // the MDS row transaction version: on_commit fills both with the same C.
    ObTabletAsyncIndexRefreshInfo latest_info;
    mds::MdsWriter unused_writer;
    mds::TwoPhaseCommitState trans_stat;
    share::SCN trans_version;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_refresh_info(
        latest_info, unused_writer, trans_stat, trans_version));
    ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
    ASSERT_EQ(create_published_version, latest_info.get_published_version());
    ASSERT_EQ(create_published_version, trans_version.get_val_for_tx());
  }

  // A later refresh may publish the same numeric value C while its physical
  // INC is not visible yet. Its MDS row commits at D, so D != C prevents it
  // from being mistaken for the original CREATE bootstrap row.
  ObTabletAsyncIndexRefreshInfo same_value_refresh_info;
  same_value_refresh_info.set_published_version(create_published_version);
  ObMySQLTransaction same_value_trans;
  EQ(OB_SUCCESS, same_value_trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(
      tablet_id, ls_id, same_value_refresh_info, same_value_trans));
  EQ(OB_SUCCESS, same_value_trans.end(true /* commit */));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS,
       ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    tablet_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ =
        true;
    ObTabletStatusCache &tablet_status_cache =
        tablet_handle.get_obj()->tablet_status_cache_;
    ASSERT_TRUE(tablet_status_cache.is_valid());
    tablet_status_cache.set_value(
        tablet_status_cache.get_tablet_status(), create_published_version,
        tablet_status_cache.get_delete_commit_version());
    ObTabletAsyncIndexRefreshInfo latest_info;
    mds::MdsWriter unused_writer;
    mds::TwoPhaseCommitState trans_stat;
    share::SCN trans_version;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_refresh_info(
        latest_info, unused_writer, trans_stat, trans_version));
    ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
    ASSERT_EQ(create_published_version, latest_info.get_published_version());
    ASSERT_GT(trans_version.get_val_for_tx(), create_published_version);

    int64_t physical_published_version = 0;
    int64_t latest_published_version = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_max_published_version(
        physical_published_version));
    ASSERT_LT(physical_published_version, create_published_version);
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_published_version(
        latest_published_version));
    ASSERT_EQ(create_published_version, latest_published_version);
  }

  // An empty refresh advances logical publication without installing an INC.
  // A pending publication keeps the committed cache and must not become visible.
  const int64_t later_published_version = create_published_version + 1;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS,
       ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    tablet_handle.get_obj()->async_index_membership_cache_.set_value(
        false, create_published_version);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }
  ObTabletAsyncIndexRefreshInfo later_refresh_info;
  later_refresh_info.set_published_version(later_published_version);
  ObMySQLTransaction later_trans;
  EQ(OB_SUCCESS, later_trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(
      tablet_id, ls_id, later_refresh_info, later_trans));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS,
       ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    tablet_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ = true;
    int64_t latest_published_version = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_published_version(
        latest_published_version));
    ASSERT_EQ(create_published_version, latest_published_version);
    ASSERT_TRUE(tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    // Model an early commit notification: pending MDS must not refill the old value.
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->on_mds_info_cache_user_finish(
        tablet_id, ObMdsInfoCacheTarget::ASYNC_INDEX_MEMBERSHIP, true,
        later_published_version));
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_published_version(
        latest_published_version));
    ASSERT_EQ(create_published_version, latest_published_version);
    ASSERT_FALSE(tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }
  EQ(OB_SUCCESS, later_trans.end(true /* commit */));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS,
       ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    tablet_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ =
        true;
    ObTabletStatusCache &tablet_status_cache =
        tablet_handle.get_obj()->tablet_status_cache_;
    ASSERT_TRUE(tablet_status_cache.is_valid());
    tablet_status_cache.set_value(
        tablet_status_cache.get_tablet_status(), create_published_version,
        tablet_status_cache.get_delete_commit_version());
    // Model the local-commit window before its cache notification: the old
    // cache remains valid, but a locked refresh must use committed MDS directly.
    ObAsyncIndexMembershipCache &cache = tablet_handle.get_obj()->async_index_membership_cache_;
    cache.set_value(true, create_published_version);
    int64_t refresh_start = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_start_version(refresh_start));
    ASSERT_EQ(create_published_version, refresh_start);
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_start_version(
        refresh_start, false /*use_cache*/));
    ASSERT_EQ(later_published_version, refresh_start);
    ASSERT_EQ(create_published_version, cache.get_published_version());
    cache.reset();
    int64_t physical_published_version = 0;
    int64_t query_published_version = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_max_published_version(
        physical_published_version));
    ASSERT_LT(physical_published_version, create_published_version);
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_async_index_query_published_version(
           share::SCN::max_scn(), query_published_version));
    ASSERT_EQ(later_published_version, query_published_version);
    share::SCN read_snapshot;
    EQ(OB_SUCCESS, read_snapshot.convert_for_tx(create_published_version));
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_async_index_query_published_version(
           read_snapshot, query_published_version));
    ASSERT_EQ(create_published_version, query_published_version);

    // P is the logical data boundary; C is the later refresh commit SCN.
    // A logical query at P < S < C can use P, but a deletion scan at S must
    // use a newer physical MVCC snapshot including that refresh transaction.
    ObTabletAsyncIndexRefreshInfo latest_info;
    mds::MdsWriter unused_writer;
    mds::TwoPhaseCommitState trans_stat;
    share::SCN commit_scn;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_latest_async_index_refresh_info(
        latest_info, unused_writer, trans_stat, commit_scn));
    ASSERT_EQ(mds::TwoPhaseCommitState::ON_COMMIT, trans_stat);
    ASSERT_EQ(later_published_version, latest_info.get_published_version());
    ASSERT_GT(commit_scn.get_val_for_tx() - 1, later_published_version);
    share::SCN before_commit_snapshot;
    EQ(OB_SUCCESS, before_commit_snapshot.convert_for_tx(commit_scn.get_val_for_tx() - 1));
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_query_published_version(
        before_commit_snapshot, query_published_version));
    ASSERT_EQ(later_published_version, query_published_version);

    // Physical deletion visibility is tested below on a real async-delete fixture.
    // This ordinary-table case retains the publication/commit ordering assertions.

  }
}

// ---------------------------------------------------------------
// Test 3: register + commit + ls_reboot + read-back
//
// Verifies that the replay path (on_replay →
// ObTabletAsyncIndexRefreshInfoReplayExecutor → replay_async_index_refresh_info)
// correctly re-applies the MDS data after an LS restart.
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoMds, register_and_ls_reboot)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("drop table if exists t_async_idx2", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx2(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx2", tablet_id, ls_id));

  // Register and commit
  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_     = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  arg.info_.set_published_version(7000);
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(5000));
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(6000));
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(7000));
  char buf[512];
  int64_t pos = 0;
  EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));

  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  EQ(0, conn->register_multi_data_source(R.tenant_id_,
                                         ls_id,
                                         ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                                         buf, pos, flag));
  EQ(0, trans.end(true));

  // Flush tx ctx and reboot the LS to trigger replay
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id));
  LOGI("ls_reboot", K(ls_id));
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  // Read back after reboot – must equal pre-reboot state
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    ObTabletAsyncIndexRefreshInfo info;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_info(
        share::SCN::max_scn(), info, 1_s));

    ASSERT_TRUE(info.is_valid());
    ASSERT_EQ(3, info.get_refresh_snapshots().count());
    ASSERT_EQ(5000, info.get_refresh_snapshots().at(0));
    ASSERT_EQ(6000, info.get_refresh_snapshots().at(1));
    ASSERT_EQ(7000, info.get_refresh_snapshots().at(2));
    ASSERT_TRUE(info.has_published_version());
    ASSERT_EQ(7000, info.get_published_version());
    LOGI("read back after reboot ok", K(info));
  }
}

// A reader that observes an uncommitted membership write must use the previous
// committed value without populating the shared cache. After commit, the next
// read must observe and cache the new value.
TEST_F(TestAsyncIndexRefreshInfoMds, uncommitted_membership_is_not_cached)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_async_idx_uncommitted", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx_uncommitted(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_uncommitted", tablet_id, ls_id));

  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  char buf[512];
  int64_t pos = 0;
  EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));

  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  EQ(OB_SUCCESS, conn->register_multi_data_source(
      R.tenant_id_, ls_id, ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
      buf, pos, flag));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    tablet_handle.get_obj()->async_index_membership_cache_.reset();
    bool has_async_index = true;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_FALSE(tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  EQ(OB_SUCCESS, trans.end(true /* commit */));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
  }
}

// Cover the reverse direction of uncommitted_membership_is_not_cached:
// readers must keep seeing the committed true value while false is pending,
// then observe and cache false only after commit.
TEST_F(TestAsyncIndexRefreshInfoMds, uncommitted_false_reads_true_then_commit) {
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write(
            "drop table if exists t_async_idx_uncommitted_false_commit",
            affected_rows));
  EQ(0, sql_proxy.write(
            "create table t_async_idx_uncommitted_false_commit(c1 int)",
            affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_uncommitted_false_commit", tablet_id,
                          ls_id));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, true));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_TRUE(tablet_handle.get_obj()
                    ->async_index_membership_cache_.has_async_index());
  }

  ObTabletAsyncIndexRefreshInfo pending_info;
  pending_info.set_has_async_index(false);
  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS,
     register_refresh_info_in_trans(tablet_id, ls_id, pending_info, trans));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  // Replace the Tablet while it still carries the old cache. Commit must
  // invalidate the current Tablet, not the instance used by registration.
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle old_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, old_handle));
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_snapshot_version(
        tablet_id, old_handle.get_obj()->get_snapshot_version() + 1));
    ObTabletHandle new_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, new_handle));
    ASSERT_NE(old_handle.get_obj(), new_handle.get_obj());
    ASSERT_TRUE(new_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_EQ(old_handle.get_obj()->async_index_membership_cache_.get_invalidation_count(),
              new_handle.get_obj()->async_index_membership_cache_.get_invalidation_count());
  }

  EQ(OB_SUCCESS, trans.end(true /* commit */));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = true;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_FALSE(tablet_handle.get_obj()
                     ->async_index_membership_cache_.has_async_index());
  }
}

// Exercise the transfer-completion cache handoff with real MDS and COW tablets.
// Only the detached old Tablet's transfer flag is constructed here; this does
// not run the transfer DAG or relocate data between LSes.
TEST_F(TestAsyncIndexRefreshInfoMds, transfer_completion_discards_empty_publication_cache)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(OB_SUCCESS, sql_proxy.write(
      "drop table if exists t_async_idx_transfer_cache", affected_rows));
  EQ(OB_SUCCESS, sql_proxy.write(
      "create table t_async_idx_transfer_cache(c1 int)", affected_rows));
  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(OB_SUCCESS, get_tablet_and_ls("t_async_idx_transfer_cache", tablet_id, ls_id));

  ObTabletHandle old_handle;
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, old_handle));
    old_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ = true;
    int64_t published_version = -1;
    EQ(OB_SUCCESS, old_handle.get_obj()->get_latest_async_index_published_version(
        published_version));
    ASSERT_EQ(0, published_version);
    ASSERT_TRUE(old_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_FALSE(old_handle.get_obj()->async_index_membership_cache_.has_published_version());

    // Detach the old Tablet so the normal MDS commit below does not notify it.
    // A transfer relocation likewise leaves its pre-relocation empty cache intact.
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_snapshot_version(
        tablet_id, old_handle.get_obj()->get_snapshot_version() + 1));
    ObTabletHandle current_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, current_handle));
    ASSERT_NE(old_handle.get_obj(), current_handle.get_obj());
    ASSERT_TRUE(current_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_FALSE(current_handle.get_obj()->async_index_membership_cache_.has_published_version());
  }

  const int64_t expected_published_version = 7000;
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(expected_published_version);
  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(tablet_id, ls_id, info, trans));
  EQ(OB_SUCCESS, trans.end(true /* commit */));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ObTabletHandle current_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, current_handle));
    ObTabletAsyncIndexRefreshInfo committed_info;
    EQ(OB_SUCCESS, current_handle.get_obj()->get_latest_committed_async_index_refresh_info(
        committed_info));
    ASSERT_EQ(expected_published_version, committed_info.get_published_version());
    ASSERT_TRUE(old_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_FALSE(old_handle.get_obj()->async_index_membership_cache_.has_published_version());

    old_handle.get_obj()->tablet_meta_.transfer_info_.has_transfer_table_ = true;
    ASSERT_FALSE(current_handle.get_obj()->tablet_meta_.has_transfer_table());
    current_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ = true;
    EQ(OB_SUCCESS, current_handle.get_obj()->copy_mds_info_cache_from(*old_handle.get_obj()));
    ASSERT_FALSE(current_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_EQ(old_handle.get_obj()->async_index_membership_cache_.get_invalidation_count() + 1,
              current_handle.get_obj()->async_index_membership_cache_.get_invalidation_count());
    int64_t published_version = 0;
    EQ(OB_SUCCESS, current_handle.get_obj()->get_latest_async_index_published_version(
        published_version));
    ASSERT_EQ(expected_published_version, published_version);

    // Ordinary COW after transfer completion must retain the warm publication.
    const int64_t invalidation_count =
        current_handle.get_obj()->async_index_membership_cache_.get_invalidation_count();
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet_svr()->update_tablet_snapshot_version(
        tablet_id, current_handle.get_obj()->get_snapshot_version() + 1));
    ObTabletHandle replacement_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, replacement_handle));
    ASSERT_NE(current_handle.get_obj(), replacement_handle.get_obj());
    const ObAsyncIndexMembershipCache &cache =
        replacement_handle.get_obj()->async_index_membership_cache_;
    ASSERT_TRUE(cache.is_valid());
    ASSERT_EQ(expected_published_version, cache.get_published_version());
    ASSERT_EQ(invalidation_count, cache.get_invalidation_count());
    replacement_handle.get_obj()->table_store_cache_.is_async_index_refresh_target_ = true;
    EQ(OB_SUCCESS, replacement_handle.get_obj()->get_latest_async_index_published_version(
        published_version));
    ASSERT_EQ(expected_published_version, published_version);
  }
}

// An aborted false tombstone must be invisible both before and after abort,
// including when the previously committed true value has already been cached.
TEST_F(TestAsyncIndexRefreshInfoMds,
       uncommitted_false_abort_preserves_warm_true) {
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0,
     sql_proxy.write("drop table if exists t_async_idx_uncommitted_false_abort",
                     affected_rows));
  EQ(0,
     sql_proxy.write("create table t_async_idx_uncommitted_false_abort(c1 int)",
                     affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_uncommitted_false_abort", tablet_id,
                          ls_id));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, true));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  ObTabletAsyncIndexRefreshInfo pending_info;
  pending_info.set_has_async_index(false);
  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS,
     register_refresh_info_in_trans(tablet_id, ls_id, pending_info, trans));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  EQ(OB_SUCCESS, trans.end(false /* commit */));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ASSERT_TRUE(tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        !tablet_handle.get_obj()->async_index_membership_cache_.is_valid() ||
        tablet_handle.get_obj()
            ->async_index_membership_cache_.has_async_index());
  }
}

// A pending true value must preserve a warm false cache, including after abort.
TEST_F(TestAsyncIndexRefreshInfoMds,
       uncommitted_true_abort_preserves_warm_false) {
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0,
     sql_proxy.write("drop table if exists t_async_idx_uncommitted_true_abort",
                     affected_rows));
  EQ(0,
     sql_proxy.write("create table t_async_idx_uncommitted_true_abort(c1 int)",
                     affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0,
     get_tablet_and_ls("t_async_idx_uncommitted_true_abort", tablet_id, ls_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = true;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_FALSE(tablet_handle.get_obj()
                     ->async_index_membership_cache_.has_async_index());
  }

  ObTabletAsyncIndexRefreshInfo pending_info;
  pending_info.set_has_async_index(true);
  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS,
     register_refresh_info_in_trans(tablet_id, ls_id, pending_info, trans));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = true;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  EQ(OB_SUCCESS, trans.end(false /* commit */));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    ASSERT_TRUE(tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    bool has_async_index = true;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    ASSERT_TRUE(
        !tablet_handle.get_obj()->async_index_membership_cache_.is_valid() ||
        !tablet_handle.get_obj()
             ->async_index_membership_cache_.has_async_index());
  }
}

// Updating only refresh snapshots keeps membership=true. Verify that the warm
// membership cache remains correct across both commit and abort.
TEST_F(TestAsyncIndexRefreshInfoMds,
       same_membership_snapshot_update_keeps_cache) {
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_async_idx_same_membership",
                        affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx_same_membership(c1 int)",
                        affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_same_membership", tablet_id, ls_id));

  ObTabletAsyncIndexRefreshInfo initial_info;
  initial_info.set_has_async_index(true);
  EQ(OB_SUCCESS, initial_info.append_refresh_snapshot(100));
  ObMySQLTransaction initial_trans;
  EQ(OB_SUCCESS, initial_trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(tablet_id, ls_id, initial_info,
                                                initial_trans));
  EQ(OB_SUCCESS, initial_trans.end(true /* commit */));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
  }

  ObTabletAsyncIndexRefreshInfo committed_info;
  committed_info.set_has_async_index(true);
  EQ(OB_SUCCESS, committed_info.append_refresh_snapshot(100));
  EQ(OB_SUCCESS, committed_info.append_refresh_snapshot(200));
  ObMySQLTransaction commit_trans;
  EQ(OB_SUCCESS, commit_trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(tablet_id, ls_id,
                                                committed_info, commit_trans));
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_TRUE(tablet_handle.get_obj()
                    ->async_index_membership_cache_.has_async_index());
  }
  EQ(OB_SUCCESS, commit_trans.end(true /* commit */));

  ObTabletAsyncIndexRefreshInfo aborted_info;
  aborted_info.set_has_async_index(true);
  EQ(OB_SUCCESS, aborted_info.append_refresh_snapshot(100));
  EQ(OB_SUCCESS, aborted_info.append_refresh_snapshot(200));
  EQ(OB_SUCCESS, aborted_info.append_refresh_snapshot(300));
  ObMySQLTransaction abort_trans;
  EQ(OB_SUCCESS, abort_trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(OB_SUCCESS, register_refresh_info_in_trans(tablet_id, ls_id, aborted_info,
                                                abort_trans));
  EQ(OB_SUCCESS, abort_trans.end(false /* commit */));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS,
       MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS,
       tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
    ASSERT_TRUE(
        tablet_handle.get_obj()->async_index_membership_cache_.is_valid());
    ASSERT_TRUE(tablet_handle.get_obj()
                    ->async_index_membership_cache_.has_async_index());

    ObTabletAsyncIndexRefreshInfo actual_info;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_async_index_refresh_info(
                       share::SCN::max_scn(), actual_info, 1_s));
    ASSERT_EQ(2, actual_info.get_refresh_snapshots().count());
    ASSERT_EQ(100, actual_info.get_refresh_snapshots().at(0));
    ASSERT_EQ(200, actual_info.get_refresh_snapshots().at(1));
    ASSERT_TRUE(actual_info.has_async_index());
  }
}

// A commit must finish while a reader is paused between MDS read and refill.
// The reader must not cache its old value after that invalidation.
TEST_F(TestAsyncIndexRefreshInfoMds, commit_during_membership_cache_refill)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "drop table if exists t_async_idx_refill", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create table t_async_idx_refill(c1 int)", affected_rows));
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls("t_async_idx_refill", tablet_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, true));
  ObTabletHandle tablet;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_tablet(tablet_id, tablet));
    tablet.get_obj()->async_index_membership_cache_.reset();
  }
  GCONF.debug_sync_timeout.set_value("10000000");
  ObMalloc allocator;
  ObDSSessionActions read_actions;
  ObDSSessionActions control_actions;
  ASSERT_EQ(OB_SUCCESS, read_actions.init(1024, allocator));
  ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, allocator));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", false, control_actions));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "AFTER_ASYNC_INDEX_MEMBERSHIP_MDS_READ signal ai_read_done "
      "wait_for ai_refill_continue timeout 10000000 execute 1", false, read_actions));
  std::atomic<int> read_ret(OB_EAGAIN);
  std::atomic<int> commit_ret(OB_EAGAIN);
  std::atomic<bool> commit_finished(false);
  std::thread reader([&]() {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = tenant_guard.switch_to(R.tenant_id_);
    bool membership = false;
    if (OB_SUCC(ret) && OB_SUCC(ret = GDS.set_thread_local_actions(read_actions))) {
      ret = tablet.get_obj()->get_has_async_index(membership);
    }
    read_ret.store(ret);
  });
  const int wait_ret = GDS.add_debug_sync(
      "now wait_for ai_read_done timeout 10000000", false, control_actions);
  std::thread writer([&]() {
    commit_ret.store(register_membership(tablet_id, ls_id, false, true));
    commit_finished.store(true);
  });
  for (int64_t i = 0; i < 500 && !commit_finished.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const bool finished_before_refill = commit_finished.load();
  const int signal_ret = GDS.add_debug_sync("now signal ai_refill_continue", false, control_actions);
  reader.join();
  writer.join();
  const int reset_ret = GDS.add_debug_sync("reset", false, control_actions);
  ASSERT_EQ(OB_SUCCESS, wait_ret);
  ASSERT_EQ(OB_SUCCESS, signal_ret);
  ASSERT_EQ(OB_SUCCESS, reset_ret);
  ASSERT_EQ(OB_SUCCESS, read_ret.load());
  ASSERT_EQ(OB_SUCCESS, commit_ret.load());
  ASSERT_TRUE(finished_before_refill);
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_tablet(tablet_id, tablet));
    const ObAsyncIndexMembershipCache &cache = tablet.get_obj()->async_index_membership_cache_;
    ASSERT_FALSE(cache.is_valid() && cache.has_async_index());
    bool membership = true;
    ASSERT_EQ(OB_SUCCESS, tablet.get_obj()->get_has_async_index(membership));
    ASSERT_FALSE(membership);
  }
}

// A concurrent Tablet CAS must not leave a stale cache after commit. Cover
// both membership transitions while CAS races with registration and commit.
TEST_F(TestAsyncIndexRefreshInfoMds,
       tablet_cas_does_not_publish_stale_membership_cache)
{
  struct RaceCase
  {
    const char *table_name_;
    const char *lookup_event_;
    const char *continue_event_;
    bool initial_membership_;
    bool updated_membership_;
  };
  const RaceCase race_cases[] = {
      {"t_async_idx_cas_false_true", "ai_lookup_ft", "ai_continue_ft",
       false, true},
      {"t_async_idx_cas_true_false", "ai_lookup_tf", "ai_continue_tf",
       true, false},
  };

  GCONF.debug_sync_timeout.set_value("10000000");
  ObMalloc debug_sync_allocator;

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  for (const RaceCase &race_case : race_cases) {
    ObDSSessionActions point_actions;
    ObDSSessionActions control_actions;
    ASSERT_EQ(OB_SUCCESS, point_actions.init(1024, debug_sync_allocator));
    ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, debug_sync_allocator));

    int64_t affected_rows = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("drop table if exists %s", race_case.table_name_));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS,
              sql.assign_fmt("create table %s(c1 int)", race_case.table_name_));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    ObTabletID tablet_id;
    ObLSID ls_id;
    ASSERT_EQ(OB_SUCCESS,
              get_tablet_and_ls(race_case.table_name_, tablet_id, ls_id));
    if (race_case.initial_membership_) {
      ASSERT_EQ(OB_SUCCESS,
                register_membership(tablet_id, ls_id, true, true));
    }

    ObTabletHandle old_tablet_handle;
    int ret = OB_SUCCESS;
    MTL_SWITCH(R.tenant_id_) {
      ObLSHandle ls_handle;
      ASSERT_EQ(OB_SUCCESS,
                MTL(ObLSService *)->get_ls(
                    ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
      ASSERT_EQ(OB_SUCCESS,
                ls_handle.get_ls()->get_tablet(
                    tablet_id, old_tablet_handle));
      bool has_async_index = !race_case.initial_membership_;
      ASSERT_EQ(OB_SUCCESS,
                old_tablet_handle.get_obj()->get_has_async_index(
                    has_async_index));
      ASSERT_EQ(race_case.initial_membership_, has_async_index);
      ASSERT_TRUE(old_tablet_handle.get_obj()
                      ->async_index_membership_cache_.is_valid());
      ASSERT_EQ(race_case.initial_membership_,
                old_tablet_handle.get_obj()
                    ->async_index_membership_cache_.has_async_index());
    }
    ASSERT_NE(nullptr, old_tablet_handle.get_obj());
    const int64_t new_snapshot_version =
        old_tablet_handle.get_obj()->get_snapshot_version() + 1;

    ASSERT_EQ(
        OB_SUCCESS,
        GDS.add_debug_sync("reset", false /* is_global */, control_actions));
    ObSqlString sync_action;
    ASSERT_EQ(
        OB_SUCCESS,
        sync_action.assign_fmt(
            "AFTER_ASYNC_INDEX_MEMBERSHIP_LIVE_TABLET_LOOKUP "
            "signal %s wait_for %s timeout 10000000 execute 1",
            race_case.lookup_event_, race_case.continue_event_));
    ASSERT_EQ(OB_SUCCESS,
              GDS.add_debug_sync(
                  sync_action.string(), false /* is_global */, point_actions));

    std::atomic<int> writer_debug_sync_ret(OB_EAGAIN);
    std::atomic<int> writer_ret(OB_EAGAIN);
    std::atomic<bool> writer_finished(false);
    std::thread writer([&]() {
      const int sync_ret = GDS.set_thread_local_actions(point_actions);
      writer_debug_sync_ret.store(sync_ret);
      if (OB_SUCCESS == sync_ret) {
        writer_ret.store(
            register_membership(
                tablet_id, ls_id, race_case.updated_membership_, true));
      } else {
        writer_ret.store(sync_ret);
      }
      writer_finished.store(true);
    });

    ObSqlString wait_action;
    int wait_ret = wait_action.assign_fmt(
        "now wait_for %s timeout 10000000", race_case.lookup_event_);
    if (OB_SUCCESS == wait_ret) {
      wait_ret = GDS.add_debug_sync(
          wait_action.string(), false /* is_global */, control_actions);
    }
    const bool writer_blocked_at_lookup = !writer_finished.load();

    std::atomic<int> cas_ret(OB_EAGAIN);
    std::atomic<bool> cas_finished(false);
    std::thread tablet_cas([&]() {
      int ret = OB_SUCCESS;
      share::ObTenantSwitchGuard tenant_guard;
      if (OB_FAIL(tenant_guard.switch_to(R.tenant_id_))) {
        LOG_WARN("failed to switch tenant for tablet CAS", K(ret),
                 "tenant_id", R.tenant_id_);
      } else {
        ObLSHandle ls_handle;
        if (OB_FAIL(MTL(ObLSService *)->get_ls(
                ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
          LOG_WARN("failed to get LS for tablet CAS", K(ret), K(ls_id));
        } else if (OB_FAIL(ls_handle.get_ls()
                               ->get_tablet_svr()
                               ->update_tablet_snapshot_version(
                                   tablet_id, new_snapshot_version))) {
          LOG_WARN("failed to update tablet snapshot version", K(ret),
                   K(tablet_id), K(new_snapshot_version));
        }
      }
      cas_ret.store(ret);
      cas_finished.store(true);
    });

    for (int64_t i = 0; i < 200 && !cas_finished.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const bool cas_finished_before_continue = cas_finished.load();

    ObSqlString signal_action;
    int signal_ret = signal_action.assign_fmt(
        "now signal %s", race_case.continue_event_);
    if (OB_SUCCESS == signal_ret) {
      signal_ret = GDS.add_debug_sync(
          signal_action.string(), false /* is_global */, control_actions);
    }
    writer.join();
    tablet_cas.join();
    const int reset_ret =
        GDS.add_debug_sync("reset", false /* is_global */, control_actions);

    ASSERT_EQ(OB_SUCCESS, wait_ret);
    ASSERT_TRUE(writer_blocked_at_lookup);
    ASSERT_FALSE(cas_finished_before_continue);
    ASSERT_EQ(OB_SUCCESS, signal_ret);
    ASSERT_EQ(OB_SUCCESS, writer_debug_sync_ret.load());
    ASSERT_EQ(OB_SUCCESS, writer_ret.load());
    ASSERT_EQ(OB_SUCCESS, cas_ret.load());
    ASSERT_EQ(OB_SUCCESS, reset_ret);

    ObTabletHandle new_tablet_handle;
    MTL_SWITCH(R.tenant_id_) {
      ObLSHandle ls_handle;
      ASSERT_EQ(OB_SUCCESS,
                MTL(ObLSService *)->get_ls(
                    ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
      ASSERT_EQ(OB_SUCCESS,
                ls_handle.get_ls()->get_tablet(
                    tablet_id, new_tablet_handle));
      ASSERT_NE(old_tablet_handle.get_obj(), new_tablet_handle.get_obj());
      ASSERT_GE(new_tablet_handle.get_obj()->get_snapshot_version(),
                new_snapshot_version);
      bool has_async_index = !race_case.updated_membership_;
      ASSERT_EQ(OB_SUCCESS,
                new_tablet_handle.get_obj()->get_has_async_index(
                    has_async_index));
      ASSERT_EQ(race_case.updated_membership_, has_async_index);
      ASSERT_TRUE(new_tablet_handle.get_obj()
                      ->async_index_membership_cache_.is_valid());
      ASSERT_EQ(race_case.updated_membership_,
                new_tablet_handle.get_obj()
                    ->async_index_membership_cache_.has_async_index());
    }
  }
}

// A failed CREATE-like transaction must not publish membership=true into either
// the tablet MDS table or its fast cache.
TEST_F(TestAsyncIndexRefreshInfoMds, aborted_true_membership_stays_false)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_async_idx_abort_true", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx_abort_true(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_abort_true", tablet_id, ls_id));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, false));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = true;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
  }
}

// A failed last-index DROP-like transaction must not overwrite an already
// committed membership=true value with false.
TEST_F(TestAsyncIndexRefreshInfoMds, aborted_false_membership_stays_true)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_async_idx_abort_false", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx_abort_false(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_abort_false", tablet_id, ls_id));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, true));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, false, false));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = false;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_TRUE(has_async_index);
  }
}

// Negative membership is a real committed state, not the absence of a row.
// Verify that replay restores false after the LS and tablet caches are rebuilt.
TEST_F(TestAsyncIndexRefreshInfoMds, committed_false_survives_ls_reboot)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_async_idx_replay_false", affected_rows));
  EQ(0, sql_proxy.write("create table t_async_idx_replay_false(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_async_idx_replay_false", tablet_id, ls_id));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, true, true));
  EQ(OB_SUCCESS, register_membership(tablet_id, ls_id, false, true));

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id));
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
    bool has_async_index = true;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
  }
}

// ---------------------------------------------------------------
// Test 4: get_kept_snapshot_for_async_index
//
// Case A: tablet has no async index → INT64_MAX (no refresh-info row)
// Case B: only the schema metadata bit is true, with no MDS row → membership false
// Case C: tablet has async index, [1000, 2000, 3000] registered → 1000 (minimum)
// Case D: tablet has async index, [5000] registered → 5000 (single entry)
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoMds, gc_snapshot_no_async_index)
{
  // A tablet without any async index must yield INT64_MAX.
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("drop table if exists t_gc_no_async_idx", affected_rows));
  EQ(0, sql_proxy.write("create table t_gc_no_async_idx(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_gc_no_async_idx", tablet_id, ls_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    bool has_async_index = true;
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    int64_t min_snapshot = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_kept_snapshot_for_async_index(min_snapshot));
    ASSERT_EQ(INT64_MAX, min_snapshot);
    LOGI("gc_snapshot_no_async_index ok", K(min_snapshot));
  }
}

TEST_F(TestAsyncIndexRefreshInfoMds, gc_snapshot_without_mds)
{
  // Without an MDS row the authoritative membership is false.
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("drop table if exists t_gc_async_no_info", affected_rows));
  EQ(0, sql_proxy.write("create table t_gc_async_no_info(c1 int, c2 varchar(256)) ORGANIZATION HEAP", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_gc_async_no_info", tablet_id, ls_id));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    bool has_async_index = false;
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_has_async_index(has_async_index));
    ASSERT_FALSE(has_async_index);
    int64_t min_snapshot = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_kept_snapshot_for_async_index(min_snapshot));
    ASSERT_EQ(INT64_MAX, min_snapshot);
    LOGI("gc_snapshot_without_mds ok", K(min_snapshot));
  }
}

TEST_F(TestAsyncIndexRefreshInfoMds, gc_snapshot_returns_minimum)
{
  // Register [1000, 2000, 3000]; must return the minimum: 1000.
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("drop table if exists t_gc_multi", affected_rows));
  EQ(0, sql_proxy.write("create table t_gc_multi(c1 int, c2 varchar(256)) ORGANIZATION HEAP", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_gc_multi", tablet_id, ls_id));

  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_     = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(1000));
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(2000));
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(3000));

  char buf[512];
  int64_t pos = 0;
  EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));

  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  EQ(0, conn->register_multi_data_source(R.tenant_id_, ls_id, ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                                                     buf, pos, flag));
  EQ(0, trans.end(true));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    int64_t min_snapshot = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_kept_snapshot_for_async_index(min_snapshot));
    ASSERT_EQ(1000, min_snapshot);
    LOGI("gc_snapshot_returns_minimum ok", K(min_snapshot));
  }
}

TEST_F(TestAsyncIndexRefreshInfoMds, gc_snapshot_single_entry)
{
  // Register [5000]; must return 5000.
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("drop table if exists t_gc_single", affected_rows));
  EQ(0, sql_proxy.write("create table t_gc_single(c1 int, c2 varchar(256)) ORGANIZATION HEAP", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_gc_single", tablet_id, ls_id));

  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_     = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  EQ(OB_SUCCESS, arg.info_.append_refresh_snapshot(5000));

  char buf[512];
  int64_t pos = 0;
  EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));

  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  EQ(0, conn->register_multi_data_source(R.tenant_id_, ls_id, ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                                                     buf, pos, flag));
  EQ(0, trans.end(true));

  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
    ObTabletHandle tablet_handle;
    EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

    int64_t min_snapshot = 0;
    EQ(OB_SUCCESS, tablet_handle.get_obj()->get_kept_snapshot_for_async_index(min_snapshot));
    ASSERT_EQ(5000, min_snapshot);
    LOGI("gc_snapshot_single_entry ok", K(min_snapshot));
  }
}

// ---------------------------------------------------------------
// Test 5: end
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshInfoMds, end)
{
  SERVER_LOG(INFO, "all tests done");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase::common;
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (OB_ISNULL(s3_bucket) || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = OB_NOT_NULL(s3_endpoint_override)
      && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *s3_ak = std::getenv("OB_MITTEST_S3_ACCESS_ID");
  if (OB_ISNULL(s3_ak) || '\0' == s3_ak[0]) {
    s3_ak = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_AK;
  }
  const char *s3_sk = std::getenv("OB_MITTEST_S3_ACCESS_KEY");
  if (OB_ISNULL(s3_sk) || '\0' == s3_sk[0]) {
    s3_sk = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_SK;
  }
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (OB_ISNULL(s3_region) || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  const int format_ret = databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("50G");
  GCONF.memory_limit.set_value("50G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
