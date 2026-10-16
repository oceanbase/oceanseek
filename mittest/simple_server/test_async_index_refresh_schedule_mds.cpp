/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/async_index/ob_async_index_deps.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::share;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y)
#define LOGI(fmt, ...) SERVER_LOG(INFO, fmt, ##__VA_ARGS__)

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};
TestRunCtx R;

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

// Resolve the ObLS* that owns ls_id within the current MTL tenant context.
static int get_ls(const ObLSID &ls_id, ObLSHandle &ls_handle, ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
    SERVER_LOG(WARN, "fail to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ls is null", K(ret), K(ls_id));
  }
  return ret;
}

class TestAsyncIndexRefreshScheduleMds : public ObSimpleClusterTestBase
{
public:
  TestAsyncIndexRefreshScheduleMds()
      : ObSimpleClusterTestBase("test_async_index_refresh_schedule_", "50G", "50G")
  {}
};

TEST_F(TestAsyncIndexRefreshScheduleMds, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(TestAsyncIndexRefreshScheduleMds, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("async_idx_sched_t", "20G", "20G", false, 4));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "async_idx_sched_t"));
  ASSERT_NE(0, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("async_idx_sched_t"));
}

// ---------------------------------------------------------------
// Reading the refresh info of a tablet that has never had an entry
// must succeed with an empty info (OB_EMPTY_RESULT is normalized).
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshScheduleMds, read_empty_returns_success)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_sched1", affected_rows));
  EQ(0, sql_proxy.write("create table t_sched1(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_sched1", tablet_id, ls_id));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    EQ(OB_SUCCESS, get_ls(ls_id, ls_handle, ls));

    ObAsyncIndexDepProvider provider;
    ObTabletAsyncIndexRefreshInfo info;
    EQ(OB_SUCCESS, provider.get_async_index_refresh_info(R.tenant_id_, ls, tablet_id, info));
    ASSERT_EQ(0, info.get_refresh_snapshots().count());
  }
}

// ---------------------------------------------------------------
// set_async_index_refresh_info (leader register) followed by
// get_async_index_refresh_info must round-trip the snapshot array.
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshScheduleMds, set_then_get_roundtrip)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_sched2", affected_rows));
  EQ(0, sql_proxy.write("create table t_sched2(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_sched2", tablet_id, ls_id));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    EQ(OB_SUCCESS, get_ls(ls_id, ls_handle, ls));

    ObAsyncIndexDepProvider provider;

    // write [1000, 2000, 3000] via the production set path
    ObTabletAsyncIndexRefreshInfo to_write;
    to_write.set_has_async_index(true);
    EQ(OB_SUCCESS, to_write.append_refresh_snapshot(1000));
    EQ(OB_SUCCESS, to_write.append_refresh_snapshot(2000));
    EQ(OB_SUCCESS, to_write.append_refresh_snapshot(3000));
    EQ(OB_SUCCESS, provider.set_async_index_refresh_info(R.tenant_id_, ls, tablet_id, to_write));

    // read back via the production get path
    ObTabletAsyncIndexRefreshInfo read_back;
    EQ(OB_SUCCESS, provider.get_async_index_refresh_info(R.tenant_id_, ls, tablet_id, read_back));
    ASSERT_TRUE(read_back.is_valid());
    ASSERT_EQ(3, read_back.get_refresh_snapshots().count());
    ASSERT_EQ(1000, read_back.get_refresh_snapshots().at(0));
    ASSERT_EQ(2000, read_back.get_refresh_snapshots().at(1));
    ASSERT_EQ(3000, read_back.get_refresh_snapshots().at(2));

    // a second write (append a target) overwrites the persisted info
    ObTabletAsyncIndexRefreshInfo updated;
    EQ(OB_SUCCESS, updated.assign(read_back));
    EQ(OB_SUCCESS, updated.append_refresh_snapshot(4000));
    EQ(OB_SUCCESS, provider.set_async_index_refresh_info(R.tenant_id_, ls, tablet_id, updated));

    ObTabletAsyncIndexRefreshInfo read_back2;
    EQ(OB_SUCCESS, provider.get_async_index_refresh_info(R.tenant_id_, ls, tablet_id, read_back2));
    ASSERT_EQ(4, read_back2.get_refresh_snapshots().count());
    ASSERT_EQ(4000, read_back2.get_refresh_snapshots().at(3));
  }
}

// ---------------------------------------------------------------
// After an LS reboot, the replay path must restore the refresh info
// previously written through the production set path.
// ---------------------------------------------------------------
TEST_F(TestAsyncIndexRefreshScheduleMds, set_then_reboot_replay)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists t_sched3", affected_rows));
  EQ(0, sql_proxy.write("create table t_sched3(c1 int)", affected_rows));

  ObTabletID tablet_id;
  ObLSID ls_id;
  EQ(0, get_tablet_and_ls("t_sched3", tablet_id, ls_id));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    EQ(OB_SUCCESS, get_ls(ls_id, ls_handle, ls));

    ObAsyncIndexDepProvider provider;
    ObTabletAsyncIndexRefreshInfo to_write;
    to_write.set_has_async_index(true);
    EQ(OB_SUCCESS, to_write.append_refresh_snapshot(5000));
    EQ(OB_SUCCESS, to_write.append_refresh_snapshot(6000));
    EQ(OB_SUCCESS, provider.set_async_index_refresh_info(R.tenant_id_, ls, tablet_id, to_write));
  }

  // Flush tx ctx and reboot the LS to force the replay path.
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id));
  LOGI("ls_reboot", K(ls_id));
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    EQ(OB_SUCCESS, get_ls(ls_id, ls_handle, ls));

    ObAsyncIndexDepProvider provider;
    ObTabletAsyncIndexRefreshInfo read_back;
    EQ(OB_SUCCESS, provider.get_async_index_refresh_info(R.tenant_id_, ls, tablet_id, read_back));
    ASSERT_TRUE(read_back.is_valid());
    ASSERT_EQ(2, read_back.get_refresh_snapshots().count());
    ASSERT_EQ(5000, read_back.get_refresh_snapshots().at(0));
    ASSERT_EQ(6000, read_back.get_refresh_snapshots().at(1));
    LOGI("read back after reboot ok", K(read_back));
  }
}

TEST_F(TestAsyncIndexRefreshScheduleMds, end)
{
  SERVER_LOG(INFO, "all tests done");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
