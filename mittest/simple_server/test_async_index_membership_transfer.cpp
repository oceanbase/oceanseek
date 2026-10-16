/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

// owner: wenxiangjing.wxj
// owner group: storage

// Verify that async-index membership follows a data tablet through transfer
// and remains available after destination LS replay.

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "share/balance/ob_balance_job_table_operator.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::transaction;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y)
#define NEQ(x, y) GTEST_ASSERT_NE(x, y)

namespace
{

int register_async_index_membership(
    observer::ObInnerSQLConnection *conn,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  constexpr int64_t BUF_LEN = 512;
  char buffer[BUF_LEN];
  int64_t pos = 0;
  ObTabletAsyncIndexRefreshInfoArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.info_.set_has_async_index(true);
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(arg.serialize(buffer, BUF_LEN, pos))) {
    LOG_WARN("failed to serialize async index membership", KR(ret), K(arg));
  } else {
    ObRegisterMdsFlag flag;
    if (OB_FAIL(conn->register_multi_data_source(
            tenant_id,
            ls_id,
            ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
            buffer,
            pos,
            flag))) {
      LOG_WARN("failed to register async index membership",
          KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int get_async_index_membership(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    bool &has_async_index)
{
  int ret = OB_SUCCESS;
  has_async_index = false;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(
            ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(
                   tablet_id, tablet_handle))) {
      LOG_WARN("failed to get tablet", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_has_async_index(
                   has_async_index))) {
      LOG_WARN("failed to get async index membership",
          KR(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

} // namespace

class TestAsyncIndexMembershipTransfer : public ObSimpleClusterTestBase
{
public:
  TestAsyncIndexMembershipTransfer()
      : ObSimpleClusterTestBase(
            "test_async_index_membership_transfer_", "50G", "50G"),
        tenant_id_(OB_INVALID_TENANT_ID)
  {}

  int do_balance();
  int wait_balance_clean();

private:
  int do_balance_inner_();

private:
  uint64_t tenant_id_;
};

int TestAsyncIndexMembershipTransfer::do_balance_inner_()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_) {
    rootserver::ObTenantBalanceService *balance_service =
        MTL(rootserver::ObTenantBalanceService *);
    int64_t job_cnt = 0;
    int64_t start_time = OB_INVALID_TIMESTAMP;
    int64_t finish_time = OB_INVALID_TIMESTAMP;
    share::ObBalanceJob job;
    balance_service->reset();
    balance_service->stop();
    if (OB_FAIL(balance_service->gather_stat_())) {
      LOG_WARN("failed to gather balance stat", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(balance_service->gather_ls_status_stat(
                   tenant_id_, balance_service->ls_array_, true))) {
      LOG_WARN("failed to gather ls status stat", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(share::ObBalanceJobTableOperator::get_balance_job(
                   tenant_id_,
                   false,
                   *GCTX.sql_proxy_,
                   job,
                   start_time,
                   finish_time))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get balance job", KR(ret), K_(tenant_id));
      }
    } else if (OB_FAIL(balance_service->try_finish_current_job_(job, job_cnt))) {
      LOG_WARN("failed to finish balance job", KR(ret), K(job));
    }
    if (OB_SUCC(ret)
        && 0 == job_cnt
        && OB_FAIL(balance_service->partition_balance_(true))) {
      LOG_WARN("failed to do partition balance", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int TestAsyncIndexMembershipTransfer::do_balance()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_balance_inner_())) {
  } else if (OB_FAIL(do_balance_inner_())) {
  }
  return ret;
}

int TestAsyncIndexMembershipTransfer::wait_balance_clean()
{
  int ret = OB_SUCCESS;
  bool is_clean = false;
  const int64_t deadline = ObTimeUtil::current_time() + 60_s;
  while (OB_SUCC(ret)
         && !is_clean
         && ObTimeUtil::current_time() < deadline) {
    MTL_SWITCH(tenant_id_) {
      int64_t start_time = OB_INVALID_TIMESTAMP;
      int64_t finish_time = OB_INVALID_TIMESTAMP;
      share::ObBalanceJob job;
      ret = share::ObBalanceJobTableOperator::get_balance_job(
          tenant_id_,
          false,
          *GCTX.sql_proxy_,
          job,
          start_time,
          finish_time);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        int64_t transfer_task_count = 0;
        if (OB_FAIL(SSH::g_select_int64(
                tenant_id_,
                "select count(*) as val from __all_transfer_task",
                transfer_task_count))) {
          LOG_WARN("failed to query transfer task count",
              KR(ret), K_(tenant_id));
        } else {
          is_clean = (0 == transfer_task_count);
        }
      }
    }
    if (OB_SUCC(ret) && !is_clean) {
      if (OB_FAIL(do_balance())) {
        LOG_WARN("failed to advance balance job", KR(ret), K_(tenant_id));
      } else {
        ob_usleep(200_ms);
      }
    }
  }
  if (OB_SUCC(ret) && !is_clean) {
    ret = OB_TIMEOUT;
    LOG_WARN("wait balance clean timeout", KR(ret), K_(tenant_id));
  }
  return ret;
}

TEST_F(TestAsyncIndexMembershipTransfer, membership_survives_transfer_and_replay)
{
  static const char *TENANT_NAME = "async_idx_transfer";
  EQ(OB_SUCCESS, create_tenant(TENANT_NAME, "40G", "40G", false, 10));
  EQ(OB_SUCCESS, get_tenant_id(tenant_id_, TENANT_NAME));
  NEQ(OB_INVALID_TENANT_ID, tenant_id_);
  EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(TENANT_NAME));

  EQ(OB_SUCCESS,
      SSH::create_ls(tenant_id_, get_curr_observer().self_addr_));
  int64_t affected_rows = 0;
  EQ(OB_SUCCESS,
      GCTX.sql_proxy_->write(
          "alter system set _enable_parallel_table_creation = false tenant=all",
          affected_rows));
  EQ(OB_SUCCESS, wait_balance_clean());

  common::ObMySQLProxy &sql_proxy =
      get_curr_simple_server().get_sql_proxy2();
  EQ(OB_SUCCESS,
      sql_proxy.write("create database if not exists test", affected_rows));
  EQ(OB_SUCCESS, sql_proxy.write("use test", affected_rows));
  EQ(OB_SUCCESS,
      sql_proxy.write("drop tablegroup if exists tg_async_idx", affected_rows));
  rootserver::ObNewTableTabletAllocator::alloc_tablet_ls_offset_ = 0;
  EQ(OB_SUCCESS,
      sql_proxy.write("create table t_async_idx_1(c1 int)", affected_rows));
  EQ(OB_SUCCESS,
      sql_proxy.write("create table t_async_idx_2(c1 int)", affected_rows));

  share::ObLSID ls_id_1;
  share::ObLSID ls_id_2;
  common::ObTabletID tablet_id_1;
  common::ObTabletID tablet_id_2;
  EQ(OB_SUCCESS,
      SSH::select_table_loc(tenant_id_, "t_async_idx_1", ls_id_1));
  EQ(OB_SUCCESS,
      SSH::select_table_loc(tenant_id_, "t_async_idx_2", ls_id_2));
  NEQ(ls_id_1, ls_id_2);
  EQ(OB_SUCCESS,
      SSH::select_table_tablet(
          tenant_id_, "t_async_idx_1", tablet_id_1));
  EQ(OB_SUCCESS,
      SSH::select_table_tablet(
          tenant_id_, "t_async_idx_2", tablet_id_2));
  EQ(OB_SUCCESS,
      sql_proxy.write(
          "create tablegroup tg_async_idx sharding='NONE'", affected_rows));

  ObMySQLTransaction trans;
  EQ(OB_SUCCESS, trans.start(GCTX.sql_proxy_, tenant_id_));
  observer::ObInnerSQLConnection *conn =
      static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  EQ(OB_SUCCESS,
      register_async_index_membership(
          conn, tenant_id_, ls_id_1, tablet_id_1));
  EQ(OB_SUCCESS,
      register_async_index_membership(
          conn, tenant_id_, ls_id_2, tablet_id_2));
  EQ(OB_SUCCESS, trans.end(true));

  bool has_async_index_1 = false;
  bool has_async_index_2 = false;
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_1, tablet_id_1, has_async_index_1));
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_2, tablet_id_2, has_async_index_2));
  ASSERT_TRUE(has_async_index_1);
  ASSERT_TRUE(has_async_index_2);

  EQ(OB_SUCCESS,
      sql_proxy.write(
          "alter tablegroup tg_async_idx add "
          "t_async_idx_1,t_async_idx_2",
          affected_rows));
  const int64_t transfer_deadline = ObTimeUtil::current_time() + 60_s;
  while (ls_id_1 != ls_id_2
         && ObTimeUtil::current_time() < transfer_deadline) {
    EQ(OB_SUCCESS, do_balance());
    ob_usleep(200_ms);
    EQ(OB_SUCCESS,
        SSH::select_table_loc(tenant_id_, "t_async_idx_1", ls_id_1));
    EQ(OB_SUCCESS,
        SSH::select_table_loc(tenant_id_, "t_async_idx_2", ls_id_2));
  }
  EQ(ls_id_1, ls_id_2);
  EQ(OB_SUCCESS, wait_balance_clean());

  has_async_index_1 = false;
  has_async_index_2 = false;
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_1, tablet_id_1, has_async_index_1));
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_2, tablet_id_2, has_async_index_2));
  ASSERT_TRUE(has_async_index_1);
  ASSERT_TRUE(has_async_index_2);

  EQ(OB_SUCCESS, SSH::freeze_tx_ctx(tenant_id_, ls_id_1));
  EQ(OB_SUCCESS, SSH::ls_reboot(tenant_id_, ls_id_1));

  has_async_index_1 = false;
  has_async_index_2 = false;
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_1, tablet_id_1, has_async_index_1));
  EQ(OB_SUCCESS,
      get_async_index_membership(
          tenant_id_, ls_id_2, tablet_id_2, has_async_index_2));
  ASSERT_TRUE(has_async_index_1);
  ASSERT_TRUE(has_async_index_2);
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
