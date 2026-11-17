// owner: gengli.wzy
// owner group: transaction

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "share/ob_table_access_helper.h"
#include "storage/mds_table/ob_test_mds_types.h"
#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::storage::mds;

namespace {

void resolve_any_tablet_on_ls(const uint64_t tenant_id,
                             const share::ObLSID &ls_id,
                             common::ObTabletID &tablet_id)
{
  char where_tablet[512] = {0};
  databuff_printf(where_tablet, sizeof(where_tablet), "where ls_id = %ld", ls_id.id());
  GTEST_ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(
      tenant_id, {"tablet_id"}, OB_ALL_TABLET_TO_LS_TNAME, where_tablet, tablet_id));
}

void resolve_tablet_and_ls_on_tenant(const uint64_t tenant_id,
                                     const char *table_name,
                                     common::ObTabletID &tablet_id,
                                     share::ObLSID &ls_id)
{
  char where_table[512] = {0};
  databuff_printf(where_table, sizeof(where_table), "where table_name = '%s'", table_name);
  GTEST_ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(
      tenant_id, {"tablet_id"}, OB_ALL_TABLE_TNAME, where_table, tablet_id));

  char where_tablet[512] = {0};
  databuff_printf(where_tablet, sizeof(where_tablet), "where tablet_id = %ld", tablet_id.id());
  GTEST_ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(
      tenant_id, {"ls_id"}, OB_ALL_TABLET_TO_LS_TNAME, where_tablet, ls_id));
}

int register_test_mds_in_tx(observer::ObInnerSQLConnection *conn,
                            const uint64_t tenant_id,
                            const share::ObLSID &ls_id,
                            const common::ObTabletID &tablet_id,
                            const ObTxDataSourceType type,
                            const int64_t key,
                            const int64_t value,
                            const ObRegisterMdsFlag &flag)
{
  int ret = OB_SUCCESS;
  constexpr int64_t BUF_LEN = 256;
  char buffer[BUF_LEN];
  int64_t pos = 0;
  ObTestMdsRegisterArg arg;
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.key_ = key;
  arg.value_ = value;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(arg.serialize(buffer, BUF_LEN, pos))) {
  } else if (OB_FAIL(conn->register_multi_data_source(tenant_id, ls_id, type, buffer, pos, flag))) {
  }
  return ret;
}

} // namespace

#define EQ(x, y) GTEST_ASSERT_EQ(x, y);
#define NEQ(x, y) GTEST_ASSERT_NE(x, y);
#define LE(x, y) GTEST_ASSERT_LE(x, y);
#define GE(x, y) GTEST_ASSERT_GE(x, y);

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
};

TestRunCtx R;

class ObTxDataTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTxDataTest() : ObSimpleClusterTestBase("test_tx_data_", "50G", "50G") {}
};

TEST_F(ObTxDataTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

// 创建租户并不轻量，看场景必要性使用
TEST_F(ObTxDataTest, add_tenant)
{
  // 创建普通租户tt1
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "40G", "40G", false, 10));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_));
  ASSERT_NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObTxDataTest, create_new_ls)
{
  // 在单节点ObServer下创建新的日志流, 注意避免被RS任务GC掉
  EQ(0, SSH::create_ls(R.tenant_id_, get_curr_observer().self_addr_));
  int64_t ls_count = 0;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select count(ls_id) as val from __all_ls where ls_id!=1", ls_count));
  EQ(2, ls_count);
}

TEST_F(ObTxDataTest, rollback_to)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));
  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, SSH::write(conn1, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn1, "insert into stu1 values(100)", affected_rows));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn1, tx_id));
  LOGI("find_tx:%ld", tx_id.get_id());
  EQ(0, SSH::write(conn1, "savepoint sp1"));
  EQ(0, SSH::write(conn1, "insert into stu1 values(200)", affected_rows));
  EQ(0, SSH::write(conn1, "rollback to sp1"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);
  EQ(0, SSH::write(conn1, "commit"));
}

TEST_F(ObTxDataTest, rollback_to_with_redo)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));

  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, SSH::write(conn1, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn1, "insert into stu1 values(100)", affected_rows));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn1, tx_id));
  LOGI("find_tx:%ld", tx_id.get_id());

  EQ(0, SSH::write(conn1, "savepoint sp1"));
  EQ(0, SSH::write(conn1, "insert into stu1 values(200)", affected_rows));
  ObLSID loc1;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
  // when tx has redo, rollback to need write log
  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));

  EQ(0, SSH::write(conn1, "rollback to sp1"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);
  EQ(0, SSH::write(conn1, "commit"));
}

TEST_F(ObTxDataTest, rollback_to_with_read_sstable_uncommit)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));

  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, SSH::write(conn1, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn1, "insert into stu1 values(100)", affected_rows));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn1, tx_id));
  LOGI("find_tx:%ld", tx_id.get_id());

  EQ(0, SSH::write(conn1, "savepoint sp1"));
  EQ(0, SSH::write(conn1, "insert into stu1 values(200)", affected_rows));
  ObLSID loc1;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
  // when tx has redo, rollback to need write log
  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));

  EQ(0, SSH::write(conn1, "rollback to sp1"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);

  EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc1));
  // read from sstable uncommit row
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);

  EQ(0, SSH::write(conn1, "commit"));
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col1) as val from stu1",val));
  EQ(100, val);
}

TEST_F(ObTxDataTest, rollback_to_with_ls_replay)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));

  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, SSH::write(conn1, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn1, "insert into stu1 values(100)", affected_rows));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn1, tx_id));
  LOGI("find_tx:%ld", tx_id.get_id());

  EQ(0, SSH::write(conn1, "savepoint sp1"));
  EQ(0, SSH::write(conn1, "insert into stu1 values(200)", affected_rows));
  ObLSID loc1;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
  // when tx has redo, rollback to need write log
  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));

  EQ(0, SSH::write(conn1, "rollback to sp1"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);

  EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc1));
  // read from sstable uncommit row
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);

  LOGI("ls_reboot:%ld", loc1.id());
  // tx has not commit, tx ctx recover from tx_sstable
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));

  EQ(0, SSH::write(conn1, "commit"));
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col1) as val from stu1",val));
  EQ(100, val);
}

TEST_F(ObTxDataTest, rollback_to_with_ls_replay_from_middle)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));

  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, SSH::write(conn1, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn1, "insert into stu1 values(100)", affected_rows));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn1, tx_id));
  LOGI("find_tx:%ld", tx_id.get_id());

  EQ(0, SSH::write(conn1, "savepoint sp1"));
  EQ(0, SSH::write(conn1, "insert into stu1 values(200)", affected_rows));
  ObLSID loc1;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
  // when tx has redo, rollback to need write log
  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));

  EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc1));

  EQ(0, SSH::write(conn1, "rollback to sp1"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn1, "select sum(col1) val from stu1", val));
  EQ(100, val);

  EQ(0, SSH::write(conn1, "commit"));
  // make tx_ctx checkpoint
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));

  LOGI("ls_reboot:%ld", loc1.id());
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));

  EQ(0, SSH::select_int64(sql_proxy, "select sum(col1) as val from stu1",val));
  EQ(100, val);
}

TEST_F(ObTxDataTest, mds_tx_ls_reboot)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  ObLSID ls_id1(1001);
  ObLSID ls_id2(1002);
  common::ObTabletID tablet_on_ls1;
  common::ObTabletID tablet_on_ls2;
  resolve_any_tablet_on_ls(R.tenant_id_, ls_id1, tablet_on_ls1);
  EQ(0, register_test_mds_in_tx(conn, R.tenant_id_, ls_id1, tablet_on_ls1,
      ObTxDataSourceType::TEST_SINGLE_ROW, 0, 1, flag));

  EQ(0, SSH::submit_redo(R.tenant_id_, ls_id1));
  EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, ls_id1));

  resolve_any_tablet_on_ls(R.tenant_id_, ls_id2, tablet_on_ls2);
  EQ(0, register_test_mds_in_tx(conn, R.tenant_id_, ls_id2, tablet_on_ls2,
      ObTxDataSourceType::TEST_MULTI_ROW, 1, 2, flag));
  ObTransID tx_id;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select trans_id as val from __all_virtual_trans_stat where is_exiting=0 and session_id<=1 limit 1", tx_id.tx_id_));
  LOGI("find active_tx tx_id:%ld", tx_id.get_id());

  EQ(0, trans.end(true));
  // make tx_ctx checkpoint
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id1));
  LOGI("ls_reboot:%ld", ls_id1.id());
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id1));

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id2));
  LOGI("ls_reboot:%ld", ls_id2.id());
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id2));
}

TEST_F(ObTxDataTest, mds_tx_ls_reboot2)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  EQ(0, sql_proxy.write("drop table if exists stu1", affected_rows));
  EQ(0, sql_proxy.write("create table stu1(col1 int)", affected_rows));
  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  ObRegisterMdsFlag flag;
  common::ObTabletID tablet_id;
  share::ObLSID ls_id;
  resolve_tablet_and_ls_on_tenant(R.tenant_id_, "stu1", tablet_id, ls_id);
  EQ(0, register_test_mds_in_tx(conn, R.tenant_id_, ls_id, tablet_id,
      ObTxDataSourceType::TEST_SINGLE_ROW, 0, 1, flag));

  EQ(0, SSH::submit_redo(R.tenant_id_, ls_id));
  EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, ls_id));

  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));

  EQ(0, register_test_mds_in_tx(conn, R.tenant_id_, ls_id, tablet_id,
      ObTxDataSourceType::TEST_MULTI_ROW, 1, 2, flag));
  ObTransID tx_id;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select trans_id as val from __all_virtual_trans_stat where is_exiting=0 and session_id<=1 limit 1", tx_id.tx_id_));
  LOGI("find active_tx tx_id:%ld", tx_id.get_id());

  EQ(0, trans.end(true));
  // make tx_ctx checkpoint
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ls_id));
  // make tx_data checkpoint
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, ls_id));
  LOGI("ls_reboot:%ld", ls_id.id());
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));
}

TEST_F(ObTxDataTest, end)
{
  if (R.time_sec_ > 0) {
    ::sleep(R.time_sec_);
  }
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  oceanbase::unittest::R.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
