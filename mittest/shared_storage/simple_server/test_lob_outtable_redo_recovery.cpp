/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <cstdlib>
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/simple_server/env/ob_simple_server_restart_helper.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
const char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

static const char *TEST_NAME = "test_lob_outtable_redo_recovery";
static const char *TENANT_NAME = "lob_redo_recovery";
static constexpr int64_t LOB_BYTES = 2 * 1024 * 1024 + 17;

class LobRedoRecoveryBase : public ObSimpleClusterTestBase
{
public:
  LobRedoRecoveryBase()
    : ObSimpleClusterTestBase(TEST_NAME, "10G", "16G", "12G"), tenant_id_(OB_INVALID_TENANT_ID)
  {}

  ObMySQLProxy &inner_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  void init_user_(bool create)
  {
    if (create) {
      ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(TENANT_NAME, "4G", "4G", false, 1, "2G"));
    }
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_, TENANT_NAME));
    uint64_t data_version = 0;
    ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, data_version));
    ASSERT_GE(data_version, DATA_VERSION_4_6_3_1);
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, inner_proxy().write(
        tenant_id_, "SET GLOBAL ob_query_timeout = 60000000", affected_rows));
    ASSERT_EQ(OB_SUCCESS, inner_proxy().write(
        tenant_id_, "SET GLOBAL ob_trx_timeout = 120000000", affected_rows));
    ObSqlString user;
    ASSERT_EQ(OB_SUCCESS, user.assign_fmt("root@%s", TENANT_NAME));
    ASSERT_EQ(OB_SUCCESS, pool_.set_db_param(user.ptr(), "", "test"));
    ObAddr address;
    ASSERT_TRUE(address.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(),
        get_curr_simple_server().get_mysql_port()));
    ObConnPoolConfigParam config;
    config.sqlclient_wait_timeout_ = 1000;
    config.long_query_timeout_ = 300L * 1000 * 1000;
    config.connection_refresh_interval_ = 200L * 1000;
    config.connection_pool_warn_time_ = 10L * 1000 * 1000;
    config.sqlclient_per_observer_conn_limit_ = 10000;
    ASSERT_EQ(OB_SUCCESS, pool_.init(address, config));
    pool_.set_mode(sqlclient::ObMySQLConnection::MYSQL_MODE);
    ASSERT_EQ(OB_SUCCESS, user_proxy_.init(&pool_));
  }

  void get_meta_tablet_(ObLSHandle &ls_handle, ObTabletID &tablet_id)
  {
    schema::ObSchemaGetterGuard guard;
    const schema::ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, guard));
    ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, ObString("test"),
        ObString("lob_redo_recovery"), false, table_schema));
    ASSERT_NE(nullptr, table_schema);
    ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
        tenant_id_, table_schema->get_aux_lob_meta_tid(), table_schema));
    ASSERT_NE(nullptr, table_schema);
    tablet_id = table_schema->get_tablet_id();
    ObTabletIDArray tablet_ids;
    ObArray<ObLSID> ls_ids;
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
    ASSERT_EQ(OB_SUCCESS,
        ObTabletToLSTableOperator::batch_get_ls(inner_proxy(), tenant_id_, tablet_ids, ls_ids));
    ASSERT_EQ(1, ls_ids.count());
    ASSERT_NE(nullptr, MTL(ObLSService *));
    ASSERT_EQ(OB_SUCCESS,
        MTL(ObLSService *)->get_ls(ls_ids.at(0), ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls_handle.get_ls());
  }

  void collect_complete_keys_(const ObTabletID &tablet_id, ObIArray<ObOuttableLobControlKey> &keys)
  {
    keys.reuse();
    ObOuttableLobControlPageIterator iter;
    ObArenaAllocator allocator("LobRecoveryKeys");
    ASSERT_EQ(OB_SUCCESS, iter.init(inner_proxy(), 60L * 1000 * 1000, tenant_id_, 64));
    while (true) {
      allocator.reuse();
      ObOuttableLobControlRow row;
      const int ret = iter.get_next(allocator, row);
      if (OB_ITER_END == ret) {
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(row.key_.is_valid());
      ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO,
          row.key_.macro_id_.storage_object_type());
      ASSERT_EQ(tablet_id.id(), row.key_.macro_id_.third_id());
      if (OUTTABLE_LOB_CONTROL_COMPLETE == row.status_) {
        ASSERT_EQ(OB_SUCCESS, keys.push_back(row.key_));
      } else {
        ASSERT_EQ(OUTTABLE_LOB_CONTROL_DOING, row.status_);
      }
    }
  }

  void check_uncheckpointed_(ObLS &ls, const ObTabletID &tablet_id, const SCN &before_write)
  {
    ObTabletHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, handle,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
    ASSERT_NE(nullptr, handle.get_obj());
    ASSERT_LE(handle.get_obj()->get_clog_checkpoint_scn(), before_write);
  }

  void check_rows_()
  {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT LENGTH(v) AS bytes, "
        "MD5(v) = MD5(REPEAT('r', %ld)) AS matches FROM lob_redo_recovery WHERE id = 1",
        LOB_BYTES));
    {
      ObMySQLProxy::MySQLResult result;
      ASSERT_EQ(OB_SUCCESS, user_proxy_.read(result, sql.ptr()));
      ASSERT_NE(nullptr, result.get_result());
      ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
      int64_t bytes = 0;
      int64_t matches = 0;
      ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("bytes", bytes));
      ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("matches", matches));
      ASSERT_EQ(LOB_BYTES, bytes);
      ASSERT_EQ(1, matches);
      ASSERT_EQ(OB_ITER_END, result.get_result()->next());
    }
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, user_proxy_.read(result,
        "SELECT COUNT(*) AS cnt FROM lob_redo_recovery WHERE id = 2"));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    int64_t count = -1;
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", count));
    ASSERT_EQ(0, count);
  }

  uint64_t tenant_id_;
  sqlclient::ObSingleMySQLConnectionPool pool_;
  ObMySQLProxy user_proxy_;
};

class LobRedoBorn : public LobRedoRecoveryBase {};
class LobRedoRestart : public LobRedoRecoveryBase {};

TEST_F(LobRedoBorn, committed_objects_require_replay)
{
  ASSERT_NO_FATAL_FAILURE(init_user_(true));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, user_proxy_.write("CREATE TABLE lob_redo_recovery "
      "(id INT PRIMARY KEY, v LONGBLOB) LOB_INROW_THRESHOLD = 0 LOB_INTABLE_THRESHOLD = 0",
      affected_rows));
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
  ASSERT_TRUE(config.is_valid());
  ASSERT_FALSE(config->_no_logging);
  ObLSHandle ls_handle;
  ObTabletID meta_tablet_id;
  ASSERT_NO_FATAL_FAILURE(get_meta_tablet_(ls_handle, meta_tablet_id));
  ObLS &ls = *ls_handle.get_ls();
  ObArray<ObOuttableLobControlKey> keys;
  ASSERT_NO_FATAL_FAILURE(collect_complete_keys_(meta_tablet_id, keys));
  ASSERT_TRUE(keys.empty());
  SCN before_write;
  ASSERT_EQ(OB_SUCCESS, ls.get_end_scn(before_write));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "INSERT INTO lob_redo_recovery VALUES (1, REPEAT('r', %ld))", LOB_BYTES));
  ASSERT_EQ(OB_SUCCESS, user_proxy_.write(sql.ptr(), affected_rows));
  ASSERT_EQ(1, affected_rows);
  SCN after_write;
  ASSERT_EQ(OB_SUCCESS, ls.get_end_scn(after_write));
  ASSERT_GT(after_write, before_write);
  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy_, tenant_id_));
    DEFER(if (trans.is_started()) { EXPECT_EQ(OB_SUCCESS, trans.end(false)); });
    ASSERT_EQ(OB_SUCCESS,
        trans.write("INSERT INTO lob_redo_recovery VALUES (2, REPEAT('x', 17))", affected_rows));
    ASSERT_EQ(OB_SUCCESS, trans.end(false));
  }
  ASSERT_NO_FATAL_FAILURE(check_rows_());
  ASSERT_NO_FATAL_FAILURE(collect_complete_keys_(meta_tablet_id, keys));
  ASSERT_EQ(2, keys.count());
  ASSERT_NO_FATAL_FAILURE(check_uncheckpointed_(ls, meta_tablet_id, before_write));
  // Remove only this fresh tenant/table's committed logging objects, not the rollback object.
  for (int64_t i = 0; i < keys.count(); ++i) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS,
        ObLobOutTableIO::is_exist(keys.at(i).macro_id_, ls.get_ls_id().id(), exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(OB_SUCCESS,
        OB_STORAGE_OBJECT_MGR.delete_object(keys.at(i).macro_id_, 0, ls.get_ls_id().id()));
    ASSERT_EQ(OB_SUCCESS,
        ObLobOutTableIO::is_exist(keys.at(i).macro_id_, ls.get_ls_id().id(), exists));
    ASSERT_FALSE(exists);
  }
  // The fixture exits without shutdown; Meta redo must still be beyond the tablet checkpoint.
  ASSERT_NO_FATAL_FAILURE(check_uncheckpointed_(ls, meta_tablet_id, before_write));
}

TEST_F(LobRedoRestart, replay_restores_objects_and_meta_rows)
{
  ASSERT_NO_FATAL_FAILURE(init_user_(false));
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObLSHandle ls_handle;
  ObTabletID meta_tablet_id;
  ASSERT_NO_FATAL_FAILURE(get_meta_tablet_(ls_handle, meta_tablet_id));
  ObArray<ObOuttableLobControlKey> keys;
  ASSERT_NO_FATAL_FAILURE(collect_complete_keys_(meta_tablet_id, keys));
  ASSERT_EQ(2, keys.count());
  for (int64_t i = 0; i < keys.count(); ++i) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::is_exist(
        keys.at(i).macro_id_, ls_handle.get_ls()->get_ls_id().id(), exists));
    ASSERT_TRUE(exists);
  }
  ASSERT_NO_FATAL_FAILURE(check_rows_());
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (nullptr == bucket || '\0' == bucket[0]) {
    bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = nullptr != endpoint_override && '\0' != endpoint_override[0];
  const char *endpoint = use_test_endpoint ? endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *ak = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *sk = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *region = std::getenv("OB_MITTEST_S3_REGION");
  if (nullptr == region || '\0' == region[0]) {
    region = oceanbase::unittest::S3_REGION;
  }
  // Both forked phases must use the same object-storage prefix.
  char storage_info[1000] = {0};
  const int64_t now_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  int ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      bucket, now_ns, endpoint, ak, sk, region);
  if (oceanbase::common::OB_SUCCESS == ret) {
    oceanbase::shared_storage_info = storage_info;
    GCONF.ob_startup_mode.set_value("shared_storage");
    std::string gtest_log = "test_lob_outtable_redo_recovery_gtest.log";
    oceanbase::unittest::init_gtest_output(gtest_log);
    ObSimpleServerRestartHelper helper(argc, argv, oceanbase::storage::TEST_NAME,
        "LobRedoBorn", "LobRedoRestart");
    helper.set_sleep_sec(10);
    ret = helper.run();
  }
  return ret;
}
