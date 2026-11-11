/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <cstdlib>
#include <string>
#include <gmock/gmock.h>

#define private public
#define protected public
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_gc_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_merge_task.h"
#include "sql/engine/dml/ob_dml_service.h"
#undef protected
#undef private

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/table/object_pool/ob_table_object_pool.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_utils.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/store/ob_outtable_lob_control_ctx.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_macro_id_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_meta_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_diff_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"

namespace oceanbase
{
const char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

class TestOuttableLobControlE2E : public ObSimpleClusterTestBase
{
public:
  TestOuttableLobControlE2E()
    : ObSimpleClusterTestBase("test_outtable_lob_control_e2e", "14G", "20G", "14G"),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_name_(nullptr)
  {}

  ObMySQLProxy &inner_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  ObMySQLProxy &user_proxy()
  {
    return user_sql_proxy_;
  }

  void init_user_(const char *tenant_name = "lob_ctrl_e2e", const char *log_disk_size = "4G");
  void create_lob_table_(const char *table_name);
  void check_table_api_write_(
      const ObString &credential,
      const table::ObTableOperationType::Type op_type,
      const int32_t id,
      const char fill_char,
      const int64_t byte_length,
      const int64_t expected_affected_rows);
  void collect_keys_(ObIArray<ObOuttableLobControlKey> &keys);
  void collect_new_keys_(
      const ObIArray<ObOuttableLobControlKey> &before,
      ObIArray<ObOuttableLobControlKey> &keys);
  void check_keys_(
      ObISQLClient &client,
      const ObIArray<ObOuttableLobControlKey> &keys,
      const int64_t expected_status);
  void check_lob_(
      ObISQLClient &client,
      const int64_t id,
      const char fill_char,
      const int64_t expected_bytes);
  void age_keys_(
      const ObIArray<ObOuttableLobControlKey> &keys,
      const int64_t expected_status);
  int append_key_condition_(const ObOuttableLobControlKey &key, ObSqlString &sql);
  void insert_gc_row_(int64_t id, char fill, int64_t bytes, bool erase,
      ObIArray<ObOuttableLobControlKey> &keys);
  void get_gc_tablets_(const ObIArray<ObOuttableLobControlKey> &keys,
      ObTabletID &meta_tablet_id, ObTabletIDArray &ctrl_tablet_ids);
  void freeze_gc_tablet_(const ObTabletID &tablet_id,
      ObSSGCTabletWithMetaScn &input, SCN &checkpoint);
  void check_physical_keys_(const ObSSGCTabletWithMetaScnArray &inputs,
      const ObIArray<ObOuttableLobControlKey> &keys, bool expected_present);
  void scan_and_diff_gc_(ObSSMarkSweepDag &dag);
  void check_gc_candidates_(const ObSSMarkSweepCtx &ctx, const ObSSLobCtrlGCScanFile &file,
      const ObIArray<ObOuttableLobControlKey> &keys, bool exact_keys);
  void check_remote_owner_complete_(const bool commit);

  uint64_t tenant_id_;
  const char *table_name_;
  sqlclient::ObSingleMySQLConnectionPool user_sql_pool_;
  ObMySQLProxy user_sql_proxy_;
};

void TestOuttableLobControlE2E::init_user_(const char *tenant_name, const char *log_disk_size)
{
  int64_t tenant_count = 0;
  {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT COUNT(*) AS cnt FROM oceanbase.__all_tenant "
        "WHERE tenant_name = '%s'", tenant_name));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, inner_proxy().read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", tenant_count));
  }
  ASSERT_GE(tenant_count, 0);
  ASSERT_LE(tenant_count, 1);
  if (0 == tenant_count) {
    ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry(
        tenant_name, "4G", log_disk_size, false, 1, "2G"));
  }
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_, tenant_name));
  ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id_);
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id_, data_version));
  ASSERT_GE(data_version, DATA_VERSION_4_6_3_1);

  // Set default session variables only for the test tenant before opening a real user connection.
  // Nine blocks exceed the default 16 MiB limit; raise REPEAT's result-size limit first.
  const char *session_defaults[] = {
      "SET GLOBAL tx_isolation = 'REPEATABLE-READ'",
      "SET GLOBAL max_allowed_packet = 67108864",
      "SET GLOBAL ob_query_timeout = 60000000",
      "SET GLOBAL ob_trx_timeout = 120000000",
  };
  int64_t affected_rows = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(session_defaults); ++i) {
    ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_, session_defaults[i], affected_rows));
  }
  if (!user_proxy().is_inited()) {
    // Keep each test's connection pool bound to its own tenant, including full-suite execution.
    ObSqlString user;
    ASSERT_EQ(OB_SUCCESS, user.assign_fmt("root@%s", tenant_name));
    ASSERT_EQ(OB_SUCCESS, user_sql_pool_.set_db_param(user.ptr(), "", "test"));
    ObAddr address;
    ASSERT_TRUE(address.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(),
        get_curr_simple_server().get_mysql_port()));
    ObConnPoolConfigParam pool_config;
    pool_config.sqlclient_wait_timeout_ = 1000;
    pool_config.long_query_timeout_ = 300L * 1000L * 1000L;
    pool_config.connection_refresh_interval_ = 200L * 1000L;
    pool_config.connection_pool_warn_time_ = 10L * 1000L * 1000L;
    pool_config.sqlclient_per_observer_conn_limit_ = 10000;
    ASSERT_EQ(OB_SUCCESS, user_sql_pool_.init(address, pool_config));
    user_sql_pool_.set_mode(sqlclient::ObMySQLConnection::MYSQL_MODE);
    ASSERT_EQ(OB_SUCCESS, user_sql_proxy_.init(&user_sql_pool_));
  }
  {
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, user_proxy().read(result,
        "SELECT @@tx_isolation = 'REPEATABLE-READ' AS is_repeatable_read"));
    sqlclient::ObMySQLResult *rows = result.get_result();
    ASSERT_NE(nullptr, rows);
    ASSERT_EQ(OB_SUCCESS, rows->next());
    int64_t is_repeatable_read = 0;
    ASSERT_EQ(OB_SUCCESS, rows->get_int("is_repeatable_read", is_repeatable_read));
    ASSERT_EQ(1, is_repeatable_read);
    ASSERT_EQ(OB_ITER_END, rows->next());
  }
}

void TestOuttableLobControlE2E::create_lob_table_(const char *table_name)
{
  table_name_ = table_name;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "CREATE TABLE test.%s (id INT PRIMARY KEY, v LONGBLOB) "
      "LOB_INROW_THRESHOLD = 0 LOB_INTABLE_THRESHOLD = 0", table_name_));
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(sql.ptr(), affected_rows));
}

int TestOuttableLobControlE2E::append_key_condition_(
    const ObOuttableLobControlKey &key,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("macro_id = "))) {
  } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(key.macro_id_, sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND hash = %lu", key.hash_))) {
  }
  return ret;
}

void TestOuttableLobControlE2E::collect_keys_(ObIArray<ObOuttableLobControlKey> &keys)
{
  keys.reuse();
  ObOuttableLobControlPageIterator iterator;
  ObArenaAllocator allocator("LobE2EKeys");
  ASSERT_EQ(OB_SUCCESS, iterator.init(inner_proxy(), 60L * 1000L * 1000L, tenant_id_, 64));
  while (true) {
    allocator.reuse();
    ObOuttableLobControlRow row;
    const int ret = iterator.get_next(allocator, row);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(row.key_.is_valid());
    ASSERT_EQ(OB_SUCCESS, keys.push_back(row.key_));
  }
}

void TestOuttableLobControlE2E::collect_new_keys_(
    const ObIArray<ObOuttableLobControlKey> &before,
    ObIArray<ObOuttableLobControlKey> &keys)
{
  ObArray<ObOuttableLobControlKey> current;
  ASSERT_NO_FATAL_FAILURE(collect_keys_(current));
  keys.reuse();
  for (int64_t i = 0; i < current.count(); ++i) {
    const ObOuttableLobControlKey &key = current.at(i);
    bool found = false;
    for (int64_t j = 0; !found && j < before.count(); ++j) {
      found = key.macro_id_ == before.at(j).macro_id_ && key.hash_ == before.at(j).hash_;
    }
    if (!found) {
      ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
    }
  }
}

void TestOuttableLobControlE2E::check_keys_(
    ObISQLClient &client,
    const ObIArray<ObOuttableLobControlKey> &keys,
    const int64_t expected_status)
{
  // Route internal connections by tenant; user connections already log in as root@tenant.
  // Pass 1 for user connections to keep the database and avoid synthesizing test_<tenant_id>.
  const uint64_t sql_tenant_id = (&client == &inner_proxy()) ? tenant_id_ : OB_SYS_TENANT_ID;
  for (int64_t i = 0; i < keys.count(); ++i) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS,
        sql.assign("SELECT status FROM oceanbase.__all_outtable_lob_control WHERE "));
    ASSERT_EQ(OB_SUCCESS, append_key_condition_(keys.at(i), sql));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, client.read(result, sql_tenant_id, sql.ptr()));
    sqlclient::ObMySQLResult *rows = result.get_result();
    ASSERT_NE(nullptr, rows);
    if (0 == expected_status) {
      ASSERT_EQ(OB_ITER_END, rows->next());
    } else {
      int64_t status = 0;
      ASSERT_EQ(OB_SUCCESS, rows->next());
      ASSERT_EQ(OB_SUCCESS, rows->get_int("status", status));
      ASSERT_EQ(expected_status, status);
      ASSERT_EQ(OB_ITER_END, rows->next());
    }
  }
}

void TestOuttableLobControlE2E::check_lob_(
    ObISQLClient &client,
    const int64_t id,
    const char fill_char,
    const int64_t expected_bytes)
{
  const uint64_t sql_tenant_id = (&client == &inner_proxy()) ? tenant_id_ : OB_SYS_TENANT_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT LENGTH(v) AS byte_length, MD5(v) = MD5(REPEAT('%c', %ld)) AS content_matches "
      "FROM test.%s WHERE id = %ld", fill_char, expected_bytes, table_name_, id));
  // LENGTH may read metadata only; MD5 requires reading the entire LOB value.
  // Keep the result local to this function so it is released before reusing the single connection.
  ObMySQLProxy::MySQLResult result;
  ASSERT_EQ(OB_SUCCESS, client.read(result, sql_tenant_id, sql.ptr()));
  sqlclient::ObMySQLResult *rows = result.get_result();
  ASSERT_NE(nullptr, rows);
  ASSERT_EQ(OB_SUCCESS, rows->next());
  int64_t byte_length = 0;
  int64_t content_matches = 0;
  ASSERT_EQ(OB_SUCCESS, rows->get_int("byte_length", byte_length));
  ASSERT_EQ(OB_SUCCESS, rows->get_int("content_matches", content_matches));
  ASSERT_EQ(expected_bytes, byte_length);
  ASSERT_EQ(1, content_matches);
  ASSERT_EQ(OB_ITER_END, rows->next());
}

void TestOuttableLobControlE2E::age_keys_(
    const ObIArray<ObOuttableLobControlKey> &keys,
    const int64_t expected_status)
{
  int64_t timeout_us = 0;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    ASSERT_TRUE(tenant_config.is_valid());
    timeout_us = tenant_config->_outtable_lob_fail_gc_timeout;
  }
  ASSERT_GT(timeout_us, 0);
  const int64_t old_time = ObTimeUtility::current_time_us() - timeout_us - 60L * 1000L * 1000L;
  ASSERT_GT(old_time, 0);
  // Age only full keys collected from these real writes, guarded by the expected status.
  // Keep the real GC path: do not fabricate control rows or directly change status or delete rows.
  for (int64_t i = 0; i < keys.count(); ++i) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "UPDATE oceanbase.__all_outtable_lob_control "
        "SET last_modify_time = usec_to_time(%ld) WHERE ", old_time));
    ASSERT_EQ(OB_SUCCESS, append_key_condition_(keys.at(i), sql));
    ASSERT_EQ(OB_SUCCESS, sql.append_fmt(" AND status = %ld", expected_status));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_, sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
  }
}

TEST_F(TestOuttableLobControlE2E, remote_owner_complete_rolls_back_with_t1)
{
  check_remote_owner_complete_(false /*commit*/);
}

TEST_F(TestOuttableLobControlE2E, remote_owner_complete_commits_with_t1)
{
  check_remote_owner_complete_(true /*commit*/);
}

void TestOuttableLobControlE2E::check_remote_owner_complete_(const bool commit)
{
  ASSERT_NO_FATAL_FAILURE(init_user_());
  ObOuttableLobControlRow doing_row;
  doing_row.key_.macro_id_ = blocksstable::MacroBlockId(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 2026102901 + commit, 0, 0, 0);
  doing_row.key_.hash_ = 0x4455667788990011ULL;
  doing_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt("DELETE FROM %s WHERE ",
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  ASSERT_EQ(OB_SUCCESS, append_key_condition_(doing_row.key_, cleanup_sql));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS,
        inner_proxy().write(tenant_id_, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  common::sqlclient::ObISQLConnectionPool *connection_pool = inner_proxy().get_pool();
  ASSERT_NE(nullptr, connection_pool);
  ASSERT_EQ(common::sqlclient::INNER_POOL, connection_pool->get_type());
  common::sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, connection_pool->acquire(tenant_id_, connection, &inner_proxy(), 0));
  ASSERT_NE(nullptr, connection);
  observer::ObInnerSQLConnection *inner_connection =
      static_cast<observer::ObInnerSQLConnection *>(connection);
  sql::ObSQLSessionInfo &session = inner_connection->get_session();
  const bool saved_remote_session = session.is_remote_session();
  sql::ObExecContext *saved_exec_ctx = session.get_cur_exec_ctx();
  ObArenaAllocator allocator("LobCtrlOwner");
  sql::ObSqlCtx parent_sql_ctx;
  parent_sql_ctx.stmt_type_ = sql::stmt::T_INSERT;
  sql::ObExecContext parent_exec_ctx(allocator);
  parent_exec_ctx.set_sql_ctx(&parent_sql_ctx);
  parent_exec_ctx.set_my_session(&session);
  parent_exec_ctx.set_sql_proxy(&inner_proxy());
  bool trans_started = false;
  DEFER({
    parent_exec_ctx.set_outtable_lob_control_owned_tx(nullptr);
    session.set_is_remote(saved_remote_session);
    session.set_cur_exec_ctx(saved_exec_ctx);
    int cleanup_ret = OB_SUCCESS;
    if (trans_started) {
      cleanup_ret = connection->rollback();
      EXPECT_EQ(OB_SUCCESS, cleanup_ret);
    }
    EXPECT_EQ(OB_SUCCESS, connection_pool->release(connection, OB_SUCCESS == cleanup_ret));
  });

  ASSERT_EQ(OB_SUCCESS, connection->execute_write(tenant_id_,
      "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", affected_rows));
  ASSERT_EQ(OB_SUCCESS, connection->start_transaction(tenant_id_, true /*with_snapshot*/));
  trans_started = true;
  ObArenaAllocator row_allocator("LobCtrlOwnerRow");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(*connection, tenant_id_,
      doing_row.key_.macro_id_, doing_row.key_.hash_, false /*for_update*/,
      row_allocator, row_scn, selected_row));
  transaction::ObTxDesc *tx_desc = session.get_tx_desc();
  ASSERT_NE(nullptr, tx_desc);
  ASSERT_TRUE(tx_desc->is_in_tx());
  ASSERT_FALSE(tx_desc->is_shadow());
  const transaction::ObTransID tx_id = tx_desc->get_tx_id();
  const SCN snapshot_scn = tx_desc->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot_scn.is_valid());
  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, manager.insert(doing_row));

  // Simulate a remote executor owning T1, not an RPC/PX worker borrowing A's transaction.
  ObOuttableLobControlCtx control_ctx;
  ASSERT_EQ(OB_SUCCESS, control_ctx.init(allocator));
  ASSERT_NE(nullptr, control_ctx.get_keys());
  ASSERT_EQ(OB_SUCCESS, control_ctx.get_keys()->push_back(doing_row.key_));
  ASSERT_EQ(nullptr, session.get_inner_conn());
  const bool had_start_stmt = session.has_start_stmt();
  session.set_is_remote(true);
  EXPECT_EQ(OB_NOT_SUPPORTED, control_ctx.complete(parent_exec_ctx));
  parent_exec_ctx.set_outtable_lob_control_owned_tx(tx_desc);
  {
    schema::ObSchemaGetterGuard schema_guard;
    const schema::ObTableSchema *non_control_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard));
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
        tenant_id_, OB_ALL_DUMMY_TID, non_control_schema));
    ASSERT_NE(nullptr, non_control_schema);
    ASSERT_FALSE(non_control_schema->is_aux_outtable_lob_control_table());
    sql::ObSqlCtx sql_ctx;
    sql_ctx.schema_guard_ = &schema_guard;
    sql::ObExecContext child_exec_ctx(allocator);
    child_exec_ctx.set_my_session(&session);
    child_exec_ctx.set_sql_ctx(&sql_ctx);
    const int64_t saved_nested_count = session.get_nested_count();
    const bool saved_scope =
        parent_exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_;
    DEFER({
      session.set_nested_count(saved_nested_count);
      parent_exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = saved_scope;
      session.set_cur_exec_ctx(saved_exec_ctx);
    });
    session.set_nested_count(1);
    parent_exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
    session.set_cur_exec_ctx(&parent_exec_ctx);
    sql::LinkExecCtxGuard link_guard(session, child_exec_ctx);
    ASSERT_TRUE(sql::ObSQLUtils::is_outtable_lob_control_owner_sql(&child_exec_ctx));
    bool allow_control_complete = true;
    ASSERT_EQ(OB_SUCCESS, sql::ObSQLUtils::check_outtable_lob_control_complete(
        child_exec_ctx, non_control_schema->get_table_id(), allow_control_complete));
    EXPECT_FALSE(allow_control_complete);
    // Owning T1 authorizes Control completion, not nested DML against another table.
    EXPECT_EQ(OB_NOT_SUPPORTED, sql::ObDMLService::check_nested_sql_legality(
        child_exec_ctx, non_control_schema->get_table_id()));
  }
  ASSERT_EQ(OB_SUCCESS, control_ctx.complete(parent_exec_ctx));
  EXPECT_TRUE(session.is_remote_session());
  EXPECT_EQ(saved_exec_ctx, session.get_cur_exec_ctx());
  EXPECT_EQ(nullptr, session.get_inner_conn());
  EXPECT_EQ(had_start_stmt, session.has_start_stmt());
  EXPECT_FALSE(parent_exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_);
  ASSERT_EQ(tx_desc, session.get_tx_desc());
  EXPECT_EQ(tx_id, tx_desc->get_tx_id());
  EXPECT_TRUE(tx_desc->is_in_tx());
  EXPECT_EQ(snapshot_scn, tx_desc->get_tx_snapshot_version());
  parent_exec_ctx.set_outtable_lob_control_owned_tx(nullptr);
  session.set_is_remote(saved_remote_session);
  // Simulate the outer statement boundary before reusing the owning inner connection.
  session.get_retry_info_for_update().reset();

  // COMPLETE is visible inside T1, while another reader still sees T2's committed DOING.
  ObSEArray<int64_t, 1> statuses;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select_status(
      *connection, tenant_id_, *control_ctx.get_keys(), false /*for_update*/, statuses));
  ASSERT_EQ(1, statuses.count());
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, statuses.at(0));
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(inner_proxy(), tenant_id_,
      doing_row.key_.macro_id_, doing_row.key_.hash_, false /*for_update*/,
      row_allocator, row_scn, selected_row));
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_DOING, selected_row.status_);
  ASSERT_EQ(OB_SUCCESS, commit ? connection->commit() : connection->rollback());
  trans_started = false;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(inner_proxy(), tenant_id_,
      doing_row.key_.macro_id_, doing_row.key_.hash_, false /*for_update*/,
      row_allocator, row_scn, selected_row));
  EXPECT_EQ(commit ? OUTTABLE_LOB_CONTROL_COMPLETE : OUTTABLE_LOB_CONTROL_DOING,
      selected_row.status_);
}

TEST_F(TestOuttableLobControlE2E, pdml_completion_commits_and_rolls_back_with_t1)
{
  ASSERT_NO_FATAL_FAILURE(init_user_());
  table_name_ = "lob_ctrl_pdml";
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "CREATE TABLE test.lob_ctrl_pdml_src (id INT PRIMARY KEY) "
      "PARTITION BY HASH(id) PARTITIONS 2", affected_rows));
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "INSERT INTO test.lob_ctrl_pdml_src VALUES (1), (2), (3), (4), (5), (6), (7), (8)",
      affected_rows));
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "CREATE TABLE test.lob_ctrl_pdml (id INT PRIMARY KEY, v LONGBLOB) "
      "LOB_INROW_THRESHOLD = 0 LOB_INTABLE_THRESHOLD = 0 "
      "PARTITION BY HASH(id) PARTITIONS 2", affected_rows));
  const int64_t lob_bytes = 90000;
  ObArray<ObOuttableLobControlKey> before;
  ObArray<ObOuttableLobControlKey> new_keys;
  for (int64_t round = 0; round < 3; ++round) {
    const bool autocommit = (2 == round);
    const bool commit = (0 != round);
    const int64_t id_offset = round * 100;
    SCOPED_TRACE(autocommit ? "autocommit" : (commit ? "commit" : "rollback"));
    ObSqlString insert_sql;
    ASSERT_EQ(OB_SUCCESS, insert_sql.assign_fmt(
        "INSERT /*+ ENABLE_PARALLEL_DML PARALLEL(2) */ INTO test.lob_ctrl_pdml "
        "SELECT id + %ld, REPEAT('p', %ld) FROM test.lob_ctrl_pdml_src",
        id_offset, lob_bytes));
    {
      ObSqlString explain_sql;
      ASSERT_EQ(OB_SUCCESS, explain_sql.assign_fmt("EXPLAIN %s", insert_sql.ptr()));
      ObMySQLProxy::MySQLResult result;
      ASSERT_EQ(OB_SUCCESS, user_proxy().read(result, explain_sql.ptr()));
      ASSERT_NE(nullptr, result.get_result());
      std::string plan;
      int ret = OB_SUCCESS;
      while (OB_SUCCESS == (ret = result.get_result()->next())) {
        ObString line;
        ASSERT_EQ(OB_SUCCESS, result.get_result()->get_varchar(0L, line));
        plan.append(line.ptr(), line.length()).append("\n");
      }
      ASSERT_EQ(OB_ITER_END, ret);
      const size_t qc_pos = plan.find("PX COORDINATOR");
      const size_t insert_pos = plan.find("INSERT");
      ASSERT_NE(std::string::npos, qc_pos) << plan;
      ASSERT_NE(std::string::npos, insert_pos) << plan;
      // INSERT must be below QC; a serial INSERT above a parallel SELECT is not PDML.
      ASSERT_LT(qc_pos, insert_pos) << plan;
    }
    ASSERT_NO_FATAL_FAILURE(collect_keys_(before));
    ObMySQLTransaction trans;
    if (!autocommit) {
      ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_, true /*with_snapshot*/));
    }
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false));
      }
    });
    if (autocommit) {
      ASSERT_EQ(OB_SUCCESS, user_proxy().write(insert_sql.ptr(), affected_rows));
    } else {
      ASSERT_EQ(OB_SUCCESS, trans.write(insert_sql.ptr(), affected_rows));
    }
    ASSERT_EQ(8, affected_rows);
    ASSERT_NO_FATAL_FAILURE(collect_new_keys_(before, new_keys));
    ASSERT_EQ(8, new_keys.count());
    if (!autocommit) {
      ASSERT_NO_FATAL_FAILURE(check_keys_(trans, new_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
      ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), new_keys, OUTTABLE_LOB_CONTROL_DOING));
      ASSERT_NO_FATAL_FAILURE(check_lob_(trans, id_offset + 1, 'p', lob_bytes));
      ASSERT_EQ(OB_SUCCESS, trans.end(commit));
    }
    ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), new_keys,
        commit ? OUTTABLE_LOB_CONTROL_COMPLETE : OUTTABLE_LOB_CONTROL_DOING));
    if (commit) {
      ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), id_offset + 1, 'p', lob_bytes));
    } else {
      ObMySQLProxy::MySQLResult result;
      ASSERT_EQ(OB_SUCCESS, user_proxy().read(result, "SELECT id FROM test.lob_ctrl_pdml"));
      ASSERT_NE(nullptr, result.get_result());
      ASSERT_EQ(OB_ITER_END, result.get_result()->next());
    }
  }
}

TEST_F(TestOuttableLobControlE2E, sql_writes_and_rollback_fail_gc)
{
  ASSERT_NO_FATAL_FAILURE(init_user_());
  ASSERT_NO_FATAL_FAILURE(create_lob_table_("lob_ctrl_e2e"));
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  const int64_t sizes[] = {8 * chunk_size + 17, chunk_size + 17,
                          2 * chunk_size + 17, chunk_size + 17};
  const char chars[] = {'a', 'b', 'c', 'd'};
  const int64_t expected_affected_rows[] = {1, 1, 2, 2};
  const char *statements[] = {
      "INSERT INTO test.lob_ctrl_e2e VALUES (1, REPEAT('a', %ld))",
      "UPDATE test.lob_ctrl_e2e SET v = REPEAT('b', %ld) WHERE id = 1",
      "REPLACE INTO test.lob_ctrl_e2e VALUES (1, REPEAT('c', %ld))",
      "INSERT INTO test.lob_ctrl_e2e VALUES (1, REPEAT('d', %ld)) "
      "ON DUPLICATE KEY UPDATE v = VALUES(v)",
  };
  ObArray<ObOuttableLobControlKey> before;
  ObArray<ObOuttableLobControlKey> new_keys;
  ObArray<ObOuttableLobControlKey> committed_keys;
  int64_t affected_rows = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(statements); ++i) {
    SCOPED_TRACE(statements[i]);
    ASSERT_NO_FATAL_FAILURE(collect_keys_(before));
    ObMySQLTransaction trans;
    // Take an RR snapshot on a real user connection; LOB writes then commit T2 independently.
    ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_, true /*with_snapshot*/));
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false));
      }
    });
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(statements[i], sizes[i]));
    ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
    ASSERT_EQ(expected_affected_rows[i], affected_rows);
    ASSERT_NO_FATAL_FAILURE(collect_new_keys_(before, new_keys));
    ASSERT_EQ((sizes[i] + chunk_size - 1) / chunk_size, new_keys.count());
    // COMPLETE belongs to T1; other connections see only T2's committed DOING until T1 commits.
    ASSERT_NO_FATAL_FAILURE(check_keys_(trans, new_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
    ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), new_keys, OUTTABLE_LOB_CONTROL_DOING));
    ASSERT_NO_FATAL_FAILURE(check_lob_(trans, 1, chars[i], sizes[i]));
    ASSERT_EQ(OB_SUCCESS, trans.end(true));
    ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), new_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
    ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 1, chars[i], sizes[i]));
    // Other cases share this tenant and may leave DOING rows after rollback.
    ASSERT_EQ(OB_SUCCESS, append(committed_keys, new_keys));
  }

  ObArray<ObOuttableLobControlKey> rollback_keys;
  ASSERT_FALSE(committed_keys.empty());
  {
    // Use the full tenant snapshot only to identify this transaction's new keys.
    ASSERT_NO_FATAL_FAILURE(collect_keys_(before));
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_, true /*with_snapshot*/));
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false));
      }
    });
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "INSERT INTO test.lob_ctrl_e2e VALUES (2, REPEAT('r', %ld))", chunk_size + 17));
    ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
    ASSERT_NO_FATAL_FAILURE(collect_new_keys_(before, rollback_keys));
    ASSERT_EQ(2, rollback_keys.count());
    ASSERT_NO_FATAL_FAILURE(check_keys_(trans, rollback_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
    ASSERT_NO_FATAL_FAILURE(check_lob_(trans, 2, 'r', chunk_size + 17));
    ASSERT_EQ(OB_SUCCESS, trans.end(false));
  }
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), rollback_keys, OUTTABLE_LOB_CONTROL_DOING));
  {
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS,
        user_proxy().read(result, "SELECT id FROM test.lob_ctrl_e2e WHERE id = 2"));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_ITER_END, result.get_result()->next());
  }

  // Only simulate aging of real leftover rows; use production Fail GC for transitions and deletion.
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ASSERT_GE(GET_MIN_CLUSTER_VERSION(), CLUSTER_VERSION_4_6_3_1);
  ObArenaAllocator dag_allocator("LobE2EFailGC");
  ObSSFailGCDag dag;
  ObSSFailGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_SUCCESS, param.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_NE(nullptr, dag.get_ctx());
  dag.get_ctx()->sql_timeout_us_ = 60L * 1000L * 1000L;
  ObSSLobCtrlFailGCTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
  ASSERT_NE(nullptr, task);
  ASSERT_EQ(OB_SUCCESS, task->init());
  ASSERT_NO_FATAL_FAILURE(age_keys_(committed_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_NO_FATAL_FAILURE(age_keys_(rollback_keys, OUTTABLE_LOB_CONTROL_DOING));
  ASSERT_EQ(OB_SUCCESS, task->process());
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), rollback_keys, OUTTABLE_LOB_CONTROL_FAIL));
  ASSERT_NO_FATAL_FAILURE(check_keys_(
      inner_proxy(), committed_keys, OUTTABLE_LOB_CONTROL_COMPLETE));

  // DOING-to-FAIL refreshes timestamps; age only this rollback's FAIL rows again for round two.
  ASSERT_NO_FATAL_FAILURE(age_keys_(rollback_keys, OUTTABLE_LOB_CONTROL_FAIL));
  ASSERT_EQ(OB_SUCCESS, task->process());
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), rollback_keys, 0 /*absent*/));
  ASSERT_NO_FATAL_FAILURE(check_keys_(
      inner_proxy(), committed_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 1, 'd', sizes[3]));
}

void TestOuttableLobControlE2E::insert_gc_row_(
    int64_t id, char fill, int64_t bytes, bool erase, ObIArray<ObOuttableLobControlKey> &keys)
{
  ObArray<ObOuttableLobControlKey> before;
  ASSERT_NO_FATAL_FAILURE(collect_keys_(before));
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&user_proxy(), tenant_id_));
  DEFER({
    if (trans.is_started()) {
      EXPECT_EQ(OB_SUCCESS, trans.end(false));
    }
  });
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("INSERT INTO test.%s VALUES (%ld, REPEAT('%c', %ld))",
      table_name_, id, fill, bytes));
  ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
  ASSERT_EQ(1, affected_rows);
  if (erase) {
    // A committed INSERT followed by DELETE in the same T1 leaves real COMPLETE rows behind.
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("DELETE FROM test.%s WHERE id = %ld", table_name_, id));
    ASSERT_EQ(OB_SUCCESS, trans.write(sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
  }
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_NO_FATAL_FAILURE(collect_new_keys_(before, keys));
  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  ASSERT_EQ((bytes + chunk_size - 1) / chunk_size, keys.count());
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), keys, OUTTABLE_LOB_CONTROL_COMPLETE));
}

void TestOuttableLobControlE2E::get_gc_tablets_(
    const ObIArray<ObOuttableLobControlKey> &keys,
    ObTabletID &meta_tablet_id,
    ObTabletIDArray &ctrl_tablet_ids)
{
  schema::ObSchemaGetterGuard guard;
  const schema::ObTableSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
      tenant_id_, ObString("test"), ObString(table_name_), false, schema));
  ASSERT_NE(nullptr, schema);
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, schema->get_aux_lob_meta_tid(), schema));
  ASSERT_NE(nullptr, schema);
  meta_tablet_id = schema->get_tablet_id();
  ASSERT_TRUE(meta_tablet_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, ObString("oceanbase"),
      ObString(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME), false, schema));
  ASSERT_NE(nullptr, schema);
  ASSERT_EQ(schema::PARTITION_FUNC_TYPE_HASH, schema->get_part_option().get_part_func_type());
  ASSERT_GT(schema->get_part_option().get_part_num(), 0);
  ctrl_tablet_ids.reuse();
  for (int64_t i = 0; i < keys.count(); ++i) {
    schema::ObBasePartition *partition = nullptr;
    ObObj hash;
    ObObj part_value;
    hash.set_uint64(keys.at(i).hash_);
    ASSERT_EQ(OB_SUCCESS, sql::ObExprFuncPartHashBase::calc_value_for_mysql(
        hash, part_value, ObUInt64Type));
    const int64_t part_idx = part_value.get_int() % schema->get_part_option().get_part_num();
    ASSERT_EQ(OB_SUCCESS, schema->get_part_by_idx(part_idx, OB_INVALID_INDEX, partition));
    ASSERT_NE(nullptr, partition);
    if (!has_exist_in_array(ctrl_tablet_ids, partition->get_tablet_id())) {
      ASSERT_EQ(OB_SUCCESS, ctrl_tablet_ids.push_back(partition->get_tablet_id()));
    }
  }
}

void TestOuttableLobControlE2E::freeze_gc_tablet_(
    const ObTabletID &tablet_id, ObSSGCTabletWithMetaScn &input, SCN &checkpoint)
{
  ObTabletIDArray tablet_ids;
  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
      inner_proxy(), tenant_id_, tablet_ids, ls_ids));
  ASSERT_EQ(1, ls_ids.count());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
      ls_ids.at(0), ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS &ls = *ls_handle.get_ls();
  ObTabletHandle before;
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, before,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  const SCN old_checkpoint = before.get_obj()->get_clog_checkpoint_scn();
  input.gc_tablet_ = ObSSGCTablet(
      ls_ids.at(0), tablet_id, before.get_obj()->get_reorganization_scn());
  const int64_t deadline_us = ObTimeUtility::current_time_us() + 180L * 1000L * 1000L;
  ASSERT_EQ(OB_SUCCESS, ls.tablet_freeze(
      tablet_id, true, deadline_us, false, ObFreezeSourceFlag::TEST_MODE));
  bool ready = false;
  while (!ready && ObTimeUtility::current_time_us() < deadline_us) {
    ObTabletHandle local;
    ASSERT_EQ(OB_SUCCESS, ls.get_tablet(tablet_id, local,
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
    checkpoint = local.get_obj()->get_clog_checkpoint_scn();
    ObSEArray<ObTableHandleV2, 8> memtables;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->get_all_memtables_from_memtable_mgr(memtables));
    bool has_frozen = false;
    for (int64_t i = 0; i < memtables.count(); ++i) {
      has_frozen |= memtables.at(i).get_table()->is_frozen_memtable();
    }
    if (!has_frozen && checkpoint >= old_checkpoint) {
      ObArenaAllocator allocator(ObMemAttr(tenant_id_, "LobGCUpload"));
      ObTabletHandle shared;
      const int ret = MTL(ObSSMetaService *)->get_tablet(input.gc_tablet_.ls_id_, tablet_id,
          input.gc_tablet_.transfer_scn_, allocator, shared, input.meta_scn_);
      ASSERT_TRUE(OB_SUCCESS == ret || OB_TABLET_NOT_EXIST == ret) << ret;
      ready = OB_SUCCESS == ret && shared.is_valid()
          && shared.get_obj()->get_clog_checkpoint_scn() >= checkpoint;
      if (ready) {
        checkpoint = shared.get_obj()->get_clog_checkpoint_scn();
      }
    }
    if (!ready) {
      ob_usleep(100L * 1000L);
    }
  }
  ASSERT_TRUE(ready) << tablet_id.id() << " old_checkpoint=" << old_checkpoint.get_val_for_tx();
  ASSERT_TRUE(input.is_valid());
}

void TestOuttableLobControlE2E::check_physical_keys_(
    const ObSSGCTabletWithMetaScnArray &inputs,
    const ObIArray<ObOuttableLobControlKey> &keys,
    bool expected_present)
{
  ObArray<blocksstable::MacroBlockId> ids;
  for (int64_t i = 0; i < inputs.count(); ++i) {
    ObSSMarkSweepMacroIDIterator iter;
    ObSEArray<SCN, 1> versions;
    ASSERT_EQ(OB_SUCCESS, versions.push_back(inputs.at(i).meta_scn_));
    ASSERT_EQ(OB_SUCCESS, iter.init(inner_proxy(), tenant_id_, inputs.at(i).gc_tablet_,
        versions));
    blocksstable::MacroBlockId id;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter.get_next(id))) {
      ASSERT_EQ(OB_SUCCESS, ids.push_back(id));
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
  for (int64_t i = 0; i < keys.count(); ++i) {
    ASSERT_EQ(expected_present, has_exist_in_array(ids, keys.at(i).macro_id_))
        << "hash=" << keys.at(i).hash_;
  }
}

void TestOuttableLobControlE2E::scan_and_diff_gc_(ObSSMarkSweepDag &dag)
{
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_EQ(OB_SUCCESS, ctx.lob_meta_pipeline_.init(
      tenant_id_, ctx.memory_limit_, ctx.file_buf_size_, 0, &ctx.compare_));
  ASSERT_EQ(OB_SUCCESS, ctx.lob_ctrl_scan_file_mgr_.init(tenant_id_));
  ObSSLobMetaGCScanTask meta_scan;
  meta_scan.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, meta_scan.init(0, ctx.lob_meta_scan_inputs_.count()));
  ASSERT_EQ(OB_SUCCESS, meta_scan.process());
  ASSERT_EQ(OB_SUCCESS, ctx.lob_meta_pipeline_.prepare_merge());
  int64_t kept_count = 0;
  ASSERT_EQ(OB_SUCCESS, ctx.lob_meta_pipeline_.get_input_count(
      ObMarkSweepGroupType::KEPT, kept_count));
  ASSERT_EQ(1, kept_count);
  ASSERT_EQ(OB_SUCCESS, ctx.lob_meta_pipeline_.complete_group(ObMarkSweepGroupType::KEPT));

  // Produce several real scan files and merge them; deletion below uses a two-row batch size.
  for (int64_t i = 0; i < ctx.lob_ctrl_scan_inputs_.count(); ++i) {
    ObSSLobCtrlGCScanTask ctrl_scan;
    ctrl_scan.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, ctrl_scan.init(i, i + 1));
    ASSERT_EQ(OB_SUCCESS, ctrl_scan.process());
  }
  ASSERT_FALSE(ctx.lob_ctrl_scan_files_.empty());
  ObSSLobCtrlGCMergeTask merge;
  merge.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, merge.merge_batch_(ctx, ctx.lob_ctrl_scan_files_, 0,
      ctx.lob_ctrl_scan_files_.count(), ctx.lob_ctrl_merged_file_));
  ASSERT_TRUE(ctx.lob_ctrl_merged_file_.is_valid());
  ObSSLobCtrlGCDiffTask diff;
  diff.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, diff.init());
  ASSERT_EQ(OB_SUCCESS, diff.process());
  ASSERT_TRUE(ctx.lob_ctrl_diff_file_.is_valid());
}

void TestOuttableLobControlE2E::check_gc_candidates_(
    const ObSSMarkSweepCtx &ctx, const ObSSLobCtrlGCScanFile &file,
    const ObIArray<ObOuttableLobControlKey> &keys, bool exact_keys)
{
  ObArenaAllocator allocator("LobGCDiffCheck");
  ObArray<int64_t> row_scns;
  ObArray<bool> current_versions_found;
  for (int64_t i = 0; i < keys.count(); ++i) {
    ObOuttableLobControlRow row;
    int64_t row_scn = 0;
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(inner_proxy(), tenant_id_,
        keys.at(i).macro_id_, keys.at(i).hash_, false, allocator, row_scn, row));
    ASSERT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, row.status_);
    ASSERT_EQ(OB_SUCCESS, row_scns.push_back(row_scn));
    ASSERT_EQ(OB_SUCCESS, current_versions_found.push_back(false));
  }
  ASSERT_TRUE(file.is_valid());
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(OB_SUCCESS, reader.init_for_pread(file.get_fd(), file.get_dir_id(),
      0, tenant_id_, file.sample_item_, ctx.file_buf_size_));
  ASSERT_EQ(OB_SUCCESS, reader.open());
  int64_t count = 0;
  const ObSSOuttableLobCtrlCandidate *item = nullptr;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = reader.get_next_item(item))) {
    ASSERT_NE(nullptr, item);
    ASSERT_LT(item->mark_row_scn_, ctx.min_outtable_data_scn_.get_val_for_tx());
    bool found = false;
    for (int64_t i = 0; i < keys.count(); ++i) {
      if (item->macro_id_ == keys.at(i).macro_id_ && item->hash_ == keys.at(i).hash_) {
        found = true;
        current_versions_found.at(i) = current_versions_found.at(i)
            || item->mark_row_scn_ == row_scns.at(i);
      }
    }
    if (exact_keys) {
      ASSERT_TRUE(found) << "hash=" << item->hash_;
    }
    ++count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(file.item_count_, count);
  // Older DOING versions may coexist, but each requested current COMPLETE version must be present.
  for (int64_t i = 0; i < keys.count(); ++i) {
    ASSERT_TRUE(current_versions_found.at(i)) << "hash=" << keys.at(i).hash_;
  }
}

TEST_F(TestOuttableLobControlE2E, shared_versions_to_complete_row_gc)
{
  ASSERT_NO_FATAL_FAILURE(init_user_("lob_ctrl_gc_e2e", "3G"));
  ASSERT_NO_FATAL_FAILURE(create_lob_table_("lob_ctrl_gc_e2e"));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET _shared_inc_sstable_upload_interval = '1s'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET inc_sstable_upload_thread_score = 20", affected_rows));
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET _shared_advance_checkpoint_interval = '1m'", affected_rows));
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObSSGCService *service = MTL(ObSSGCService *);
  ASSERT_NE(nullptr, service);
  service->stop();
  service->wait();
  DEFER(EXPECT_EQ(OB_SUCCESS, service->start()));
  volatile bool is_stopped = false;
  ObSSMarkSweepGCGenTask gen(is_stopped);
  ASSERT_EQ(OB_SUCCESS, gen.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, gen.cancel_dag_if_exist_());
  const int64_t cancel_deadline_us = ObTimeUtility::current_time_us() + 60L * 1000L * 1000L;
  bool dag_exists = true;
  while (dag_exists && ObTimeUtility::current_time_us() < cancel_deadline_us) {
    ASSERT_EQ(OB_SUCCESS, gen.check_dag_exist_(dag_exists));
    if (dag_exists) {
      ob_usleep(100L * 1000L);
    }
  }
  ASSERT_FALSE(dag_exists);
  ASSERT_EQ(OB_SUCCESS, gen.try_release_executor_());
  bool acquired = false;
  bool check_old_executor = false;
  int64_t old_executor = 0;
  ASSERT_EQ(OB_SUCCESS, gen.try_acquire_executor_(acquired, check_old_executor, old_executor));
  ASSERT_TRUE(acquired);
  DEFER(EXPECT_EQ(OB_SUCCESS, gen.try_release_executor_()));

  const int64_t chunk_size = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  ObArray<ObOuttableLobControlKey> live_keys;
  ObArray<ObOuttableLobControlKey> orphan_keys;
  ObArray<ObOuttableLobControlKey> late_keys;
  ObArray<ObOuttableLobControlKey> all_keys;
  ObArray<ObOuttableLobControlKey> newer_live_keys;
  ObArray<ObOuttableLobControlKey> old_live_keys;
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(2, 'o', 3 * chunk_size + 17, true, orphan_keys));
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(1, 'k', chunk_size + 17, false, live_keys));
  ASSERT_EQ(OB_SUCCESS, old_live_keys.assign(live_keys));
  // Reusing the payload touches the orphan's hash partitions with newer live rows.
  // Their real checkpoints can then cover the old candidates without rewriting candidate SCNs.
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(4, 'o', 3 * chunk_size + 17, false, newer_live_keys));
  ASSERT_EQ(OB_SUCCESS, append(live_keys, newer_live_keys));
  newer_live_keys.reuse();
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(5, 'k', chunk_size + 17, false, newer_live_keys));
  ASSERT_EQ(OB_SUCCESS, append(live_keys, newer_live_keys));
  ASSERT_EQ(OB_SUCCESS, append(all_keys, live_keys));
  ASSERT_EQ(OB_SUCCESS, append(all_keys, orphan_keys));
  ObTabletID meta_tablet_id;
  ObTabletIDArray ctrl_tablet_ids;
  ASSERT_NO_FATAL_FAILURE(get_gc_tablets_(all_keys, meta_tablet_id, ctrl_tablet_ids));
  ObSSGCTabletWithMetaScn meta_input;
  SCN meta_checkpoint;
  ASSERT_NO_FATAL_FAILURE(freeze_gc_tablet_(meta_tablet_id, meta_input, meta_checkpoint));

  // The later orphan is present in the Ctrl files, but newer than the frozen Meta checkpoint.
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(3, 'n', chunk_size + 17, true, late_keys));
  ASSERT_EQ(OB_SUCCESS, append(all_keys, late_keys));
  ASSERT_NO_FATAL_FAILURE(get_gc_tablets_(all_keys, meta_tablet_id, ctrl_tablet_ids));
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(tenant_id_, gen.round_id_, 16L << 20, 2L << 20, 2));
  ASSERT_EQ(OB_SUCCESS, param.tablet_with_meta_scn_arr_.push_back(meta_input));
  SCN max_checkpoint = meta_checkpoint;
  SCN min_checkpoint = meta_checkpoint;
  for (int64_t i = 0; i < ctrl_tablet_ids.count(); ++i) {
    ObSSGCTabletWithMetaScn input;
    SCN checkpoint;
    ASSERT_NO_FATAL_FAILURE(freeze_gc_tablet_(ctrl_tablet_ids.at(i), input, checkpoint));
    min_checkpoint = SCN::min(min_checkpoint, checkpoint);
    max_checkpoint = SCN::max(max_checkpoint, checkpoint);
    ASSERT_EQ(OB_SUCCESS, param.tablet_with_meta_scn_arr_.push_back(input));
  }
  // Exercise real stage inputs, not tenant-wide Study scheduling or physical object deletion.
  // Only the test table and its touched Ctrl partitions participate; all versions come from upload.
  const int64_t deadline_us = ObTimeUtility::current_time_us() + 180L * 1000L * 1000L;
  bool checkpoint_ready = false;
  while (!checkpoint_ready && ObTimeUtility::current_time_us() < deadline_us) {
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_max_committed_meta_scn(
        param.snapshot_meta_scn_));
    param.safe_meta_scn_ = param.snapshot_meta_scn_;
    ASSERT_EQ(OB_SUCCESS, gen.get_all_ls_ss_checkpoint_scn_(param.snapshot_meta_scn_,
        param.safe_meta_scn_, param.reserved_meta_scns_, SCN(),
        param.ls_ss_checkpoint_scn_map_, param.recyclable_deleted_ls_ids_));
    checkpoint_ready = true;
    for (int64_t i = 0; i < param.tablet_with_meta_scn_arr_.count(); ++i) {
      SCN checkpoint;
      ASSERT_EQ(OB_SUCCESS, param.ls_ss_checkpoint_scn_map_.get_refactored(
          param.tablet_with_meta_scn_arr_.at(i).gc_tablet_.ls_id_, checkpoint));
      checkpoint_ready = checkpoint_ready && checkpoint > max_checkpoint;
    }
    if (!checkpoint_ready) {
      ob_usleep(500L * 1000L);
    }
  }
  ASSERT_TRUE(checkpoint_ready);
  ObArenaAllocator allocator("LobGCE2E");
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSMarkSweepPrepareTask prepare;
  prepare.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, prepare.init());
  ASSERT_EQ(OB_SUCCESS, prepare.process());
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ObSSOuttableGCTask user_gc;
  user_gc.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, user_gc.init());
  ASSERT_EQ(OB_SUCCESS, user_gc.prepare_scan_inputs_(ctx, ctx.lob_meta_scan_inputs_,
      ctx.lob_ctrl_scan_inputs_, ctx.min_outtable_data_scn_));
  ASSERT_EQ(1, ctx.lob_meta_scan_inputs_.count());
  ASSERT_EQ(meta_input.meta_scn_, ctx.lob_meta_scan_inputs_.at(0).meta_scn_);
  ASSERT_EQ(min_checkpoint, ctx.min_outtable_data_scn_);
  ASSERT_EQ(ctrl_tablet_ids.count(), ctx.lob_ctrl_scan_inputs_.count());
  for (int64_t i = 0; i < late_keys.count(); ++i) {
    ObOuttableLobControlRow row;
    int64_t row_scn = 0;
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(inner_proxy(), tenant_id_,
        late_keys.at(i).macro_id_, late_keys.at(i).hash_, false, allocator, row_scn, row));
    ASSERT_GT(row_scn, meta_checkpoint.get_val_for_tx());
  }
  ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_meta_scan_inputs_, live_keys, true));
  ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_meta_scan_inputs_, orphan_keys, false));
  ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_meta_scan_inputs_, late_keys, false));
  ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_ctrl_scan_inputs_, all_keys, true));
  ASSERT_NO_FATAL_FAILURE(scan_and_diff_gc_(dag));
  ASSERT_NO_FATAL_FAILURE(check_gc_candidates_(
      ctx, ctx.lob_ctrl_merged_file_, old_live_keys, false /*exact_keys*/));
  ASSERT_NO_FATAL_FAILURE(check_gc_candidates_(
      ctx, ctx.lob_ctrl_diff_file_, orphan_keys, true /*exact_keys*/));
  ObSSLobCtrlGCDeleteTask delete_task;
  delete_task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, delete_task.init());
  ASSERT_EQ(OB_SUCCESS, delete_task.process());
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), orphan_keys, 0 /*absent*/));
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), live_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), late_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 1, 'k', chunk_size + 17));
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 4, 'o', 3 * chunk_size + 17));
  // Re-consuming the same physical candidates must be harmless after successful deletion.
  ASSERT_EQ(OB_SUCCESS, delete_task.process());
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), live_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), late_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
}

TEST_F(TestOuttableLobControlE2E, cold_empty_control_checkpoints_do_not_pin_gc)
{
  ASSERT_NO_FATAL_FAILURE(init_user_("lob_ctrl_cold_gc_e2e", "3G"));
  ASSERT_NO_FATAL_FAILURE(create_lob_table_("lob_ctrl_cold_gc_e2e"));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET _shared_inc_sstable_upload_interval = '1s'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET inc_sstable_upload_thread_score = 20", affected_rows));
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET _shared_advance_checkpoint_interval = '1m'", affected_rows));
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObSSGCService *service = MTL(ObSSGCService *);
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, service);
  ASSERT_NE(nullptr, meta_service);
  service->stop();
  service->wait();
  DEFER(EXPECT_EQ(OB_SUCCESS, service->start()));
  volatile bool is_stopped = false;
  ObSSMarkSweepGCGenTask gen(is_stopped);
  ASSERT_EQ(OB_SUCCESS, gen.init(tenant_id_));
  ASSERT_EQ(OB_SUCCESS, gen.cancel_dag_if_exist_());
  const int64_t cancel_deadline_us = ObTimeUtility::current_time_us() + 60L * 1000L * 1000L;
  bool dag_exists = true;
  while (dag_exists && ObTimeUtility::current_time_us() < cancel_deadline_us) {
    ASSERT_EQ(OB_SUCCESS, gen.check_dag_exist_(dag_exists));
    if (dag_exists) {
      ob_usleep(100L * 1000L);
    }
  }
  ASSERT_FALSE(dag_exists);
  ASSERT_EQ(OB_SUCCESS, gen.try_release_executor_());
  bool acquired = false;
  bool check_old_executor = false;
  int64_t old_executor = 0;
  ASSERT_EQ(OB_SUCCESS, gen.try_acquire_executor_(acquired, check_old_executor, old_executor));
  ASSERT_TRUE(acquired);
  DEFER(EXPECT_EQ(OB_SUCCESS, gen.try_release_executor_()));

  ObArray<ObOuttableLobControlKey> existing_keys;
  ASSERT_NO_FATAL_FAILURE(collect_keys_(existing_keys));
  ASSERT_TRUE(existing_keys.empty());
  const int64_t bytes = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  ObArray<ObOuttableLobControlKey> orphan_keys;
  ObArray<ObOuttableLobControlKey> live_keys;
  ObArray<ObOuttableLobControlKey> all_keys;
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(2, 'o', bytes, true, orphan_keys));
  ASSERT_NO_FATAL_FAILURE(insert_gc_row_(1, 'k', bytes, false, live_keys));
  ASSERT_EQ(1, orphan_keys.count());
  ASSERT_EQ(1, live_keys.count());
  ASSERT_NE(orphan_keys.at(0).hash_, live_keys.at(0).hash_);
  ASSERT_EQ(OB_SUCCESS, append(all_keys, orphan_keys));
  ASSERT_EQ(OB_SUCCESS, append(all_keys, live_keys));
  ObTabletID meta_tablet_id;
  ObTabletIDArray touched_ctrl_ids;
  ASSERT_NO_FATAL_FAILURE(get_gc_tablets_(all_keys, meta_tablet_id, touched_ctrl_ids));

  ObTabletIDArray all_ctrl_ids;
  {
    schema::ObSchemaGetterGuard guard;
    const schema::ObTableSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, guard));
    ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, ObString("oceanbase"),
        ObString(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME), false, schema));
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(128, schema->get_part_option().get_part_num());
    for (int64_t i = 0; i < schema->get_part_option().get_part_num(); ++i) {
      schema::ObBasePartition *partition = nullptr;
      ASSERT_EQ(OB_SUCCESS, schema->get_part_by_idx(i, OB_INVALID_INDEX, partition));
      ASSERT_NE(nullptr, partition);
      ASSERT_EQ(OB_SUCCESS, all_ctrl_ids.push_back(partition->get_tablet_id()));
    }
  }
  ObArray<ObLSID> ctrl_ls_ids;
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
      inner_proxy(), tenant_id_, all_ctrl_ids, ctrl_ls_ids));
  ASSERT_EQ(all_ctrl_ids.count(), ctrl_ls_ids.count());

  // Freeze only written tablets. The untouched Control partitions must retain their initial CP.
  ObSSGCTabletWithMetaScn uploaded_meta;
  SCN max_checkpoint;
  ASSERT_NO_FATAL_FAILURE(freeze_gc_tablet_(meta_tablet_id, uploaded_meta, max_checkpoint));
  for (int64_t i = 0; i < touched_ctrl_ids.count(); ++i) {
    ObSSGCTabletWithMetaScn uploaded_ctrl;
    SCN checkpoint;
    ASSERT_NO_FATAL_FAILURE(freeze_gc_tablet_(touched_ctrl_ids.at(i), uploaded_ctrl, checkpoint));
    max_checkpoint = SCN::max(max_checkpoint, checkpoint);
  }
  ObArenaAllocator row_allocator(ObMemAttr(tenant_id_, "LobColdRow"));
  ObOuttableLobControlRow orphan_row;
  int64_t orphan_row_scn = 0;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(inner_proxy(), tenant_id_,
      orphan_keys.at(0).macro_id_, orphan_keys.at(0).hash_, false,
      row_allocator, orphan_row_scn, orphan_row));
  ASSERT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, orphan_row.status_);
  ASSERT_GT(orphan_row_scn, SCN::base_scn().get_val_for_tx());

  const int64_t deadline_us = ObTimeUtility::current_time_us() + 180L * 1000L * 1000L;
  // Flush initialization writes in system Meta without freezing the empty Control partitions.
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        SYS_LS, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->logstream_freeze(
        0, true, deadline_us, ObFreezeSourceFlag::TEST_MODE));
  }
  bool completed = false;
  int64_t studied_ctrl_count = 0;
  int64_t cold_empty_count = 0;
  while (!completed && ObTimeUtility::current_time_us() < deadline_us) {
    // A fresh round snapshot lets newly uploaded empty partitions join the real Study.
    ObSSMarkSweepDagParam param;
    ASSERT_EQ(OB_SUCCESS, param.init(tenant_id_, gen.round_id_, 16L << 20, 2L << 20, 2));
    ASSERT_EQ(OB_SUCCESS, meta_service->get_max_committed_meta_scn(param.snapshot_meta_scn_));
    param.safe_meta_scn_ = param.snapshot_meta_scn_;
    ASSERT_EQ(OB_SUCCESS, gen.get_all_ls_ss_checkpoint_scn_(param.snapshot_meta_scn_,
        param.safe_meta_scn_, param.reserved_meta_scns_, SCN(),
        param.ls_ss_checkpoint_scn_map_, param.recyclable_deleted_ls_ids_));
    bool checkpoint_ready = true;
    SCN ls_checkpoint;
    ASSERT_EQ(OB_SUCCESS, param.ls_ss_checkpoint_scn_map_.get_refactored(
        uploaded_meta.gc_tablet_.ls_id_, ls_checkpoint));
    checkpoint_ready = ls_checkpoint > max_checkpoint;
    for (int64_t i = 0; i < ctrl_ls_ids.count(); ++i) {
      const int ret = param.ls_ss_checkpoint_scn_map_.get_refactored(
          ctrl_ls_ids.at(i), ls_checkpoint);
      ASSERT_TRUE(OB_SUCCESS == ret || OB_HASH_NOT_EXIST == ret) << ret;
      checkpoint_ready = checkpoint_ready && OB_SUCCESS == ret && ls_checkpoint > max_checkpoint;
    }
    if (!checkpoint_ready) {
      ob_usleep(500L * 1000L);
      continue;
    }
    ASSERT_EQ(OB_SUCCESS, gen.study_tablet_meta_scn_(param.snapshot_meta_scn_,
        param.ls_ss_checkpoint_scn_map_, param.tablet_with_meta_scn_arr_,
        param.mark_deleted_tablets_));
    bool study_ready = true;
    studied_ctrl_count = 0;
    cold_empty_count = 0;
    for (int64_t i = 0; i < all_ctrl_ids.count(); ++i) {
      const ObSSGCTabletWithMetaScn *input = nullptr;
      int64_t matches = 0;
      for (int64_t j = 0; j < param.tablet_with_meta_scn_arr_.count(); ++j) {
        const ObSSGCTabletWithMetaScn &candidate = param.tablet_with_meta_scn_arr_.at(j);
        if (candidate.gc_tablet_.tablet_id_ == all_ctrl_ids.at(i)
            && candidate.gc_tablet_.ls_id_ == ctrl_ls_ids.at(i)) {
          input = &candidate;
          ++matches;
        }
      }
      ASSERT_LE(matches, 1);
      studied_ctrl_count += matches;
      if (nullptr == input) {
        study_ready = false;
      } else {
        study_ready = study_ready && input->clog_checkpoint_scn_.is_valid_and_not_min()
            && !input->clog_checkpoint_scn_.is_max()
            && input->clog_checkpoint_scn_.get_val_for_tx() > orphan_row_scn;
        if (!has_exist_in_array(touched_ctrl_ids, all_ctrl_ids.at(i))
            && input->orig_clog_checkpoint_scn_ == SCN::base_scn() && input->is_cold()) {
          ++cold_empty_count;
        }
      }
    }
    if (!study_ready || studied_ctrl_count != 128 || cold_empty_count == 0) {
      ob_usleep(500L * 1000L);
      continue;
    }

    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "LobColdGC"));
    ObSSMarkSweepDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    ObSSMarkSweepPrepareTask prepare;
    prepare.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, prepare.init());
    ASSERT_EQ(OB_SUCCESS, prepare.process());
    ObSSMarkSweepCtx &ctx = *dag.get_ctx();
    ObSSOuttableGCTask user_gc;
    user_gc.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, user_gc.init());
    ASSERT_EQ(OB_SUCCESS, user_gc.prepare_scan_inputs_(ctx, ctx.lob_meta_scan_inputs_,
        ctx.lob_ctrl_scan_inputs_, ctx.min_outtable_data_scn_));
    bool meta_ready = false;
    for (int64_t i = 0; i < ctx.lob_meta_scan_inputs_.count(); ++i) {
      const ObSSGCTabletWithMetaScn &input = ctx.lob_meta_scan_inputs_.at(i);
      if (input.gc_tablet_ == uploaded_meta.gc_tablet_) {
        meta_ready = input.meta_scn_ >= uploaded_meta.meta_scn_;
      }
    }
    if (!meta_ready) {
      ob_usleep(500L * 1000L);
      continue;
    }
    ASSERT_EQ(128, ctx.lob_ctrl_scan_inputs_.count());
    for (int64_t i = 0; i < all_ctrl_ids.count(); ++i) {
      int64_t matches = 0;
      for (int64_t j = 0; j < ctx.lob_ctrl_scan_inputs_.count(); ++j) {
        matches += ctx.lob_ctrl_scan_inputs_.at(j).gc_tablet_.tablet_id_ == all_ctrl_ids.at(i);
      }
      ASSERT_EQ(1, matches) << all_ctrl_ids.at(i).id();
    }

    // Recalculate the old rule from these exact scan baselines; it must still yield CP=1.
    ObSSGCTabletWithMetaScnArray scan_inputs;
    ASSERT_EQ(OB_SUCCESS, append(scan_inputs, ctx.lob_meta_scan_inputs_));
    ASSERT_EQ(OB_SUCCESS, append(scan_inputs, ctx.lob_ctrl_scan_inputs_));
    SCN old_cutoff = SCN::max_scn();
    int64_t empty_shared_cp1_count = 0;
    const ObSSGCTabletWithMetaScn *blocked_input = nullptr;
    SCN blocked_shared_scn;
    for (int64_t i = 0; i < scan_inputs.count(); ++i) {
      const ObSSGCTabletWithMetaScn &input = scan_inputs.at(i);
      const ObSSGCTabletWithMetaScn *study_input = nullptr;
      for (int64_t j = 0; j < param.tablet_with_meta_scn_arr_.count(); ++j) {
        if (param.tablet_with_meta_scn_arr_.at(j).gc_tablet_ == input.gc_tablet_) {
          study_input = &param.tablet_with_meta_scn_arr_.at(j);
          break;
        }
      }
      ASSERT_NE(nullptr, study_input);
      ObArenaAllocator tablet_allocator(ObMemAttr(tenant_id_, "LobColdCP"));
      ObTabletHandle handle;
      ASSERT_EQ(OB_SUCCESS, meta_service->get_tablet(input.gc_tablet_.ls_id_,
          input.gc_tablet_.tablet_id_, input.gc_tablet_.transfer_scn_, input.meta_scn_,
          tablet_allocator, handle));
      ASSERT_NE(nullptr, handle.get_obj());
      const SCN &checkpoint = handle.get_obj()->get_clog_checkpoint_scn();
      ASSERT_TRUE(checkpoint.is_valid_and_not_min());
      ASSERT_FALSE(checkpoint.is_max());
      const SCN &orig_scn = study_input->orig_clog_checkpoint_scn_;
      const SCN &study_scn = study_input->clog_checkpoint_scn_;
      // Wait for every real LOB baseline to cover the test rows or match its cold Study.
      const bool baseline_ready = handle.get_obj()->is_data_complete()
          && !handle.get_obj()->get_tablet_meta().split_info_.is_data_incomplete()
          && orig_scn.is_valid_and_not_min() && !orig_scn.is_max()
          && study_scn.is_valid_and_not_min() && !study_scn.is_max()
          && study_scn.get_val_for_tx() > orphan_row_scn
          && ((checkpoint.get_val_for_tx() > orphan_row_scn
                  && orig_scn.get_val_for_tx() > orphan_row_scn)
              || (checkpoint == orig_scn && study_input->is_cold()));
      if (!baseline_ready && nullptr == blocked_input) {
        blocked_input = study_input;
        blocked_shared_scn = checkpoint;
      }
      old_cutoff = SCN::min(old_cutoff, checkpoint);
      if (checkpoint == SCN::base_scn()
          && has_exist_in_array(all_ctrl_ids, input.gc_tablet_.tablet_id_)
          && !has_exist_in_array(touched_ctrl_ids, input.gc_tablet_.tablet_id_)) {
        ++empty_shared_cp1_count;
      }
    }
    if (nullptr != blocked_input) {
      LOG_INFO("wait for complete uploaded LOB baselines", KPC(blocked_input),
          K(blocked_shared_scn));
      ob_usleep(500L * 1000L);
      continue;
    }
    ASSERT_GT(empty_shared_cp1_count, 0);
    ASSERT_EQ(SCN::base_scn(), old_cutoff);
    ASSERT_TRUE(ctx.min_outtable_data_scn_.is_valid_and_not_min());
    ASSERT_FALSE(ctx.min_outtable_data_scn_.is_max());
    ASSERT_GT(ctx.min_outtable_data_scn_.get_val_for_tx(), orphan_row_scn);
    ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_meta_scan_inputs_, live_keys, true));
    ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_meta_scan_inputs_, orphan_keys, false));
    ASSERT_NO_FATAL_FAILURE(check_physical_keys_(ctx.lob_ctrl_scan_inputs_, all_keys, true));
    ASSERT_NO_FATAL_FAILURE(scan_and_diff_gc_(dag));
    ASSERT_NO_FATAL_FAILURE(check_gc_candidates_(
        ctx, ctx.lob_ctrl_diff_file_, orphan_keys, true /*exact_keys*/));
    ObSSLobCtrlGCDeleteTask delete_task;
    delete_task.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, delete_task.init());
    ASSERT_EQ(OB_SUCCESS, delete_task.process());
    ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), orphan_keys, 0 /*absent*/));
    ASSERT_NO_FATAL_FAILURE(check_keys_(inner_proxy(), live_keys, OUTTABLE_LOB_CONTROL_COMPLETE));
    ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 1, 'k', bytes));
    LOG_INFO("cold Control checkpoint GC regression passed", K(old_cutoff),
        K(ctx.min_outtable_data_scn_), K(orphan_row_scn), K(studied_ctrl_count),
        K(cold_empty_count), K(empty_shared_cp1_count));
    completed = true;
  }
  ASSERT_TRUE(completed) << "studied_ctrl_count=" << studied_ctrl_count
      << " cold_empty_count=" << cold_empty_count;
}

void TestOuttableLobControlE2E::check_table_api_write_(
    const ObString &credential,
    const table::ObTableOperationType::Type op_type,
    const int32_t id,
    const char fill_char,
    const int64_t byte_length,
    const int64_t expected_affected_rows)
{
  SCOPED_TRACE(op_type);
  SCOPED_TRACE(id);
  SCOPED_TRACE(table_name_);
  ObArray<ObOuttableLobControlKey> before;
  ObArray<ObOuttableLobControlKey> new_keys;
  ASSERT_NO_FATAL_FAILURE(collect_keys_(before));
  ObArenaAllocator allocator("LobTableApiE2E");
  char *payload = static_cast<char *>(allocator.alloc(byte_length));
  ASSERT_NE(nullptr, payload);
  MEMSET(payload, fill_char, byte_length);
  ObObj rowkey;
  rowkey.set_int32(id);
  ObObj value;
  value.set_varbinary(ObString(byte_length, payload));
  table::ObTableEntity entity;
  ASSERT_EQ(OB_SUCCESS, entity.set_rowkey_value(0, rowkey));
  ASSERT_EQ(OB_SUCCESS, entity.set_property(ObString::make_string("v"), value));
  table::ObTableOperationRequest request;
  request.credential_ = credential;
  request.table_name_ = ObString::make_string(table_name_);
  request.entity_type_ = table::ObTableEntityType::ET_KV;
  request.consistency_level_ = table::ObTableConsistencyLevel::STRONG;
  request.returning_affected_rows_ = true;
  request.table_operation_.set_type(op_type);
  request.table_operation_.set_entity(entity);
  table::ObTableEntity result_entity;
  result_entity.set_allocator(&allocator);
  table::ObTableOperationResult result;
  result.set_entity(result_entity);
  observer::ObServer &server = get_curr_observer();
  const int rpc_ret = server.get_table_rpc_proxy().to(server.get_self())
      .by(tenant_id_).timeout(60L * 1000L * 1000L).execute(request, result);
  const int operation_ret = (OB_SUCCESS == rpc_ret) ? result.get_errno() : rpc_ret;
  ASSERT_EQ(OB_SUCCESS, operation_ret);
  ASSERT_EQ(op_type, result.type());
  ASSERT_EQ(expected_affected_rows, result.get_affected_rows());
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), id, fill_char, byte_length));

  // TableAPI falls back to InTable storage without creating OutTable Control rows.
  ASSERT_NO_FATAL_FAILURE(collect_new_keys_(before, new_keys));
  ASSERT_TRUE(new_keys.empty());
}

TEST_F(TestOuttableLobControlE2E, table_api_falls_back_to_intable_writes)
{
  ASSERT_NO_FATAL_FAILURE(init_user_());
  ASSERT_NO_FATAL_FAILURE(create_lob_table_("lob_ctrl_api_e2e"));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "INSERT INTO test.lob_ctrl_api_e2e VALUES (1, '')", affected_rows));
  // Use single-operation requests to cover TableAPI sessions without a started SQL statement.
  ASSERT_EQ(OB_SUCCESS, inner_proxy().write(tenant_id_,
      "ALTER SYSTEM SET kv_group_commit_batch_size = 1", affected_rows));
  table::ObTableLoginRequest login;
  login.client_type_ = table::LIBTABLE_CLIENT;
  login.tenant_name_ = ObString::make_string("lob_ctrl_e2e");
  login.user_name_ = ObString::make_string("root");
  login.database_name_ = ObString::make_string("test");
  login.pass_scramble_ = ObString::make_string("12345678901234567890");
  // Copy login strings during RPC decoding; the response buffer is freed when the call returns.
  char credential_buf[table::ObTableApiCredential::CREDENTIAL_BUF_SIZE];
  char version_buf[256];
  table::ObTableLoginResult login_result;
  login_result.credential_.assign_buffer(credential_buf, sizeof(credential_buf));
  login_result.server_version_.assign_buffer(version_buf, sizeof(version_buf));
  observer::ObServer &server = get_curr_observer();
  ASSERT_EQ(OB_SUCCESS, server.get_table_rpc_proxy().to(server.get_self())
      .by(tenant_id_).timeout(60L * 1000L * 1000L).login(login, login_result));
  ASSERT_EQ(tenant_id_, login_result.tenant_id_);
  ASSERT_FALSE(login_result.credential_.empty());
  {
    ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
    table::ObTableObjectPoolMgr *pool = MTL(table::ObTableObjectPoolMgr *);
    ASSERT_NE(nullptr, pool);
    const int64_t deadline_us = ObTimeUtility::current_time_us() + 10L * 1000L * 1000L;
    while (1 != pool->get_kv_group_commit_batch_size()
        && ObTimeUtility::current_time_us() < deadline_us) {
      ob_usleep(100L * 1000L);
    }
    ASSERT_EQ(1, pool->get_kv_group_commit_batch_size());
  }

  const table::ObTableOperationType::Type operations[] = {
      table::ObTableOperationType::INSERT,
      table::ObTableOperationType::UPDATE,
      table::ObTableOperationType::REPLACE,
      table::ObTableOperationType::INSERT_OR_UPDATE,
      table::ObTableOperationType::INSERT_OR_UPDATE,
  };
  const int32_t ids[] = {2, 1, 1, 1, 3};
  const char chars[] = {'a', 'b', 'c', 'd', 'e'};
  // REPLACE counts delete + insert; INSERT_OR_UPDATE counts one row in either branch.
  const int64_t expected_affected_rows[] = {1, 1, 2, 1, 1};
  const int64_t byte_length = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE + 17;
  for (int64_t i = 0; i < ARRAYSIZEOF(operations); ++i) {
    ASSERT_NO_FATAL_FAILURE(check_table_api_write_(login_result.credential_, operations[i],
        ids[i], chars[i], byte_length, expected_affected_rows[i]));
  }
  ASSERT_NO_FATAL_FAILURE(check_lob_(user_proxy(), 1, 'd', byte_length));
  {
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, user_proxy().read(result,
        "SELECT COUNT(*) AS cnt FROM test.lob_ctrl_api_e2e"));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    int64_t row_count = 0;
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", row_count));
    ASSERT_EQ(3, row_count);
  }

  // TTL INSERT has its own executor and must fall back to InTable storage too.
  table_name_ = "lob_ctrl_ttl_e2e";
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "CREATE TABLE test.lob_ctrl_ttl_e2e (id INT PRIMARY KEY, v LONGBLOB, "
      "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP) LOB_INROW_THRESHOLD = 0 "
      "LOB_INTABLE_THRESHOLD = 0 MERGE_ENGINE = delete_insert "
      "TTL(ts + INTERVAL 1 DAY)", affected_rows));
  ASSERT_NO_FATAL_FAILURE(check_table_api_write_(login_result.credential_,
      table::ObTableOperationType::INSERT, 1, 't', byte_length, 1));

  table_name_ = "lob_ctrl_api_intable_e2e";
  ASSERT_EQ(OB_SUCCESS, user_proxy().write(
      "CREATE TABLE test.lob_ctrl_api_intable_e2e (id INT PRIMARY KEY, v LONGBLOB) "
      "LOB_INROW_THRESHOLD = 0 LOB_INTABLE_THRESHOLD = 4096", affected_rows));
  ASSERT_NO_FATAL_FAILURE(check_table_api_write_(login_result.credential_,
      table::ObTableOperationType::INSERT, 1, 'i', 17, 1));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (nullptr == s3_bucket || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = nullptr != s3_endpoint_override && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *s3_ak = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *s3_sk = use_test_endpoint ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (nullptr == s3_region || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char storage_info[1000] = {0};
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  const int ret = oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (oceanbase::common::OB_SUCCESS != ret) {
    return ret;
  }
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
