/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <memory>
#include <unistd.h>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define private public
#define protected public
#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_result.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/tx/ob_trans_service.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace observer;

namespace
{
uint64_t tenant_id = OB_INVALID_TENANT_ID;

// Only BEGIN/ROLLBACK are stubbed. Connection/session initialization and the
// schema guard's connection callback are the production implementations.
class RecordingConnection : public ObInnerSQLConnection
{
public:
  ~RecordingConnection() override { destroy(); }
  int start_transaction(const uint64_t &tid, bool with_snapshot) override
  {
    ++begin_count_;
    callback_before_begin_ = callback_count_ > 0;
    begin_tenant_ = tid;
    begin_snapshot_ = with_snapshot;
    int ret = begin_ret_;
    if (OB_SUCCESS == ret && (nullptr != inner_session_ || nullptr != extern_session_)) {
      rr_before_begin_ = transaction::ObTxIsolationLevel::RR == get_session().get_tx_isolation();
      readonly_before_begin_ = get_session().get_tx_read_only();
      ret = get_session().switch_tenant(wrong_tenant_ ? OB_SYS_TENANT_ID : tid);
      resource_conn_id_ = remote_route_ ? 123 : OB_INVALID_ID;
    }
    return ret;
  }
  int rollback() override
  {
    ++rollback_count_;
    resource_conn_id_ = OB_INVALID_ID;
    return rollback_ret_;
  }
  int commit() override { ++commit_count_; return OB_SUCCESS; }

  int callback_count_ = 0;
  int callback_ret_ = OB_SUCCESS;
  int begin_count_ = 0;
  int begin_ret_ = OB_SUCCESS;
  int rollback_count_ = 0;
  int rollback_ret_ = OB_SUCCESS;
  int commit_count_ = 0;
  uint64_t begin_tenant_ = OB_INVALID_TENANT_ID;
  bool callback_before_begin_ = false;
  bool begin_snapshot_ = false;
  bool rr_before_begin_ = false;
  bool readonly_before_begin_ = false;
  bool wrong_tenant_ = false;
  bool remote_route_ = false;
};

class RecordingPool : public ObInnerSQLConnectionPool
{
public:
  explicit RecordingPool(sqlclient::ObISQLConnection &connection) : connection_(connection) {}
  int acquire(const uint64_t tid, sqlclient::ObISQLConnection *&conn,
              ObISQLClient *client, const int32_t group_id) override
  {
    UNUSEDx(tid, client);
    ++acquire_count_;
    group_id_ = group_id;
    conn = OB_SUCCESS == acquire_ret_ ? &connection_ : nullptr;
    return acquire_ret_;
  }
  int release(sqlclient::ObISQLConnection *conn, const bool success) override
  {
    EXPECT_EQ(&connection_, conn);
    ++release_count_;
    release_success_ = success;
    auto *inner = dynamic_cast<ObInnerSQLConnection *>(conn);
    return nullptr == inner ? OB_SUCCESS : inner->destroy();
  }
  int on_client_inactive(ObISQLClient *client) override { UNUSED(client); return OB_SUCCESS; }

  sqlclient::ObISQLConnection &connection_;
  int acquire_ret_ = OB_SUCCESS;
  int acquire_count_ = 0;
  int release_count_ = 0;
  int32_t group_id_ = -1;
  bool release_success_ = true;
};

int recording_init(sqlclient::ObISQLConnection *connection, const uint64_t tid)
{
  UNUSED(tid);
  auto *conn = static_cast<RecordingConnection *>(connection);
  ++conn->callback_count_;
  return conn->callback_ret_;
}

class FailingResult : public ObInnerSQLResult
{
public:
  explicit FailingResult(sql::ObSQLSessionInfo &session) : ObInnerSQLResult(session, true, nullptr) {}
  int next() override { return OB_ERR_UNEXPECTED; }
};

struct ScanResources
{
  void capture(ObInnerSQLConnection &conn)
  {
    pool_ = conn.pool_;
    ASSERT_NE(nullptr, pool_);
    connection_addr_ = reinterpret_cast<uint64_t>(&conn);
    connection_init_ts_ = conn.get_init_timestamp();
    session_addr_ = reinterpret_cast<uint64_t>(&conn.get_session());
    sid_ = conn.get_session().get_server_sid();
    auto *tx = conn.get_session().get_tx_desc();
    ASSERT_NE(nullptr, tx);
    tx_id_ = tx->get_tx_id();
    ASSERT_TRUE(tx_id_.is_valid()); // This file only tests primary tenants.
    tx_addr_ = reinterpret_cast<uint64_t>(tx);
    ASSERT_EQ(sid_, tx->sess_id_);
    tx_service_ = MTL(transaction::ObTransService *);
    ASSERT_NE(nullptr, tx_service_);
    ASSERT_EQ(nullptr, conn.get_prev_read_ctx());
    ASSERT_EQ(1, conn.get_ref()); // The cursor's connection reference is gone.
    ASSERT_NO_FATAL_FAILURE(inspect());
    ASSERT_TRUE(connection_used_);
    ASSERT_TRUE(session_held_);
    ASSERT_TRUE(tx_registered_);
#ifdef ENABLE_DEBUG_LOG
    ASSERT_TRUE(tx_allocated_);
#endif
  }

  void inspect()
  {
    connection_used_ = false;
    {
      ObThreadCondGuard lock(pool_->cond_);
      DLIST_FOREACH_NORET(conn, pool_->used_conn_list_) {
        if (connection_addr_ == reinterpret_cast<uint64_t>(conn)
            && connection_init_ts_ == conn->get_init_timestamp()) {
          connection_used_ = true;
        }
      }
    }
    session_held_ = false;
    auto find_session = [&](hash::HashMapPair<uint64_t, sql::ObSQLSessionInfo *> &entry) {
      if (session_addr_ == entry.first && nullptr != entry.second
          && sid_ == entry.second->get_server_sid()) {
        session_held_ = true;
      }
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, GCTX.session_mgr_->for_each_hold_session(find_session));

    // A lookup takes a reference: return it immediately. Holding it across
    // guard cleanup would itself prevent reuse_tx() from reaching quiescence.
    auto &mgr = tx_service_->tx_desc_mgr_;
    transaction::ObTxDesc *tx = nullptr;
    const int ret = mgr.get(tx_id_, tx);
    tx_registered_ = OB_SUCCESS == ret;
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(tx_addr_, reinterpret_cast<uint64_t>(tx));
      mgr.revert(*tx);
    } else {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
    }
#ifdef ENABLE_DEBUG_LOG
    // A descriptor may have been unregistered but leaked through an outstanding
    // reference. The debug allocator tracks these too. Match SID as well because
    // connection/session/descriptor memory can be reused by background tasks.
    tx_allocated_ = false;
    auto find_tx = [&](transaction::ObTxDesc *allocated) {
      if (tx_addr_ == reinterpret_cast<uint64_t>(allocated) && sid_ == allocated->sess_id_) {
        tx_allocated_ = true;
      }
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, mgr.map_.alloc_handle_.for_each(find_tx));
#endif
  }

  void expect_released()
  {
    // Allow transient references from background traversal to be returned, but
    // never compare global allocation counts or dereference a released pointer.
    const int64_t deadline = ObTimeUtility::current_time() + 2 * 1000 * 1000L;
    do {
      ASSERT_NO_FATAL_FAILURE(inspect());
      if (!connection_used_ && !session_held_ && !tx_registered_ && !tx_allocated_) { break; }
      usleep(1000);
    } while (ObTimeUtility::current_time() < deadline);
    EXPECT_FALSE(connection_used_) << "scan connection remains in the used list";
    EXPECT_FALSE(session_held_) << "scan session still has outstanding references";
    EXPECT_FALSE(tx_registered_) << "scan transaction remains registered";
#ifdef ENABLE_DEBUG_LOG
    EXPECT_FALSE(tx_allocated_) << "unregistered scan descriptor still has outstanding references";
#endif
  }

  ObInnerSQLConnectionPool *pool_ = nullptr;
  transaction::ObTransService *tx_service_ = nullptr;
  transaction::ObTransID tx_id_;
  uint64_t connection_addr_ = 0;
  int64_t connection_init_ts_ = 0;
  uint64_t session_addr_ = 0;
  uint32_t sid_ = 0;
  uint64_t tx_addr_ = 0;
  bool connection_used_ = false;
  bool session_held_ = false;
  bool tx_registered_ = false;
  bool tx_allocated_ = false;
};
} // namespace

class TestLiteScanStrongCleanup : public ObSimpleClusterTestBase
{
public:
  TestLiteScanStrongCleanup() : ObSimpleClusterTestBase("test_lite_scan_strong_cleanup_", "10G", "20G") {}
  void SetUp() override
  {
    if (!is_started_) {
      GCONF._enable_simple_schema_on_demand = true;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ASSERT_EQ(OB_SUCCESS, create_tenant("lite_cleanup", "3G", "3G", false, 2));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, "lite_cleanup"));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("lite_cleanup", "test"));
    }
    int64_t affected = 0;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(
        "create table if not exists test.lite_cleanup_table(id int, v int) "
        "partition by range(id) subpartition by hash(v) subpartitions 2 "
        "(partition p0 values less than (100), partition p1 values less than (200))", affected));
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select database_id from oceanbase.__all_virtual_table "
        "where tenant_id = %lu and table_name = 'lite_cleanup_table'", tenant_id));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().read(result, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_uint("database_id", database_id_));
  }

  void init_connection(RecordingConnection &conn, RecordingPool &pool,
                       const bool resource = false, sql::ObSQLSessionInfo *external = nullptr)
  {
    auto *real_pool = dynamic_cast<ObInnerSQLConnectionPool *>(GCTX.sql_proxy_->get_pool());
    ASSERT_NE(nullptr, real_pool);
    ASSERT_EQ(OB_SUCCESS, conn.init(&pool, real_pool->schema_service_, real_pool->ob_sql_,
        real_pool->vt_iter_creator_, real_pool->config_, external, nullptr, nullptr, false, false, 0, resource));
  }

  void init_injected_guard(ObSchemaGetterGuard &guard, ObMultiVersionSchemaService &service, ObMySQLProxy &proxy)
  {
    // An isolated service is only a SQL-proxy holder here; never replace GCTX's
    // proxy while background schema refresh threads are running.
    service.sql_proxy_ = &proxy;
    ASSERT_EQ(OB_SUCCESS, guard.init(ObSimpleSchemaMode::ON_DEMAND));
    guard.schema_service_ = &service;
    guard.schema_guard_type_ = ObSchemaGetterGuard::TENANT_SCHEMA_GUARD;
    guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  }

  void expect_session(const uint32_t sid, const bool expected)
  {
    bool found = false;
    auto find = [&](sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *session) {
      UNUSED(key);
      found |= nullptr != session && sid == session->get_server_sid();
      return true;
    };
    ASSERT_EQ(OB_SUCCESS, GCTX.session_mgr_->for_each_session(find));
    EXPECT_EQ(expected, found);
  }

  ObSchemaLiteScanSpec spec(const bool join = false) const
  {
    ObSchemaLiteScanSpec result;
    result.table_spec_.tenant_id_ = tenant_id;
    result.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
    result.table_spec_.filter_id_ = database_id_;
    result.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID;
    if (join) {
      result.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID | OB_SCHEMA_LITE_PART_TABLET_ID;
      result.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
          | OB_SCHEMA_LITE_SUBPART_TABLET_ID;
    }
    result.caller_tag_ = "mittest_strong_cleanup";
    return result;
  }

  // exit_kind: early return / fetch error / EOF. Iterator destruction must close
  // the cursor before the caller ends the transaction.
  void leave_scan(ObSchemaGetterGuard &guard, const int family, const int exit_kind)
  {
    if (0 == family) {
      ObSchemaLiteTableIterator iter;
      const ObSchemaLiteTableResult *row = nullptr;
      ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec(), iter));
      ASSERT_EQ(OB_SUCCESS, iter.get_next(row));
      if (1 == exit_kind) {
        auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
        FailingResult failure(conn->get_session());
        iter.table_result_ = &failure;
        EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next(row));
        EXPECT_EQ(nullptr, row);
        // The owned ReadResult still holds the real cursor and is closed by reset.
        iter.reset();
      } else if (2 == exit_kind) {
        int ret = OB_SUCCESS;
        while (OB_SUCCESS == (ret = iter.get_next(row))) {}
        EXPECT_EQ(OB_ITER_END, ret);
      }
    } else {
      ObSchemaLitePartJoinIterator iter;
      const ObSchemaLiteJoinRow *row = nullptr;
      ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true),
          1 == family ? ObSchemaLiteJoinSide::PART : ObSchemaLiteJoinSide::SUBPART, iter));
      ASSERT_EQ(OB_SUCCESS, iter.get_next(row));
      if (1 == exit_kind) {
        auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
        FailingResult failure(conn->get_session());
        iter.sql_result_ = &failure;
        EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next(row));
        EXPECT_EQ(nullptr, row);
        iter.reset();
      } else if (2 == exit_kind) {
        int ret = OB_SUCCESS;
        while (OB_SUCCESS == (ret = iter.get_next(row))) {}
        EXPECT_EQ(OB_ITER_END, ret);
      }
    }
  }

  uint64_t database_id_ = OB_INVALID_ID;
};

TEST_F(TestLiteScanStrongCleanup, callback_order_and_failures_release_connection_once)
{
  for (int failure = 0; failure < 4; ++failure) {
    SCOPED_TRACE(failure);
    RecordingConnection conn;
    RecordingPool pool(conn);
    ObMySQLProxy proxy;
    ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
    if (1 == failure) { pool.acquire_ret_ = OB_ALLOCATE_MEMORY_FAILED; }
    if (2 == failure) { conn.callback_ret_ = OB_NOT_SUPPORTED; }
    if (3 == failure) { conn.begin_ret_ = OB_TIMEOUT; }
    {
      ObMySQLTransaction tx;
      const int ret = tx.start_with_connection_init(&proxy, tenant_id, true, 7, recording_init);
      EXPECT_EQ(1, pool.acquire_count_);
      EXPECT_EQ(7, pool.group_id_);
      EXPECT_EQ(1 == failure ? 0 : 1, conn.callback_count_);
      EXPECT_EQ(failure == 1 || failure == 2 ? 0 : 1, conn.begin_count_);
      if (0 == failure) {
        ASSERT_EQ(OB_SUCCESS, ret);
        EXPECT_TRUE(conn.callback_before_begin_);
        EXPECT_TRUE(conn.begin_snapshot_);
        EXPECT_EQ(tenant_id, conn.begin_tenant_);
        EXPECT_TRUE(tx.is_started());
        EXPECT_EQ(OB_SUCCESS, tx.end(false));
        EXPECT_EQ(1, conn.rollback_count_);
      } else {
        EXPECT_EQ(1 == failure ? OB_ALLOCATE_MEMORY_FAILED :
            (2 == failure ? OB_NOT_SUPPORTED : OB_TIMEOUT), ret);
        EXPECT_FALSE(tx.is_started());
        EXPECT_EQ(nullptr, tx.get_connection());
        EXPECT_EQ(0, conn.rollback_count_);
        if (failure != 1) { EXPECT_FALSE(pool.release_success_); }
      }
    }
    EXPECT_EQ(1 == failure ? 0 : 1, pool.release_count_);
    EXPECT_EQ(0, conn.commit_count_);
  }
}

TEST_F(TestLiteScanStrongCleanup, original_start_and_rollback_failure_still_release_connection)
{
  RecordingConnection conn;
  RecordingPool pool(conn);
  ObMySQLProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
  {
    ObMySQLTransaction tx;
    ASSERT_EQ(OB_SUCCESS, tx.start(&proxy, tenant_id, true, 0));
    EXPECT_EQ(0, conn.callback_count_);
    EXPECT_EQ(1, conn.begin_count_);
    conn.rollback_ret_ = OB_TIMEOUT;
    EXPECT_EQ(OB_TIMEOUT, tx.end(false));
    EXPECT_FALSE(tx.is_started());
    EXPECT_EQ(nullptr, tx.get_connection());
    EXPECT_EQ(OB_SUCCESS, tx.end(false));
  }
  EXPECT_EQ(1, pool.release_count_);
  EXPECT_FALSE(pool.release_success_);
  EXPECT_EQ(1, conn.rollback_count_);
}

TEST_F(TestLiteScanStrongCleanup, unsupported_sessions_are_rejected_before_begin)
{
  // Resource session, local fallback session, borrowed session, non-inner
  // session, and a valid managed session created under the wrong tenant MTL.
  for (int kind = 0; kind < 5; ++kind) {
    SCOPED_TRACE(kind);
    ObTenantSwitchGuard tenant;
    ASSERT_EQ(OB_SUCCESS, tenant.switch_to(4 == kind ? OB_SYS_TENANT_ID : tenant_id));
    RecordingConnection owner;
    RecordingPool owner_pool(owner);
    if (2 == kind) { ASSERT_NO_FATAL_FAILURE(init_connection(owner, owner_pool)); }
    RecordingConnection conn;
    RecordingPool pool(conn);
    ASSERT_NO_FATAL_FAILURE(init_connection(conn, pool, 0 == kind,
        2 == kind ? &owner.get_session() : nullptr));
    if (1 == kind) {
      // Model init_session's allocation/config fallback using the real helpers,
      // without changing tenant configuration or forging a managed session ID.
      ASSERT_EQ(OB_SUCCESS, conn.destroy_inner_session());
      ASSERT_EQ(OB_SUCCESS, conn.create_default_session());
      ASSERT_EQ(OB_SUCCESS, conn.init_session_info(&conn.get_session(), false, false, false));
      ASSERT_FALSE(conn.is_resource_conn());
      ASSERT_EQ(uint32_t(ObInnerSQLConnection::INNER_SQL_SESS_ID), conn.get_session().get_server_sid());
    } else if (3 == kind) {
      conn.get_session().set_real_inner_session(false);
    }
    const uint32_t sid = conn.get_session().get_server_sid();
    const auto old_isolation = conn.get_session().get_tx_isolation();
    const bool old_readonly = conn.get_session().get_tx_read_only();
    ObMySQLProxy proxy;
    ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
    std::unique_ptr<ObMultiVersionSchemaService> service(new ObMultiVersionSchemaService());
    ObSchemaGetterGuard guard;
    ASSERT_NO_FATAL_FAILURE(init_injected_guard(guard, *service, proxy));
    ObISQLClient *client = nullptr;
    EXPECT_EQ(OB_NOT_SUPPORTED, guard.prepare_schema_scan_sql_client_(tenant_id, client));
    EXPECT_EQ(nullptr, client);
    EXPECT_EQ(nullptr, guard.schema_scan_trans_);
    EXPECT_EQ(OB_INVALID_TENANT_ID, guard.schema_scan_trans_tenant_id_);
    EXPECT_EQ(0, conn.begin_count_);
    EXPECT_EQ(1, pool.release_count_);
    EXPECT_FALSE(pool.release_success_);
    if (2 == kind) {
      // Rejecting a borrowed session must neither change nor destroy its owner.
      EXPECT_EQ(old_isolation, owner.get_session().get_tx_isolation());
      EXPECT_EQ(old_readonly, owner.get_session().get_tx_read_only());
      ASSERT_NO_FATAL_FAILURE(expect_session(sid, true));
    } else if (sid != ObInnerSQLConnection::INNER_SQL_SESS_ID) {
      ASSERT_NO_FATAL_FAILURE(expect_session(sid, false));
    }
  }
}

TEST_F(TestLiteScanStrongCleanup, non_inner_connection_is_rejected)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  sqlclient::ObMySQLConnection conn;
  RecordingPool pool(conn);
  ObMySQLProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
  std::unique_ptr<ObMultiVersionSchemaService> service(new ObMultiVersionSchemaService());
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_injected_guard(guard, *service, proxy));
  ObISQLClient *client = nullptr;
  EXPECT_EQ(OB_NOT_SUPPORTED, guard.prepare_schema_scan_sql_client_(tenant_id, client));
  EXPECT_EQ(nullptr, client);
  EXPECT_EQ(nullptr, guard.schema_scan_trans_);
  EXPECT_EQ(1, pool.release_count_);
}

TEST_F(TestLiteScanStrongCleanup, rr_readonly_are_set_before_begin_and_route_failure_rolls_back)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  for (int route = 0; route < 3; ++route) {
    SCOPED_TRACE(route);
    RecordingConnection conn;
    RecordingPool pool(conn);
    ASSERT_NO_FATAL_FAILURE(init_connection(conn, pool));
    const uint32_t sid = conn.get_session().get_server_sid();
    ASSERT_NE(uint32_t(ObInnerSQLConnection::INNER_SQL_SESS_ID), sid);
    ASSERT_NO_FATAL_FAILURE(expect_session(sid, true));
    conn.remote_route_ = 1 == route;
    conn.wrong_tenant_ = 2 == route;
    ObMySQLProxy proxy;
    ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
    std::unique_ptr<ObMultiVersionSchemaService> service(new ObMultiVersionSchemaService());
    ObSchemaGetterGuard guard;
    ASSERT_NO_FATAL_FAILURE(init_injected_guard(guard, *service, proxy));
    ObISQLClient *client = nullptr;
    EXPECT_EQ(0 == route ? OB_SUCCESS : OB_NOT_SUPPORTED,
        guard.prepare_schema_scan_sql_client_(tenant_id, client));
    EXPECT_EQ(1, conn.begin_count_);
    EXPECT_TRUE(conn.rr_before_begin_);
    EXPECT_TRUE(conn.readonly_before_begin_);
    EXPECT_TRUE(conn.begin_snapshot_);
    if (0 == route) {
      EXPECT_NE(nullptr, client);
      EXPECT_EQ(tenant_id, guard.schema_scan_trans_tenant_id_);
      EXPECT_EQ(OB_SUCCESS, guard.reset());
    } else {
      EXPECT_EQ(nullptr, client);
    }
    EXPECT_EQ(nullptr, guard.schema_scan_trans_);
    EXPECT_EQ(OB_INVALID_TENANT_ID, guard.schema_scan_trans_tenant_id_);
    EXPECT_EQ(1, conn.rollback_count_);
    EXPECT_EQ(1, pool.release_count_);
    ASSERT_NO_FATAL_FAILURE(expect_session(sid, false));
  }
}

TEST_F(TestLiteScanStrongCleanup, reset_reports_rollback_failure_and_releases_connection)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  RecordingConnection conn;
  RecordingPool pool(conn);
  ASSERT_NO_FATAL_FAILURE(init_connection(conn, pool));
  const uint32_t sid = conn.get_session().get_server_sid();
  ASSERT_NO_FATAL_FAILURE(expect_session(sid, true));
  ObMySQLProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&pool));
  std::unique_ptr<ObMultiVersionSchemaService> service(new ObMultiVersionSchemaService());
  {
    ObSchemaGetterGuard guard;
    ASSERT_NO_FATAL_FAILURE(init_injected_guard(guard, *service, proxy));
    ObISQLClient *client = nullptr;
    ASSERT_EQ(OB_SUCCESS, guard.prepare_schema_scan_sql_client_(tenant_id, client));
    ASSERT_NE(nullptr, client);

    conn.rollback_ret_ = OB_TIMEOUT;
    EXPECT_EQ(OB_TIMEOUT, guard.reset());
    EXPECT_FALSE(guard.is_inited());
    EXPECT_EQ(nullptr, guard.schema_service_);
    EXPECT_EQ(nullptr, guard.schema_scan_trans_);
    EXPECT_EQ(OB_INVALID_TENANT_ID, guard.schema_scan_trans_tenant_id_);
    EXPECT_EQ(ObSchemaScanConsistencyMode::RELAXED, guard.get_schema_scan_consistency_mode());
    EXPECT_EQ(1, pool.release_count_);
    EXPECT_FALSE(pool.release_success_);
    ASSERT_NO_FATAL_FAILURE(expect_session(sid, false));
    EXPECT_EQ(OB_SUCCESS, guard.reset());
  }
  EXPECT_EQ(1, conn.rollback_count_);
  EXPECT_EQ(0, conn.commit_count_);
  EXPECT_EQ(1, pool.release_count_);
}

TEST_F(TestLiteScanStrongCleanup, scan_exit_paths_release_real_resources)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  for (int family = 0; family < 3; ++family) {
    for (int cleanup = 0; cleanup < 3; ++cleanup) {
      for (int exit_kind = 0; exit_kind < 3; ++exit_kind) {
        SCOPED_TRACE(::testing::Message() << "family=" << family << ", cleanup=" << cleanup
                                         << ", exit_kind=" << exit_kind);
        ScanResources resources;
        {
          ObSchemaGetterGuard guard;
          ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
          ASSERT_TRUE(guard.is_on_demand_schema_mode());
          guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
          ASSERT_NO_FATAL_FAILURE(leave_scan(guard, family, exit_kind));
          auto *conn = dynamic_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
          ASSERT_NE(nullptr, conn);
          ASSERT_EQ(nullptr, conn->get_prev_read_ctx());
          ASSERT_NO_FATAL_FAILURE(resources.capture(*conn));
          ASSERT_NO_FATAL_FAILURE(expect_session(resources.sid_, true));
          if (0 == cleanup) {
            EXPECT_EQ(OB_SUCCESS, guard.reset());
            EXPECT_EQ(OB_SUCCESS, guard.reset());
            EXPECT_EQ(nullptr, guard.schema_scan_trans_);
          } else if (1 == cleanup) {
            EXPECT_EQ(OB_SUCCESS, guard.reset());
            EXPECT_EQ(nullptr, guard.schema_scan_trans_);
          }
          // cleanup == 2 exercises guard destruction after caller early return.
        }
        ASSERT_NO_FATAL_FAILURE(resources.expect_released());
        ASSERT_NO_FATAL_FAILURE(expect_session(resources.sid_, false));
        // The next iteration starts a fresh scan, also checking recovery after errors.
      }
    }
  }
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
