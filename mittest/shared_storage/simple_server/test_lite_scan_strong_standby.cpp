/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <cstdlib>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define private public
#define protected public
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tx/ob_trans_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#undef protected
#undef private

namespace oceanbase
{
// Same bootstrap substitution as the other SS observer mittests. Tenant role,
// BEGIN, snapshot acquisition, SQL execution and transaction cleanup are real.
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;

namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace observer;
using namespace transaction;

namespace
{
using JoinKey = std::tuple<uint64_t, int64_t, int64_t, uint64_t>;
struct RunContext
{
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t database_id_ = OB_INVALID_ID;
  bool ready_ = false;
  std::vector<uint64_t> tables_;
  std::vector<JoinKey> parts_;
  std::vector<JoinKey> subparts_;
} run;

struct CollectSnapshot
{
  explicit CollectSnapshot(const uint32_t sid) : sid_(sid) {}
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *session)
  {
    if (nullptr != session && sid_ == session->get_server_sid()) {
      found_ = true;
      return collector_(key, session);
    }
    return true;
  }
  uint32_t sid_;
  bool found_ = false;
  concurrency_control::GetMinActiveSnapshotVersionFunctor collector_;
};

// A standby descriptor is not in the txid map. Inspect its owning connection
// and held session instead; debug builds also check the descriptor allocator.
struct ScanResources
{
  void capture(ObInnerSQLConnection &conn)
  {
    pool_ = conn.pool_;
    ASSERT_NE(nullptr, pool_);
    conn_addr_ = reinterpret_cast<uint64_t>(&conn);
    conn_init_ts_ = conn.get_init_timestamp();
    session_addr_ = reinterpret_cast<uint64_t>(&conn.get_session());
    sid_ = conn.get_session().get_server_sid();
    auto *tx = conn.get_session().get_tx_desc();
    ASSERT_NE(nullptr, tx);
    ASSERT_FALSE(tx->get_tx_id().is_valid());
    ASSERT_EQ(sid_, tx->sess_id_);
    tx_addr_ = reinterpret_cast<uint64_t>(tx);
    tx_service_ = MTL(ObTransService *);
    ASSERT_NE(nullptr, tx_service_);
    ASSERT_EQ(nullptr, conn.get_prev_read_ctx());
    ASSERT_EQ(1, conn.get_ref());
  }

  void expect_released()
  {
    bool connection_used = true;
    bool session_held = true;
    bool tx_allocated = false;
    const int64_t deadline = ObTimeUtility::current_time() + 2 * 1000 * 1000L;
    do {
      connection_used = false;
      {
        ObThreadCondGuard lock(pool_->cond_);
        DLIST_FOREACH_NORET(conn, pool_->used_conn_list_) {
          connection_used |= conn_addr_ == reinterpret_cast<uint64_t>(conn)
              && conn_init_ts_ == conn->get_init_timestamp();
        }
      }
      session_held = false;
      auto find_session = [&](hash::HashMapPair<uint64_t, sql::ObSQLSessionInfo *> &entry) {
        session_held |= session_addr_ == entry.first && nullptr != entry.second
            && sid_ == entry.second->get_server_sid();
        return OB_SUCCESS;
      };
      ASSERT_EQ(OB_SUCCESS, GCTX.session_mgr_->for_each_hold_session(find_session));
#ifdef ENABLE_DEBUG_LOG
      tx_allocated = false;
      auto find_tx = [&](ObTxDesc *tx) {
        tx_allocated |= tx_addr_ == reinterpret_cast<uint64_t>(tx) && sid_ == tx->sess_id_;
        return OB_SUCCESS;
      };
      ASSERT_EQ(OB_SUCCESS, tx_service_->tx_desc_mgr_.map_.alloc_handle_.for_each(find_tx));
#endif
      if (!connection_used && !session_held && !tx_allocated) { break; }
      ob_usleep(1000);
    } while (ObTimeUtility::current_time() < deadline);
    EXPECT_FALSE(connection_used);
    EXPECT_FALSE(session_held);
#ifdef ENABLE_DEBUG_LOG
    EXPECT_FALSE(tx_allocated);
#endif
  }

  ObInnerSQLConnectionPool *pool_ = nullptr;
  ObTransService *tx_service_ = nullptr;
  uint64_t conn_addr_ = 0;
  int64_t conn_init_ts_ = 0;
  uint64_t session_addr_ = 0;
  uint64_t tx_addr_ = 0;
  uint32_t sid_ = 0;
};
} // namespace

class TestLiteScanStrongStandby : public ObSimpleClusterTestBase
{
public:
  TestLiteScanStrongStandby()
      : ObSimpleClusterTestBase("test_lite_scan_strong_standby_", "8G", "20G", "12G") {}

  void SetUp() override
  {
    if (!is_started_) { GCONF._enable_simple_schema_on_demand = true; }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_TRUE(GCTX.is_shared_storage_mode());
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());
    if (!run.ready_) {
      // A failed setup must fail subsequent cases too, not reuse a half-switched tenant.
      ASSERT_EQ(OB_INVALID_TENANT_ID, run.tenant_id_);
      ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("lite_standby", "3G", "3G", false, 2, "4G"));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(run.tenant_id_, "lite_standby"));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("lite_standby", "test"));
      ASSERT_EQ(OB_SUCCESS, exec("create database lite_standby_db"));
      ASSERT_EQ(OB_SUCCESS, exec("create table lite_standby_db.st_plain(id int primary key)"));
      ASSERT_EQ(OB_SUCCESS, exec("create table lite_standby_db.st_one(id int) "
          "partition by range(id) (partition p0 values less than (100), partition p1 values less than (200))"));
      ASSERT_EQ(OB_SUCCESS, exec("create table lite_standby_db.st_two(id int, v int) "
          "partition by range(id) subpartition by hash(v) subpartitions 2 "
          "(partition p0 values less than (100), partition p1 values less than (200))"));
      {
        ObSqlString sql;
        ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select database_id from oceanbase.__all_virtual_table "
            "where tenant_id = %lu and table_name = 'st_plain'", run.tenant_id_));
        ObMySQLProxy::MySQLResult result;
        ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().read(result, sql.ptr()));
        ASSERT_NE(nullptr, result.get_result());
        ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
        ASSERT_EQ(OB_SUCCESS, result.get_result()->get_uint("database_id", run.database_id_));
      }
      {
        ObTenantSwitchGuard tenant;
        ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
        ObSchemaGetterGuard baseline;
        ASSERT_NO_FATAL_FAILURE(init_guard(baseline));
        ASSERT_NO_FATAL_FAILURE(scan_tables(baseline, run.tables_));
        ASSERT_NO_FATAL_FAILURE(scan_parts(baseline, ObSchemaLiteJoinSide::PART, run.parts_));
        ASSERT_NO_FATAL_FAILURE(scan_parts(baseline, ObSchemaLiteJoinSide::SUBPART, run.subparts_));
        ASSERT_EQ(3, run.tables_.size());
        ASSERT_EQ(4, run.parts_.size());
        ASSERT_EQ(4, run.subparts_.size());
        ASSERT_EQ(OB_SUCCESS, baseline.reset());
      }
      {
        // Keep timeout settings and the switchover on the same sys SQL session.
        ObSingleConnectionProxy sys;
        int64_t affected = 0;
        ASSERT_EQ(OB_SUCCESS, sys.connect(OB_SYS_TENANT_ID, 0,
            &get_curr_simple_server().get_sql_proxy()));
        ASSERT_EQ(OB_SUCCESS, sys.write(OB_SYS_TENANT_ID,
            "set session ob_query_timeout = 120000000", affected));
        ASSERT_EQ(OB_SUCCESS, sys.write(OB_SYS_TENANT_ID,
            "set session ob_trx_timeout = 120000000", affected));
        ASSERT_EQ(OB_SUCCESS, sys.write(OB_SYS_TENANT_ID,
            "alter system switchover to standby tenant 'lite_standby'", affected));
      }
      ASSERT_NO_FATAL_FAILURE(wait_for_standby());
      run.ready_ = true;
    }
    ObTenantSwitchGuard tenant;
    ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
    ASSERT_EQ(ObTenantRole::STANDBY_TENANT, MTL_GET_TENANT_ROLE_CACHE());
    ObTransID id;
    ASSERT_EQ(OB_STANDBY_READ_ONLY, MTL(ObTransService *)->gen_trans_id(id));
    ASSERT_FALSE(id.is_valid());
  }

  int exec(const char *sql)
  {
    int64_t affected = 0;
    return get_curr_simple_server().get_sql_proxy2().write(sql, affected);
  }

  void wait_for_standby()
  {
    const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000L;
    ObTenantSwitchGuard tenant;
    ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
    bool ready = false;
    int snapshot_ret = OB_GTS_NOT_READY;
    do {
      ObAllTenantInfo info;
      ASSERT_EQ(OB_SUCCESS, ObAllTenantInfoProxy::load_tenant_info(
          run.tenant_id_, GCTX.sql_proxy_, false, info));
      if (info.is_standby() && info.is_normal_status()
          && ObTenantRole::STANDBY_TENANT == MTL_GET_TENANT_ROLE_CACHE()) {
        SCN readable;
        snapshot_ret = MTL(ObTransService *)->get_read_snapshot_version(deadline, readable);
        ready = OB_SUCCESS == snapshot_ret && readable.is_valid();
        ASSERT_TRUE(OB_SUCCESS == snapshot_ret || OB_GTS_NOT_READY == snapshot_ret
            || OB_TIMEOUT == snapshot_ret) << snapshot_ret;
      }
      if (!ready) { ob_usleep(10 * 1000); }
    } while (!ready && ObTimeUtility::current_time() < deadline);
    ASSERT_TRUE(ready) << "standby role/readable snapshot not ready: " << snapshot_ret;
  }

  void init_guard(ObSchemaGetterGuard &guard)
  {
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(run.tenant_id_, guard));
    ASSERT_TRUE(guard.is_on_demand_schema_mode());
    guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  }

  ObSchemaLiteScanSpec spec(const bool join = false) const
  {
    ObSchemaLiteScanSpec spec;
    spec.table_spec_.tenant_id_ = run.tenant_id_;
    spec.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
    spec.table_spec_.filter_id_ = run.database_id_;
    spec.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID;
    if (join) {
      spec.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID | OB_SCHEMA_LITE_PART_TABLET_ID;
      spec.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
          | OB_SCHEMA_LITE_SUBPART_TABLET_ID;
    }
    spec.caller_tag_ = "mittest_strong_standby";
    return spec;
  }

  void scan_tables(ObSchemaGetterGuard &guard, std::vector<uint64_t> &ids)
  {
    ObSchemaLiteTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec(), iter));
    const ObSchemaLiteTableResult *row = nullptr;
    ids.clear();
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter.get_next(row))) {
      ASSERT_NE(nullptr, row);
      ids.push_back(row->table_id_);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    std::sort(ids.begin(), ids.end());
  }

  void scan_parts(ObSchemaGetterGuard &guard, ObSchemaLiteJoinSide side, std::vector<JoinKey> &rows)
  {
    ObSchemaLitePartJoinIterator iter;
    ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true), side, iter));
    const ObSchemaLiteJoinRow *row = nullptr;
    rows.clear();
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter.get_next(row))) {
      ASSERT_NE(nullptr, row);
      rows.emplace_back(row->table_id_, row->part_id_, row->sub_part_id_, row->tablet_id_.id());
    }
    ASSERT_EQ(OB_ITER_END, ret);
    std::sort(rows.begin(), rows.end());
  }

  void expect_gc(const uint32_t sid, const bool present, const SCN &snapshot)
  {
    CollectSnapshot target(sid);
    ASSERT_EQ(OB_SUCCESS, GCTX.session_mgr_->for_each_session(target));
    EXPECT_EQ(present, target.found_);
    EXPECT_EQ(snapshot, target.collector_.get_min_active_snapshot_version());
  }
};

TEST_F(TestLiteScanStrongStandby, table_part_subpart_share_snapshot_without_txid)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(60 * 1000 * 1000L));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  std::vector<uint64_t> ids;
  ASSERT_NO_FATAL_FAILURE(scan_tables(guard, ids));
  EXPECT_EQ(run.tables_, ids);
  ASSERT_NE(nullptr, guard.schema_scan_trans_);
  auto *conn = dynamic_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  ASSERT_NE(nullptr, conn);
  auto &session = conn->get_session();
  ASSERT_TRUE(session.is_real_inner_session());
  ASSERT_NE(uint32_t(ObInnerSQLConnection::INNER_SQL_SESS_ID), session.get_server_sid());
  ASSERT_EQ(run.tenant_id_, session.get_effective_tenant_id());
  ASSERT_EQ(OB_INVALID_ID, conn->get_resource_conn_id());
  auto *tx = session.get_tx_desc();
  ASSERT_NE(nullptr, tx);
  EXPECT_EQ(ObTxAccessMode::STANDBY_RD_ONLY, tx->get_tx_access_mode());
  EXPECT_EQ(ObTxIsolationLevel::RR, tx->isolation_);
  EXPECT_EQ(ObTxDesc::State::ACTIVE, tx->state_);
  EXPECT_TRUE(tx->is_valid());
  EXPECT_TRUE(tx->is_clean());
  EXPECT_FALSE(tx->is_xa_trans());
  EXPECT_FALSE(tx->get_tx_id().is_valid());
  EXPECT_LT(tx->expire_ts_, INT64_MAX);
  EXPECT_FALSE(tx->is_tx_timeout());
  const SCN snapshot = tx->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());
  std::vector<JoinKey> rows;
  for (auto side : {ObSchemaLiteJoinSide::PART, ObSchemaLiteJoinSide::SUBPART}) {
    ASSERT_NO_FATAL_FAILURE(scan_parts(guard, side, rows));
    EXPECT_EQ(side == ObSchemaLiteJoinSide::PART ? run.parts_ : run.subparts_, rows);
    ASSERT_EQ(conn, guard.schema_scan_trans_->get_connection());
    ASSERT_EQ(tx, session.get_tx_desc());
    EXPECT_EQ(snapshot, tx->get_tx_snapshot_version());
    EXPECT_FALSE(tx->get_tx_id().is_valid());
  }
  // The non-lite enumeration must use the same standby scan transaction too.
  ObArray<uint64_t> table_ids;
  ASSERT_EQ(OB_SUCCESS, guard.get_table_ids_in_database(run.tenant_id_, run.database_id_, table_ids));
  std::vector<uint64_t> enumerated;
  for (int64_t i = 0; i < table_ids.count(); ++i) { enumerated.push_back(table_ids.at(i)); }
  std::sort(enumerated.begin(), enumerated.end());
  EXPECT_EQ(run.tables_, enumerated);
  ASSERT_EQ(conn, guard.schema_scan_trans_->get_connection());
  ASSERT_EQ(tx, session.get_tx_desc());
  EXPECT_EQ(snapshot, tx->get_tx_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, guard.reset());
}

TEST_F(TestLiteScanStrongStandby, no_txid_snapshot_is_gc_visible_and_cleanup_releases_resources)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
  for (int cleanup = 0; cleanup < 3; ++cleanup) {
    SCOPED_TRACE(cleanup);
    ObTimeoutCtx timeout;
    ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
    ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(60 * 1000 * 1000L));
    ScanResources resources;
    {
      ObSchemaGetterGuard guard;
      ASSERT_NO_FATAL_FAILURE(init_guard(guard));
      std::vector<uint64_t> ids;
      ASSERT_NO_FATAL_FAILURE(scan_tables(guard, ids));
      ASSERT_EQ(run.tables_, ids);
      auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
      ASSERT_NO_FATAL_FAILURE(resources.capture(*conn));
      auto &session = conn->get_session();
      const SCN snapshot = session.get_tx_desc()->get_tx_snapshot_version();
      ASSERT_TRUE(snapshot.is_valid());
      {
        const auto old_state = session.get_session_state();
        auto restore = MAKE_SCOPE(old_state, [&](const sql::ObSQLSessionState &state) {
          EXPECT_EQ(OB_SUCCESS, session.set_session_state(state));
        });
        ASSERT_EQ(OB_SUCCESS, session.set_session_state(sql::SESSION_SLEEP));
        ASSERT_NO_FATAL_FAILURE(expect_gc(resources.sid_, true, snapshot));
      }
      if (0 == cleanup) {
        EXPECT_EQ(OB_SUCCESS, guard.release_schema_scan_trans_early());
        EXPECT_TRUE(guard.is_inited());
        EXPECT_EQ(nullptr, guard.schema_scan_trans_);
      } else if (1 == cleanup) {
        EXPECT_EQ(OB_SUCCESS, guard.reset());
        EXPECT_FALSE(guard.is_inited());
        EXPECT_EQ(nullptr, guard.schema_scan_trans_);
      }
      // cleanup == 2 leaves the transaction to the guard destructor.
    }
    ASSERT_NO_FATAL_FAILURE(resources.expect_released());
    ASSERT_NO_FATAL_FAILURE(expect_gc(resources.sid_, false, SCN::max_scn()));
  }
}

TEST_F(TestLiteScanStrongStandby, start_tx_whitelist_rejects_unapproved_write_xa_and_participants)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run.tenant_id_));
  auto *service = MTL(ObTransService *);
  ASSERT_NE(nullptr, service);
  for (int rejected = 0; rejected < 4; ++rejected) {
    SCOPED_TRACE(rejected);
    ObTxDesc *tx = nullptr;
    ASSERT_EQ(OB_SUCCESS, service->acquire_tx(tx));
    ASSERT_NE(nullptr, tx);
    auto release = MAKE_SCOPE(tx, [&](ObTxDesc *desc) {
      // Remove only the synthetic admission-test state before cleanup.
      desc->parts_.reset();
      desc->xid_.reset();
      if (desc->is_tx_active()) { EXPECT_EQ(OB_SUCCESS, service->stop_tx(*desc)); }
      EXPECT_EQ(OB_SUCCESS, service->release_tx(*desc));
    });
    ObTxParam param;
    EXPECT_FALSE(param.allow_standby_read_only_);
    param.allow_standby_read_only_ = 0 != rejected;
    param.cluster_id_ = GCONF.cluster_id;
    param.timeout_us_ = 60 * 1000 * 1000L;
    param.isolation_ = ObTxIsolationLevel::RR;
    param.access_mode_ = 1 == rejected ? ObTxAccessMode::RW : ObTxAccessMode::RD_ONLY;
    if (2 == rejected) {
      ASSERT_EQ(OB_SUCCESS, tx->xid_.set(ObString("standby_xa"), ObString("branch"), 1));
    } else if (3 == rejected) {
      ObTxPart part;
      part.id_ = SYS_LS;
      ASSERT_EQ(OB_SUCCESS, tx->parts_.push_back(part));
    }
    EXPECT_EQ(OB_STANDBY_READ_ONLY, service->start_tx(*tx, param));
    EXPECT_FALSE(tx->is_tx_active());
    EXPECT_FALSE(tx->get_tx_id().is_valid());
  }
}

TEST_F(TestLiteScanStrongStandby, ordinary_user_readonly_begin_is_not_whitelisted)
{
  // The network proxy already logs in as root@lite_standby; no inner-SQL tenant routing.
  EXPECT_EQ(OB_STANDBY_READ_ONLY, exec("start transaction read only"));
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  const char *endpoint = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool local_endpoint = nullptr != endpoint && '\0' != endpoint[0];
  const char *bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (nullptr == bucket || '\0' == bucket[0]) { bucket = unittest::S3_BUCKET; }
  const char *region = std::getenv("OB_MITTEST_S3_REGION");
  if (nullptr == region || '\0' == region[0]) { region = unittest::S3_REGION; }
  char storage_info[1000] = {0};
  const int ret = common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      bucket, common::ObTimeUtility::current_time_ns(),
      local_endpoint ? endpoint : unittest::S3_ENDPOINT,
      local_endpoint ? "minioadmin" : unittest::S3_AK,
      local_endpoint ? "minioadmin" : unittest::S3_SK, region);
  if (common::OB_SUCCESS != ret) { return ret; }
  shared_storage_info = storage_info;
  unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("12G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
