/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <unistd.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define private public
#define protected public
#include "env/ob_simple_cluster_test_base.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_read_context.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_execute_result.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
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

// Filter the real manager traversal to one session. An unrelated older transaction
// must not hide a missing lite scan session or make the GC assertions pass.
struct CollectScanSnapshot
{
  explicit CollectScanSnapshot(const uint32_t sid) : sid_(sid), found_(false) {}
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *session)
  {
    bool keep_going = true;
    if (nullptr != session && sid_ == session->get_server_sid()) {
      found_ = true;
      keep_going = collector_(key, session);
    }
    return keep_going;
  }
  uint32_t sid_;
  bool found_;
  concurrency_control::GetMinActiveSnapshotVersionFunctor collector_;
};

struct IdleSessionGuard
{
  explicit IdleSessionGuard(sql::ObSQLSessionInfo &session)
      : session_(session), old_state_(session.get_session_state())
  { EXPECT_EQ(OB_SUCCESS, session_.set_session_state(sql::SESSION_SLEEP)); }
  ~IdleSessionGuard() { EXPECT_EQ(OB_SUCCESS, session_.set_session_state(old_state_)); }
  sql::ObSQLSessionInfo &session_;
  sql::ObSQLSessionState old_state_;
};
} // namespace

class TestLiteScanStrongGC : public ObSimpleClusterTestBase
{
public:
  TestLiteScanStrongGC() : ObSimpleClusterTestBase("test_lite_scan_strong_gc_", "10G", "20G") {}

  void SetUp() override
  {
    if (!is_started_) {
      GCONF._enable_simple_schema_on_demand = true;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ASSERT_EQ(OB_SUCCESS, create_tenant("lite_gc", "3G", "3G", false, 2));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, "lite_gc"));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("lite_gc", "test"));
    }
    int64_t affected = 0;
    // This executable owns the tenant. One-row batches make a fetch after the
    // first row enter the real operator instead of draining a prefetched batch.
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(
        "alter system set _rowsets_max_rows = 1", affected));
    bool config_ready = false;
    const int64_t config_deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000L;
    do {
      {
        omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id));
        config_ready = config.is_valid() && 1 == config->_rowsets_max_rows;
      }
      if (!config_ready) { usleep(10 * 1000); }
    } while (!config_ready && ObTimeUtility::current_time() < config_deadline);
    ASSERT_TRUE(config_ready);
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(
        "create table if not exists test.lite_gc_table(id int) partition by hash(id) partitions 4", affected));
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select database_id from oceanbase.__all_virtual_table "
        "where tenant_id = %lu and table_name = 'lite_gc_table'", tenant_id));
    ObMySQLProxy::MySQLResult result;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().read(result, sql.ptr()));
    ASSERT_NE(nullptr, result.get_result());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
    ASSERT_EQ(OB_SUCCESS, result.get_result()->get_uint("database_id", database_id_));
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
    }
    result.caller_tag_ = "mittest_strong_gc";
    return result;
  }

  void init_guard(ObSchemaGetterGuard &guard)
  {
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
    ASSERT_TRUE(guard.is_on_demand_schema_mode());
    guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  }

  void scan_tables(ObSchemaGetterGuard &guard)
  {
    ObSchemaLiteTableIterator iter;
    ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec(), iter));
    const ObSchemaLiteTableResult *row = nullptr;
    int ret = OB_SUCCESS;
    int64_t count = 0;
    while (OB_SUCCESS == (ret = iter.get_next(row))) {
      ++count;
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(1, count);
  }

  void expect_snapshot(const uint32_t sid, const bool registered, const SCN expected)
  {
    CollectScanSnapshot target(sid);
    ASSERT_EQ(OB_SUCCESS, GCTX.session_mgr_->for_each_session(target));
    EXPECT_EQ(registered, target.found_);
    EXPECT_EQ(expected, target.collector_.get_min_active_snapshot_version());
  }

  void expire_transaction(sql::ObSQLSessionInfo &session)
  {
    sql::ObSQLSessionInfo::LockGuard lock(session.get_thread_data_lock());
    auto *tx = session.get_tx_desc();
    ASSERT_NE(nullptr, tx);
    ObSpinLockGuard tx_lock(tx->lock_);
    tx->expire_ts_ = ObClockGenerator::getClock() - 1;
  }

  void arm_next_fetch_status_check(ObInnerSQLConnection &conn)
  {
    ASSERT_NE(nullptr, conn.get_prev_read_ctx());
    auto &result = conn.get_prev_read_ctx()->get_result();
    ASSERT_FALSE(result.store_first_row_);
    auto *execution = dynamic_cast<sql::ObExecuteResult *>(result.result_set().exec_result_);
    ASSERT_NE(nullptr, execution);
    auto *root = execution->static_engine_root_;
    ASSERT_NE(nullptr, root);
    if (root->get_spec().is_vectorized()) {
      ASSERT_EQ(1, root->get_spec().max_batch_size_);
      ASSERT_NE(nullptr, execution->br_it_.get_brs());
      ASSERT_EQ(1, execution->br_it_.get_brs()->size_);
      ASSERT_EQ(0, execution->br_it_.cur_idx());
      ASSERT_FALSE(execution->br_it_.get_brs()->end_);
      root->try_check_tick_ = sql::ObOperator::CHECK_STATUS_ROWS;
    } else {
      root->try_check_tick_ = sql::ObOperator::CHECK_STATUS_TRY_TIMES - 1;
    }
    // Advance only the sampling counter. The next call must still traverse
    // lite iterator -> inner result -> executor -> the production status check.
  }

  uint64_t database_id_ = OB_INVALID_ID;
};

TEST_F(TestLiteScanStrongGC, idle_snapshot_is_collected_only_in_target_tenant_until_reset)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(60 * 1000 * 1000L));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  ASSERT_NO_FATAL_FAILURE(scan_tables(guard));
  auto *conn = dynamic_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  ASSERT_NE(nullptr, conn);
  auto &session = conn->get_session();
  const uint32_t sid = session.get_server_sid();
  ASSERT_NE(uint32_t(ObInnerSQLConnection::INNER_SQL_SESS_ID), sid);
  ASSERT_TRUE(session.is_real_inner_session());
  ASSERT_EQ(tenant_id, session.get_effective_tenant_id());
  ASSERT_NE(nullptr, session.get_tx_desc());
  const SCN snapshot = session.get_tx_desc()->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());
  ASSERT_FALSE(session.get_tx_desc()->is_tx_timeout());
  ASSERT_EQ(nullptr, conn->get_prev_read_ctx());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  {
    // Inner connections may keep QUERY_ACTIVE between statements. Explicitly
    // exercise the idle state too, so RC's active-session fallback cannot pass.
    IdleSessionGuard idle(session);
    ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  }

  {
    ObTenantSwitchGuard sys_tenant;
    ASSERT_EQ(OB_SUCCESS, sys_tenant.switch_to(OB_SYS_TENANT_ID));
    ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, SCN::max_scn()));
  }
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  ObSchemaLitePartJoinIterator parts;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts));
  const ObSchemaLiteJoinRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, parts.get_next(row));
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  parts.reset();
  ASSERT_EQ(OB_SUCCESS, guard.reset());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, false, SCN::max_scn()));
}

TEST_F(TestLiteScanStrongGC, explicit_transaction_has_deadline_and_expired_scan_cannot_continue)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  const int64_t trx_timeout = 60 * 1000 * 1000L;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(90 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(trx_timeout));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  const int64_t before_begin = ObClockGenerator::getClock();
  ASSERT_NO_FATAL_FAILURE(scan_tables(guard));
  auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  auto &session = conn->get_session();
  auto *tx = session.get_tx_desc();
  ASSERT_NE(nullptr, tx);
  const uint32_t sid = session.get_server_sid();
  ASSERT_EQ(transaction::ObTxDesc::State::ACTIVE, tx->state_);
  EXPECT_EQ(trx_timeout, tx->get_timeout_us());
  EXPECT_LT(tx->expire_ts_, INT64_MAX);
  EXPECT_GE(tx->expire_ts_, before_begin + trx_timeout);
  EXPECT_LE(tx->expire_ts_, ObClockGenerator::getClock() + trx_timeout);
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, tx->get_tx_snapshot_version()));

  // Move the real descriptor's deadline into the past, without a wall-clock sleep.
  // No cursor is open, so this models timeout between two scan statements.
  ASSERT_NO_FATAL_FAILURE(expire_transaction(session));
  ASSERT_TRUE(tx->is_tx_timeout());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, SCN::max_scn()));
  ObSchemaLitePartJoinIterator parts;
  int ret = guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts);
  const ObSchemaLiteJoinRow *row = nullptr;
  while (OB_SUCCESS == ret) {
    ret = parts.get_next(row);
  }
  EXPECT_EQ(OB_TRANS_TIMEOUT, ret);
  parts.reset();
  EXPECT_EQ(OB_SUCCESS, guard.reset());
  EXPECT_EQ(nullptr, guard.schema_scan_trans_);
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, false, SCN::max_scn()));
}

TEST_F(TestLiteScanStrongGC, open_cursor_has_sql_deadline_and_query_timeout_does_not_extend_transaction)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(90 * 1000 * 1000L));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  ObSchemaLitePartJoinIterator parts;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts));
  const ObSchemaLiteJoinRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, parts.get_next(row));
  auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  auto *tx = conn->get_session().get_tx_desc();
  ASSERT_NE(nullptr, tx);
  const int64_t tx_deadline = tx->expire_ts_;
  const uint32_t sid = conn->get_session().get_server_sid();
  const SCN snapshot = tx->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  ASSERT_NE(nullptr, conn->get_prev_read_ctx());
  auto &exec_ctx = conn->get_prev_read_ctx()->get_result().result_set().get_exec_context();
  auto *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ASSERT_NE(nullptr, plan_ctx);
  EXPECT_GT(plan_ctx->get_timeout_timestamp(), ObTimeUtility::current_time());
  EXPECT_LE(plan_ctx->get_timeout_timestamp(), timeout.get_abs_timeout());
  EXPECT_LT(plan_ctx->get_timeout_timestamp(), tx_deadline);
  ASSERT_NO_FATAL_FAILURE(arm_next_fetch_status_check(*conn));
  // Do not call check_status() directly: dropping the check from the real fetch
  // path must make this assertion fail. Three partition rows remain unread.
  // Use the same cached clock as the executor's timeout check.
  plan_ctx->set_timeout_timestamp(ObClockGenerator::getClock() - 1);
  ASSERT_TRUE(plan_ctx->is_exec_timeout());
  EXPECT_EQ(OB_TIMEOUT, parts.get_next(row));
  EXPECT_EQ(nullptr, row);
  parts.reset();
  ASSERT_EQ(nullptr, conn->get_prev_read_ctx());
  // A statement error may abort/release the transaction. If it remains active,
  // its original deadline must not be extended by the failed statement.
  auto *remaining_tx = conn->get_session().get_tx_desc();
  if (nullptr != remaining_tx && remaining_tx->is_tx_active()) {
    EXPECT_LE(remaining_tx->expire_ts_, tx_deadline);
  }

  {
    ObTimeoutCtx expired_query;
    ASSERT_EQ(OB_SUCCESS, expired_query.set_abs_timeout(ObTimeUtility::current_time() - 1));
    EXPECT_EQ(OB_TIMEOUT, guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts));
    parts.reset();
  }
  EXPECT_EQ(OB_SUCCESS, guard.reset());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, false, SCN::max_scn()));
}

TEST_F(TestLiteScanStrongGC, transaction_expires_during_fetch_before_sql_deadline)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(90 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(5 * 1000 * 1000L));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  constexpr int64_t PART_COUNT = 4;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id, database_id_,
      ObString::make_string("lite_gc_table"), false, table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(PART_COUNT, table_schema->get_partition_num());
  const uint64_t table_id = table_schema->get_table_id();
  auto *partitions = table_schema->get_part_array();
  ASSERT_NE(nullptr, partitions);
  for (int64_t i = 0; i < PART_COUNT; ++i) {
    ASSERT_NE(nullptr, partitions[i]);
  }
  ObSchemaLitePartJoinIterator parts;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts));
  const ObSchemaLiteJoinRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, parts.get_next(row));
  ASSERT_NE(nullptr, row);
  auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  auto &session = conn->get_session();
  ASSERT_NE(nullptr, session.get_tx_desc());
  const uint32_t sid = session.get_server_sid();
  const SCN snapshot = session.get_tx_desc()->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, snapshot));
  ASSERT_NE(nullptr, conn->get_prev_read_ctx());
  auto *plan_ctx = conn->get_prev_read_ctx()->get_result().result_set()
      .get_exec_context().get_physical_plan_ctx();
  ASSERT_NE(nullptr, plan_ctx);
  ASSERT_NO_FATAL_FAILURE(arm_next_fetch_status_check(*conn));
  // Let the real deadline expire so any statement deadline derived from BEGIN
  // remains intact; changing only tx->expire_ts_ could break that relationship.
  const int64_t wait_deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
  while (!session.get_tx_desc()->is_tx_timeout()
         && ObTimeUtility::current_time() < wait_deadline) {
    usleep(10 * 1000);
  }
  ASSERT_TRUE(session.get_tx_desc()->is_tx_timeout());
  const int64_t sql_deadline = plan_ctx->get_timeout_timestamp();
  ASSERT_GT(sql_deadline, ObTimeUtility::current_time());

  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, SCN::max_scn()));
  // Without forced table refresh, the open SQL can finish after transaction
  // expiry. Session snapshot collection alone does not cover read-handle protection.
  bool seen_parts[PART_COUNT] = {};
  int64_t row_count = 0;
  int ret = OB_SUCCESS;
  do {
    ASSERT_NE(nullptr, row);
    EXPECT_EQ(table_id, row->table_id_);
    int64_t part_idx = 0;
    while (part_idx < PART_COUNT && partitions[part_idx]->get_part_id() != row->part_id_) {
      ++part_idx;
    }
    ASSERT_LT(part_idx, PART_COUNT);
    ASSERT_FALSE(seen_parts[part_idx]);
    seen_parts[part_idx] = true;
    EXPECT_EQ(partitions[part_idx]->get_tablet_id(), row->tablet_id_);
    ++row_count;
  } while (OB_SUCCESS == (ret = parts.get_next(row)));
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(PART_COUNT, row_count);
  EXPECT_GT(sql_deadline, ObTimeUtility::current_time());
  parts.reset();
  ASSERT_EQ(nullptr, conn->get_prev_read_ctx());
  ASSERT_NE(nullptr, session.get_tx_desc());
  EXPECT_EQ(snapshot, session.get_tx_desc()->get_tx_snapshot_version());
  EXPECT_TRUE(session.get_tx_desc()->is_tx_timeout());
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, true, SCN::max_scn()));

  // Keep the guard: a new scan must fail in the expired transaction instead of
  // silently starting another transaction with a new snapshot.
  ret = guard.scan_lite_part_rows(spec(true), ObSchemaLiteJoinSide::PART, parts);
  if (OB_SUCCESS == ret) {
    ret = parts.get_next(row);
  }
  EXPECT_EQ(OB_TRANS_TIMEOUT, ret);
  EXPECT_EQ(nullptr, row);
  parts.reset();
  EXPECT_EQ(OB_SUCCESS, guard.reset());
  EXPECT_EQ(nullptr, guard.schema_scan_trans_);
  ASSERT_NO_FATAL_FAILURE(expect_snapshot(sid, false, SCN::max_scn()));
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
