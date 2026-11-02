/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define private public
#define protected public
#include "env/ob_simple_cluster_test_base.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_schema_scan_iterator.h"
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
using JoinKey = std::tuple<uint64_t, int64_t, int64_t, uint64_t>;

int drain_tables(ObSchemaLiteTableIterator &iter, std::vector<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  const ObSchemaLiteTableResult *row = nullptr;
  ids.clear();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(row))) {
      break;
    } else {
      ids.push_back(row->table_id_);
    }
  }
  // Do not turn query errors into a successful, truncated enumeration.
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  std::sort(ids.begin(), ids.end());
  return ret;
}

int drain_parts(ObSchemaLitePartJoinIterator &iter, std::vector<JoinKey> &keys)
{
  int ret = OB_SUCCESS;
  const ObSchemaLiteJoinRow *row = nullptr;
  keys.clear();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(row))) {
      break;
    } else {
      keys.emplace_back(row->table_id_, row->part_id_, row->sub_part_id_, row->tablet_id_.id());
    }
  }
  std::sort(keys.begin(), keys.end());
  return OB_ITER_END == ret ? OB_SUCCESS : ret;
}
} // namespace

class TestLiteScanStrongSnapshot : public ObSimpleClusterTestBase
{
public:
  TestLiteScanStrongSnapshot()
      : ObSimpleClusterTestBase("test_lite_scan_strong_snapshot_", "10G", "20G"), database_id_(0)
  {}

  void SetUp() override
  {
    if (!is_started_) {
      GCONF._enable_simple_schema_on_demand = true;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ASSERT_EQ(OB_SUCCESS, create_tenant("lite_snapshot", "3G", "3G", false, 2));
      ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, "lite_snapshot"));
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("lite_snapshot", "test"));
    }
    ASSERT_EQ(OB_SUCCESS, exec("drop database if exists lite_snapshot_db"));
    ASSERT_EQ(OB_SUCCESS, exec("drop tablegroup if exists lite_snapshot_tg"));
    ASSERT_EQ(OB_SUCCESS, exec("create database lite_snapshot_db"));
    ASSERT_EQ(OB_SUCCESS, exec("create table lite_snapshot_db.lsnap_plain(id int primary key)"));
    ASSERT_EQ(OB_SUCCESS, exec("create table lite_snapshot_db.lsnap_one(id int) "
        "partition by range(id) (partition p0 values less than (100), partition p1 values less than (200))"));
    ASSERT_EQ(OB_SUCCESS, exec("create table lite_snapshot_db.lsnap_two(id int, v int) "
        "partition by range(id) subpartition by hash(v) subpartitions 2 "
        "(partition p0 values less than (100), partition p1 values less than (200))"));
    uint64_t unused_table_id = 0;
    ASSERT_EQ(OB_SUCCESS, lookup_table("lsnap_plain", unused_table_id, database_id_));
  }

  int exec(const char *sql)
  {
    int64_t affected = 0;
    // This is an independent network connection, never the scan's inner connection.
    return get_curr_simple_server().get_sql_proxy2().write(sql, affected);
  }

  int lookup_table(const char *name, uint64_t &table_id, uint64_t &database_id)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    ObMySQLProxy::MySQLResult result;
    if (OB_FAIL(sql.assign_fmt("select table_id, database_id from oceanbase.__all_virtual_table "
                              "where tenant_id = %lu and table_name = '%s'", tenant_id, name))) {
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result.get_result()->next())) {
    } else if (OB_FAIL(result.get_result()->get_uint("table_id", table_id))) {
    } else if (OB_FAIL(result.get_result()->get_uint("database_id", database_id))) {
    }
    return ret;
  }

  ObSchemaLiteScanSpec spec(const bool join = false) const
  {
    ObSchemaLiteScanSpec result;
    result.table_spec_.tenant_id_ = tenant_id;
    result.table_spec_.kind_ = ObSchemaScanKind::BY_DATABASE;
    result.table_spec_.filter_id_ = database_id_;
    result.table_spec_.select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
        | OB_SCHEMA_SCAN_SELECT_PART_LEVEL | OB_SCHEMA_SCAN_SELECT_TABLET_ID;
    if (join) {
      result.part_flags_ = OB_SCHEMA_LITE_PART_PART_ID | OB_SCHEMA_LITE_PART_TABLET_ID;
      result.subpart_flags_ = OB_SCHEMA_LITE_SUBPART_PART_ID | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
          | OB_SCHEMA_LITE_SUBPART_TABLET_ID;
    }
    result.caller_tag_ = "mittest_strong_snapshot";
    return result;
  }

  void init_guard(ObSchemaGetterGuard &guard)
  {
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
    ASSERT_TRUE(guard.is_on_demand_schema_mode());
    guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  }

  void collect_parts(ObSchemaGetterGuard &guard, const ObSchemaLiteJoinSide side,
                     std::vector<JoinKey> &keys)
  {
    ObSchemaLitePartJoinIterator iter;
    ASSERT_EQ(OB_SUCCESS, guard.scan_lite_part_rows(spec(true), side, iter));
    ASSERT_EQ(OB_SUCCESS, drain_parts(iter, keys));
  }

  uint64_t database_id_;
};

TEST_F(TestLiteScanStrongSnapshot, table_part_subpart_keep_snapshot_across_committed_ddl)
{
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(60 * 1000 * 1000L));
  std::vector<JoinKey> expected_parts;
  std::vector<JoinKey> expected_subparts;
  {
    ObSchemaGetterGuard baseline;
    ASSERT_NO_FATAL_FAILURE(init_guard(baseline));
    ASSERT_NO_FATAL_FAILURE(collect_parts(baseline, ObSchemaLiteJoinSide::PART, expected_parts));
    ASSERT_NO_FATAL_FAILURE(collect_parts(baseline, ObSchemaLiteJoinSide::SUBPART, expected_subparts));
    ASSERT_EQ(4, expected_parts.size());
    ASSERT_EQ(4, expected_subparts.size());
    ASSERT_EQ(OB_SUCCESS, baseline.reset());
  }

  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  ObSchemaLiteTableIterator tables;
  std::vector<uint64_t> ids;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec(), tables));
  ASSERT_EQ(OB_SUCCESS, drain_tables(tables, ids));
  ASSERT_EQ(3, ids.size());
  auto *conn = dynamic_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  ASSERT_NE(nullptr, conn);
  auto *tx = conn->get_session().get_tx_desc();
  ASSERT_NE(nullptr, tx);
  ASSERT_EQ(transaction::ObTxIsolationLevel::RR, tx->isolation_);
  ASSERT_TRUE(tx->is_rdonly());
  const SCN snapshot = tx->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());
  ASSERT_EQ(nullptr, conn->get_prev_read_ctx());

  ASSERT_EQ(OB_SUCCESS, exec("create table lite_snapshot_db.lsnap_new(id int, v int) "
      "partition by range(id) subpartition by hash(v) subpartitions 2 "
      "(partition p0 values less than (100), partition p1 values less than (200))"));
  ASSERT_EQ(OB_SUCCESS, exec("alter table lite_snapshot_db.lsnap_two drop partition p0"));

  // A separate current read proves that DDL is visible; stale publication cannot make this pass.
  std::vector<JoinKey> current_parts;
  std::vector<JoinKey> current_subparts;
  {
    ObSchemaGetterGuard current;
    ASSERT_NO_FATAL_FAILURE(init_guard(current));
    ASSERT_NO_FATAL_FAILURE(collect_parts(current, ObSchemaLiteJoinSide::PART, current_parts));
    ASSERT_NO_FATAL_FAILURE(collect_parts(current, ObSchemaLiteJoinSide::SUBPART, current_subparts));
    ASSERT_NE(expected_parts, current_parts);
    ASSERT_NE(expected_subparts, current_subparts);
    ASSERT_EQ(5, current_parts.size());
    ASSERT_EQ(6, current_subparts.size());
    ASSERT_EQ(OB_SUCCESS, current.reset());
  }

  std::vector<JoinKey> actual;
  ASSERT_NO_FATAL_FAILURE(collect_parts(guard, ObSchemaLiteJoinSide::PART, actual));
  EXPECT_EQ(expected_parts, actual);
  EXPECT_EQ(conn, guard.schema_scan_trans_->get_connection());
  EXPECT_EQ(tx, conn->get_session().get_tx_desc());
  EXPECT_EQ(snapshot, tx->get_tx_snapshot_version());
  ASSERT_NO_FATAL_FAILURE(collect_parts(guard, ObSchemaLiteJoinSide::SUBPART, actual));
  EXPECT_EQ(expected_subparts, actual);
  EXPECT_EQ(snapshot, tx->get_tx_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, guard.reset());

  ObSchemaGetterGuard fresh;
  ASSERT_NO_FATAL_FAILURE(init_guard(fresh));
  ASSERT_EQ(OB_SUCCESS, fresh.scan_lite_tables(spec(), tables));
  ASSERT_EQ(OB_SUCCESS, drain_tables(tables, ids));
  EXPECT_EQ(4, ids.size());
  ASSERT_NO_FATAL_FAILURE(collect_parts(fresh, ObSchemaLiteJoinSide::SUBPART, actual));
  EXPECT_EQ(current_subparts, actual);
  EXPECT_NE(snapshot, static_cast<ObInnerSQLConnection *>(fresh.schema_scan_trans_->get_connection())
      ->get_session().get_tx_desc()->get_tx_snapshot_version());
  EXPECT_EQ(OB_SUCCESS, fresh.reset());
}

TEST_F(TestLiteScanStrongSnapshot, tablegroup_enumeration_and_lite_scan_share_transaction)
{
  ASSERT_EQ(OB_SUCCESS, exec("create tablegroup lite_snapshot_tg sharding = 'NONE'"));
  ASSERT_EQ(OB_SUCCESS, exec("alter table lite_snapshot_db.lsnap_plain tablegroup = lite_snapshot_tg"));
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObTimeoutCtx timeout;
  ASSERT_EQ(OB_SUCCESS, timeout.set_timeout(60 * 1000 * 1000L));
  ASSERT_EQ(OB_SUCCESS, timeout.set_trx_timeout_us(60 * 1000 * 1000L));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  uint64_t tg_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, guard.get_tablegroup_id(tenant_id, ObString("lite_snapshot_tg"), tg_id));
  ASSERT_NE(OB_INVALID_ID, tg_id);
  ObSimpleTableSchemaIterator members;
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schemas_in_tablegroup(tenant_id, tg_id, members));
  ASSERT_TRUE(members.is_by_id());
  ASSERT_EQ(1, members.count());
  const ObSimpleTableSchemaV2 *member = nullptr;
  ASSERT_EQ(OB_SUCCESS, members.get_next(member));
  ASSERT_NE(nullptr, member);
  const uint64_t original_member = member->get_table_id();
  ASSERT_EQ(OB_ITER_END, members.get_next(member));
  members.reset();
  ASSERT_NE(nullptr, guard.schema_scan_trans_);
  auto *conn = static_cast<ObInnerSQLConnection *>(guard.schema_scan_trans_->get_connection());
  const SCN snapshot = conn->get_session().get_tx_desc()->get_tx_snapshot_version();
  ASSERT_TRUE(snapshot.is_valid());

  ASSERT_EQ(OB_SUCCESS, exec("alter table lite_snapshot_db.lsnap_one tablegroup = lite_snapshot_tg"));
  ObSchemaLiteScanSpec tg_spec = spec();
  tg_spec.table_spec_.kind_ = ObSchemaScanKind::BY_TABLEGROUP;
  tg_spec.table_spec_.filter_id_ = tg_id;
  ObSchemaLiteTableIterator iter;
  std::vector<uint64_t> ids;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(tg_spec, iter));
  ASSERT_EQ(OB_SUCCESS, drain_tables(iter, ids));
  EXPECT_EQ(std::vector<uint64_t>({original_member}), ids);
  EXPECT_EQ(snapshot, conn->get_session().get_tx_desc()->get_tx_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, guard.reset());

  ObSchemaGetterGuard fresh;
  ASSERT_NO_FATAL_FAILURE(init_guard(fresh));
  ASSERT_EQ(OB_SUCCESS, fresh.get_table_schemas_in_tablegroup(tenant_id, tg_id, members));
  EXPECT_TRUE(members.is_by_id());
  EXPECT_EQ(2, members.count());
  members.reset();
  ASSERT_EQ(OB_SUCCESS, fresh.scan_lite_tables(tg_spec, iter));
  ASSERT_EQ(OB_SUCCESS, drain_tables(iter, ids));
  EXPECT_EQ(2, ids.size());
  EXPECT_EQ(OB_SUCCESS, fresh.reset());
}

TEST_F(TestLiteScanStrongSnapshot, empty_scans_reset_cleanly)
{
  ASSERT_EQ(OB_SUCCESS, exec("drop table lite_snapshot_db.lsnap_plain, "
      "lite_snapshot_db.lsnap_one, lite_snapshot_db.lsnap_two"));
  ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(tenant_id));
  ObSchemaGetterGuard guard;
  ASSERT_NO_FATAL_FAILURE(init_guard(guard));
  ObSchemaLiteTableIterator iter;
  std::vector<uint64_t> ids;
  ASSERT_EQ(OB_SUCCESS, guard.scan_lite_tables(spec(), iter));
  ASSERT_EQ(OB_SUCCESS, drain_tables(iter, ids));
  EXPECT_TRUE(ids.empty());
  std::vector<JoinKey> keys;
  ASSERT_NO_FATAL_FAILURE(collect_parts(guard, ObSchemaLiteJoinSide::PART, keys));
  EXPECT_TRUE(keys.empty());
  ASSERT_NO_FATAL_FAILURE(collect_parts(guard, ObSchemaLiteJoinSide::SUBPART, keys));
  EXPECT_TRUE(keys.empty());
  EXPECT_EQ(OB_SUCCESS, guard.reset());
  EXPECT_EQ(nullptr, guard.schema_scan_trans_);
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
