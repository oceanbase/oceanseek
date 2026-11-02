/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * OnDemand-only smoke for behaviors not covered by
 * test_on_demand_vs_original_schema_scan:
 *   - get_schema_count / get_schema_size
 *   - encrypted tablespace inject visibility (inner-table SQL path)
 *   - tablegroup table-id KVCache miss→put→hit
 *   - STRONG get_table_ids_in_tenant snapshot ignores later CREATE
 */

#include <gtest/gtest.h>
#include <functional>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace
{

struct RunCtx
{
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t tg_id_ = OB_INVALID_ID;
  uint64_t tg_t1_id_ = OB_INVALID_ID;
  uint64_t tg_t2_id_ = OB_INVALID_ID;
  uint64_t enc_ts_id_ = OB_INVALID_ID;
  uint64_t enc_table_id_ = OB_INVALID_ID;
  bool encryption_injected_ = false;
};
RunCtx R;

bool contains_id(const ObIArray<uint64_t> &ids, const uint64_t id)
{
  for (int64_t i = 0; i < ids.count(); ++i) {
    if (ids.at(i) == id) {
      return true;
    }
  }
  return false;
}

bool same_id_set(const ObIArray<uint64_t> &lhs, const ObIArray<uint64_t> &rhs)
{
  if (lhs.count() != rhs.count()) {
    return false;
  }
  for (int64_t i = 0; i < lhs.count(); ++i) {
    if (!contains_id(rhs, lhs.at(i))) {
      return false;
    }
  }
  return true;
}

int exec_sql(ObMySQLProxy &proxy, const char *sql)
{
  int64_t affected = 0;
  return proxy.write(sql, affected);
}

int query_uint(ObMySQLProxy &proxy, const char *sql, const char *col, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = OB_INVALID_ID;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy.read(res, sql))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
    } else if (OB_FAIL(result->get_uint(col, value))) {
    }
  }
  return ret;
}

int get_table_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                         const char *table_name, uint64_t &table_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND table_name = '%s' LIMIT 1",
      tenant_id, table_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int get_tablegroup_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                              const char *tg_name, uint64_t &tablegroup_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT tablegroup_id FROM oceanbase.__all_virtual_tablegroup "
      "WHERE tenant_id = %lu AND tablegroup_name = '%s' LIMIT 1",
      tenant_id, tg_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "tablegroup_id", tablegroup_id);
  }
  return ret;
}

int get_tablespace_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                              const char *ts_name, uint64_t &tablespace_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT tablespace_id FROM oceanbase.__all_virtual_tenant_tablespace "
      "WHERE tenant_id = %lu AND tablespace_name = '%s' LIMIT 1",
      tenant_id, ts_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "tablespace_id", tablespace_id);
  }
  return ret;
}

int get_latest_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard);
}

const ObOnDemandSchemaGetterGuard &od()
{
  return ObOnDemandSchemaGetterGuard::instance();
}

int read_tg_publish_snapshot_(ObSchemaPublishSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  ObTenantSchemaPublishState *state = nullptr;
  int64_t schema_version = OB_INVALID_VERSION;
  snapshot.reset();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_latest_guard(R.tenant_id_, guard))) {
  } else if (OB_FAIL(guard.get_schema_version(R.tenant_id_, schema_version))) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
                 R.tenant_id_, state))) {
  } else if (OB_ISNULL(state)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(state->read_stable(snapshot))) {
  } else if (schema_version > snapshot.refreshed_version_) {
    ret = OB_SCHEMA_EAGAIN;
  }
  return ret;
}

int erase_tg_kvcache_(const uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  ObSchemaPublishSnapshot snapshot;
  if (OB_FAIL(read_tg_publish_snapshot_(snapshot))) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tablegroup_table_id_cache().erase(
                 R.tenant_id_, snapshot.cache_generations_.tablegroup_,
                 tablegroup_id))) {
  }
  return ret;
}

int warmup_tg_kvcache_(ObSchemaGetterGuard &guard, ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  ids.reset();
  if (OB_FAIL(erase_tg_kvcache_(R.tg_id_))) {
  } else if (OB_FAIL(od().get_table_ids_in_tablegroup(
                 guard, R.tenant_id_, R.tg_id_, ids))) {
  }
  return ret;
}

bool tg_kvcache_hit_(const uint64_t tablegroup_id, ObIArray<uint64_t> &ids)
{
  ObSchemaPublishSnapshot snapshot;
  ids.reset();
  return OB_SUCCESS == read_tg_publish_snapshot_(snapshot)
      && OB_SUCCESS == GCTX.schema_service_->get_tablegroup_table_id_cache().get(
             R.tenant_id_, snapshot.cache_generations_.tablegroup_, tablegroup_id, ids);
}

bool wait_until_(const int64_t timeout_us, const std::function<bool()> &pred)
{
  const int64_t start = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start < timeout_us) {
    if (pred()) {
      return true;
    }
    usleep(100 * 1000);
  }
  return pred();
}

} // namespace

class TestOnDemandSchemaScan : public ObSimpleClusterTestBase
{
public:
  TestOnDemandSchemaScan()
      : ObSimpleClusterTestBase("test_on_demand_schema_scan_", "10G", "20G")
  {}

  void SetUp() override
  {
    if (!is_started_) {
      GCONF._enable_simple_schema_on_demand = true;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_NE(nullptr, GCTX.schema_service_);
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND,
              GCTX.schema_service_->get_schema_mode());
  }
};

TEST_F(TestOnDemandSchemaScan, prepare_mysql_tenant_and_data)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("od_mysql", "3G", "3G", false /*oracle_mode*/, 2));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "od_mysql"));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);

  // The schema-store lifecycle must install publication state before the first
  // externally visible guard.  A point lookup must never be its creator.
  ASSERT_NE(nullptr, GCTX.schema_service_);
  ObTenantSchemaPublishState *publish_state = nullptr;
  ObSchemaPublishSnapshot publish_snapshot;
  ObSchemaGetterGuard first_guard;
  int64_t first_guard_version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
                R.tenant_id_, publish_state));
  ASSERT_NE(nullptr, publish_state);
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, first_guard));
  ASSERT_EQ(OB_SUCCESS,
            first_guard.get_schema_version(R.tenant_id_, first_guard_version));
  ASSERT_EQ(OB_SUCCESS, publish_state->read_stable(publish_snapshot));
  ASSERT_LE(first_guard_version, publish_snapshot.refreshed_version_);

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("od_mysql", "test"));
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "use test"));

  // Minimal tables for D1/D2 count.
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_t1(c1 int primary key, c2 int)"));

  // Tablegroup for KVCache path.
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablegroup od_tg"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_t1(c1 int primary key) tablegroup = od_tg"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_t2(c1 int primary key) tablegroup = od_tg"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_tg_out(c1 int primary key)"));

  // Encrypted tablespace inject target (no real TDE).
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablespace od_enc_ts"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_enc_t(c1 int primary key) tablespace od_enc_ts"));

  usleep(2 * 1000 * 1000);

  ASSERT_EQ(OB_SUCCESS, get_tablegroup_id_by_name(sys_proxy, R.tenant_id_, "od_tg", R.tg_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_t1", R.tg_t1_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_t2", R.tg_t2_id_));
  ASSERT_EQ(OB_SUCCESS, get_tablespace_id_by_name(sys_proxy, R.tenant_id_, "od_enc_ts", R.enc_ts_id_));
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys_proxy, R.tenant_id_, "od_enc_t", R.enc_table_id_));
  ASSERT_NE(OB_INVALID_ID, R.tg_id_);
  ASSERT_NE(OB_INVALID_ID, R.enc_ts_id_);
  ASSERT_NE(OB_INVALID_ID, R.enc_table_id_);
}

TEST_F(TestOnDemandSchemaScan, D1_get_schema_count)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> ids;
  int64_t schema_count = -1;
  int64_t stable_schema_count = -1;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tenant(guard, R.tenant_id_, ids));
  ASSERT_EQ(OB_SUCCESS, od().get_schema_count(guard, R.tenant_id_, schema_count));
  ASSERT_EQ(OB_SUCCESS,
            od().get_schema_count(guard, R.tenant_id_, stable_schema_count));
  // Count covers every schema type resident in ObSchemaMgr, while ids contains
  // table IDs only; they are intentionally different units.
  ASSERT_GT(ids.count(), 0);
  ASSERT_GT(schema_count, 0);
  ASSERT_EQ(schema_count, stable_schema_count);
}

TEST_F(TestOnDemandSchemaScan, D2_get_schema_size)
{
  ObSchemaGetterGuard guard;
  int64_t schema_count = -1;
  int64_t schema_size = -2;
  int64_t stable_schema_size = -3;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_schema_count(guard, R.tenant_id_, schema_count));
  ASSERT_EQ(OB_SUCCESS, od().get_schema_size(guard, R.tenant_id_, schema_size));
  ASSERT_EQ(OB_SUCCESS,
            od().get_schema_size(guard, R.tenant_id_, stable_schema_size));
  // Size is resident schema memory in bytes, not a schema-object count.
  ASSERT_GT(schema_size, 0);
  ASSERT_GE(schema_size, schema_count);
  ASSERT_EQ(schema_size, stable_schema_size);
}

TEST_F(TestOnDemandSchemaScan, S4_encrypted_inject_before_after)
{
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> ids_before;
  ObArray<uint64_t> ids_after;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_encrypted_table_ids_in_tenant(guard, R.tenant_id_, ids_before));
  ASSERT_FALSE(contains_id(ids_before, R.enc_table_id_));

  ObSqlString upd;
  int64_t enc_affected = 0;
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  ASSERT_EQ(OB_SUCCESS, upd.assign_fmt(
      "UPDATE %s SET encryption_name = 'aes-128' WHERE tablespace_id = %lu",
      OB_ALL_TENANT_TABLESPACE_TNAME, R.enc_ts_id_));
  ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(R.tenant_id_, upd.ptr(), enc_affected));
  ASSERT_GE(enc_affected, 1);
  R.encryption_injected_ = true;

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, od().get_encrypted_table_ids_in_tenant(guard, R.tenant_id_, ids_after));
  ASSERT_TRUE(contains_id(ids_after, R.enc_table_id_));
}

TEST_F(TestOnDemandSchemaScan, T3b_tablegroup_kvcache_miss_put_hit)
{
  // RELAXED default: fetch_tablegroup_table_ids_uncached_ uses ObTablegroupTableIdCache.
  ASSERT_NE(nullptr, GCTX.schema_service_);
  ObArray<uint64_t> cache_ids;
  ObArray<uint64_t> api_ids1;
  ObArray<uint64_t> api_ids2;
  ObSchemaGetterGuard guard;
  ObTenantSchemaPublishState *state = nullptr;
  ObSchemaPublishSnapshot token;
  int64_t schema_version = OB_INVALID_VERSION;

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_schema_version(R.tenant_id_, schema_version));
  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
                R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ASSERT_EQ(OB_SUCCESS, state->read_stable(token));
  ASSERT_LE(schema_version, token.refreshed_version_);

  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->get_tablegroup_table_id_cache().erase(
                R.tenant_id_, token.cache_generations_.tablegroup_, R.tg_id_));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            GCTX.schema_service_->get_tablegroup_table_id_cache().get(
                R.tenant_id_, token.cache_generations_.tablegroup_, R.tg_id_, cache_ids));

  // Must stay RELAXED (default); STRONG bypasses this KVCache.
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, api_ids1));
  ASSERT_TRUE(contains_id(api_ids1, R.tg_t1_id_));
  ASSERT_TRUE(contains_id(api_ids1, R.tg_t2_id_));
  ASSERT_EQ(2, api_ids1.count());

  ASSERT_EQ(OB_SUCCESS, state->read_stable(token));
  cache_ids.reset();
  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->get_tablegroup_table_id_cache().get(
                R.tenant_id_, token.cache_generations_.tablegroup_, R.tg_id_, cache_ids));
  ASSERT_TRUE(same_id_set(api_ids1, cache_ids));

  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(guard, R.tenant_id_, R.tg_id_, api_ids2));
  ASSERT_TRUE(same_id_set(api_ids1, api_ids2));
}

TEST_F(TestOnDemandSchemaScan, T3b2_operation_backlog_uses_targeted_domain_fallback)
{
  ASSERT_NE(nullptr, GCTX.schema_service_);
  ObTenantSchemaPublishState *state = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
                R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(before));

  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  operation.tenant_id_ = R.tenant_id_;
  operation.table_id_ = R.tg_t1_id_;
  operation.op_type_ = OB_DDL_ALTER_TABLE;
  operation.schema_version_ = before.refreshed_version_ + 1;
  for (int64_t i = 0;
       i <= ObSchemaCacheIncrementalRefreshPolicy::MAX_OPERATION_COUNT;
       ++i) {
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  }

  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = R.tenant_id_;
  ObSchemaMgr unused_schema_mgr;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(state));
  const ObServerSchemaService::IncrementalSchemaCacheContext context(
      schema_status, before.refreshed_version_, before.refreshed_version_ + 1,
      publication);
  ASSERT_EQ(OB_SUCCESS,
            GCTX.schema_service_->refresh_incremental_schema_cache_(
                context, get_curr_simple_server().get_sql_proxy(), operations,
                unused_schema_mgr, repair_result));
  ASSERT_TRUE(repair_result.is_valid());
  ASSERT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
  EXPECT_EQ(0U, repair_result.bump_domains_ & SCHEMA_CACHE_DOMAIN_ID_RESOLVE);
  EXPECT_NE(0U, repair_result.bump_domains_ & SCHEMA_CACHE_DOMAIN_NAME_RESOLVE);
  EXPECT_NE(0U, repair_result.bump_domains_ & SCHEMA_CACHE_DOMAIN_TABLEGROUP);

  ASSERT_TRUE(publication.is_active());
  publication.add_bump_domains(repair_result.bump_domains_);
  ASSERT_EQ(OB_SUCCESS,
            publication.finish(before.refreshed_version_, before.refreshed_version_));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(after));
  EXPECT_EQ(before.cache_generations_.id_resolve_,
            after.cache_generations_.id_resolve_);
  EXPECT_EQ(before.cache_generations_.name_resolve_ + 1,
            after.cache_generations_.name_resolve_);
  EXPECT_EQ(before.cache_generations_.tablegroup_ + 1,
            after.cache_generations_.tablegroup_);
}

TEST_F(TestOnDemandSchemaScan, T3b3_tablegroup_only_refresh_keeps_unrelated_keys)
{
  const uint64_t TEST_TENANT_ID = 500100;
  const uint64_t CHANGED_GROUP_ID = 500001;
  const uint64_t UNCHANGED_GROUP_ID = 500002;
  const ObRefreshSchemaStatus schema_status(
      TEST_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  const ObServerSchemaService::IncrementalSchemaCacheContext context(
      schema_status, 100, 120, publication);
  ObSchemaMgr unused_schema_mgr;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  ObArray<uint64_t> ids;
  ObArray<uint64_t> cached_ids;
  ObTablegroupTableIdCache *cache = nullptr;

  ASSERT_NE(nullptr, GCTX.schema_service_);
  cache = &GCTX.schema_service_->get_tablegroup_table_id_cache();
  ASSERT_EQ(OB_SUCCESS, ids.push_back(500010));
  ASSERT_EQ(OB_SUCCESS, cache->put(TEST_TENANT_ID, 1, CHANGED_GROUP_ID, ids));
  ASSERT_EQ(OB_SUCCESS, cache->put(TEST_TENANT_ID, 1, UNCHANGED_GROUP_ID, ids));
  ASSERT_EQ(OB_SUCCESS, cache->put(TEST_TENANT_ID, 2, CHANGED_GROUP_ID, ids));
  operation.tenant_id_ = TEST_TENANT_ID;
  operation.tablegroup_id_ = CHANGED_GROUP_ID;
  operation.op_type_ = OB_DDL_DEL_TABLEGROUP;
  operation.schema_version_ = 110;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));

  // With no table changes, neither END_SIGN nor an initialized schema manager
  // is needed. The tablegroup stage must still run after empty table stages.
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->refresh_incremental_schema_cache_(
      context, get_curr_simple_server().get_sql_proxy(), operations,
      unused_schema_mgr, repair_result));
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
  EXPECT_EQ(SCHEMA_CACHE_REPAIR_APPLIED, repair_result.action_);
  EXPECT_EQ(SCHEMA_CACHE_DOMAIN_NONE, repair_result.bump_domains_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache->get(TEST_TENANT_ID, 1, CHANGED_GROUP_ID, cached_ids));
  ASSERT_EQ(OB_SUCCESS, cache->get(TEST_TENANT_ID, 1, UNCHANGED_GROUP_ID, cached_ids));
  EXPECT_TRUE(same_id_set(ids, cached_ids));
  cached_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, cache->get(TEST_TENANT_ID, 2, CHANGED_GROUP_ID, cached_ids));
  EXPECT_TRUE(same_id_set(ids, cached_ids));
  EXPECT_EQ(OB_SUCCESS, cache->erase(TEST_TENANT_ID, 1, UNCHANGED_GROUP_ID));
  EXPECT_EQ(OB_SUCCESS, cache->erase(TEST_TENANT_ID, 2, CHANGED_GROUP_ID));
}

TEST_F(TestOnDemandSchemaScan, T3c_tg_kvcache_no_over_invalidate_on_alter)
{
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> warm_ids;
  ObArray<uint64_t> cache_ids;
  ObSchemaPublishSnapshot before;
  ObSchemaPublishSnapshot after;

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, warmup_tg_kvcache_(guard, warm_ids));
  ASSERT_EQ(2, warm_ids.count());
  ASSERT_EQ(OB_SUCCESS, read_tg_publish_snapshot_(before));
  ASSERT_TRUE(tg_kvcache_hit_(R.tg_id_, cache_ids));

  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "alter table od_tg_t1 add column c_tg_cache_alter int default 0"));
  ASSERT_TRUE(wait_until_(10 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    const ObTableSchema *table = nullptr;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == current_guard.get_table_schema(
               R.tenant_id_, "test", "od_tg_t1", false, table)
        && OB_NOT_NULL(table)
        && nullptr != table->get_column_schema("c_tg_cache_alter");
  })) << "schema refresh did not observe ordinary ALTER";

  ASSERT_EQ(OB_SUCCESS, read_tg_publish_snapshot_(after));
  ASSERT_EQ(before.cache_generations_.tablegroup_,
            after.cache_generations_.tablegroup_);
  ASSERT_TRUE(tg_kvcache_hit_(R.tg_id_, cache_ids));
  ASSERT_TRUE(same_id_set(warm_ids, cache_ids));
}

TEST_F(TestOnDemandSchemaScan, T3d_tg_kvcache_invalidate_on_membership_change)
{
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> warm_ids;
  ObArray<uint64_t> cache_ids;
  uint64_t t3_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, warmup_tg_kvcache_(guard, warm_ids));
  ASSERT_EQ(2, warm_ids.count());
  ASSERT_TRUE(tg_kvcache_hit_(R.tg_id_, cache_ids));

  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_t3(c1 int primary key) tablegroup = od_tg"));
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    ObArray<uint64_t> ids;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == od().get_table_ids_in_tablegroup(
               current_guard, R.tenant_id_, R.tg_id_, ids)
        && 3 == ids.count();
  })) << "schema refresh did not invalidate CREATE membership";
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(
      sys_proxy, R.tenant_id_, "od_tg_t3", t3_id));
  ASSERT_TRUE(tg_kvcache_hit_(R.tg_id_, cache_ids));
  ASSERT_EQ(3, cache_ids.count());
  ASSERT_TRUE(contains_id(cache_ids, t3_id));

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "alter table od_tg_out tablegroup = 'od_tg'"));
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    ObArray<uint64_t> ids;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == od().get_table_ids_in_tablegroup(
               current_guard, R.tenant_id_, R.tg_id_, ids)
        && 4 == ids.count();
  })) << "schema refresh did not invalidate move into tablegroup";

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "alter table od_tg_out set tablegroup = ''"));
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    ObArray<uint64_t> ids;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == od().get_table_ids_in_tablegroup(
               current_guard, R.tenant_id_, R.tg_id_, ids)
        && 3 == ids.count();
  })) << "schema refresh did not invalidate move out of tablegroup";

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "drop table od_tg_t3"));
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    ObArray<uint64_t> ids;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == od().get_table_ids_in_tablegroup(
               current_guard, R.tenant_id_, R.tg_id_, ids)
        && 2 == ids.count()
        && !contains_id(ids, t3_id);
  })) << "schema refresh did not invalidate DROP membership";
}

TEST_F(TestOnDemandSchemaScan, T3e_tg_kvcache_invalidate_on_drop_tablegroup)
{
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> cache_ids;
  ObArray<uint64_t> api_ids;
  ObSchemaPublishSnapshot snapshot;
  uint64_t empty_tg_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablegroup od_tg_empty"));
  ASSERT_TRUE(wait_until_(10 * 1000 * 1000, [&]() {
    return OB_SUCCESS == get_tablegroup_id_by_name(
               sys_proxy, R.tenant_id_, "od_tg_empty", empty_tg_id)
        && is_valid_id(empty_tg_id);
  })) << "failed to resolve empty tablegroup id";

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, erase_tg_kvcache_(empty_tg_id));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(
      guard, R.tenant_id_, empty_tg_id, api_ids));
  ASSERT_EQ(0, api_ids.count());
  ASSERT_EQ(OB_SUCCESS, read_tg_publish_snapshot_(snapshot));
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tablegroup_table_id_cache().get(
      R.tenant_id_, snapshot.cache_generations_.tablegroup_, empty_tg_id, cache_ids));
  ASSERT_EQ(0, cache_ids.count());

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "drop tablegroup od_tg_empty"));
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    uint64_t id = OB_INVALID_ID;
    return OB_SUCCESS != get_tablegroup_id_by_name(
               sys_proxy, R.tenant_id_, "od_tg_empty", id)
        || !is_valid_id(id);
  })) << "drop tablegroup not refreshed";

  cache_ids.reset();
  ASSERT_EQ(OB_SUCCESS, read_tg_publish_snapshot_(snapshot));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            GCTX.schema_service_->get_tablegroup_table_id_cache().get(
                R.tenant_id_, snapshot.cache_generations_.tablegroup_,
                empty_tg_id, cache_ids));
}

TEST_F(TestOnDemandSchemaScan, T3f_tablegroup_excludes_aux_after_alter)
{
  // ALTER TABLE ... tablegroup copies tablegroup_id onto AUX_LOB / local index.
  // The tablegroup lookup must only expose user/tmp/sys tables, otherwise
  // partition balance may try to lock an AUX_LOB table and hit OB_OP_NOT_ALLOW.
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> api_ids;
  ObArray<uint64_t> cache_ids;
  ObSchemaPublishSnapshot snapshot;
  ObSqlString sql;
  uint64_t tablegroup_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t aux_id = OB_INVALID_ID;

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create tablegroup od_tg_aux"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table od_tg_lob(c1 int primary key, c2 text)"));
  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "alter table od_tg_lob tablegroup = 'od_tg_aux'"));

  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    return OB_SUCCESS == get_tablegroup_id_by_name(
               sys_proxy, R.tenant_id_, "od_tg_aux", tablegroup_id)
        && is_valid_id(tablegroup_id);
  })) << "failed to resolve od_tg_aux";
  ASSERT_TRUE(wait_until_(15 * 1000 * 1000, [&]() {
    ObSchemaGetterGuard current_guard;
    ObArray<uint64_t> ids;
    return OB_SUCCESS == get_latest_guard(R.tenant_id_, current_guard)
        && OB_SUCCESS == od().get_table_ids_in_tablegroup(
               current_guard, R.tenant_id_, tablegroup_id, ids)
        && 1 == ids.count();
  })) << "schema refresh did not observe ALTER into tablegroup";

  ASSERT_EQ(OB_SUCCESS,
            get_table_id_by_name(sys_proxy, R.tenant_id_, "od_tg_lob", table_id));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND tablegroup_id = %lu "
      "AND table_type IN (12, 13) LIMIT 1",
      R.tenant_id_, tablegroup_id));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys_proxy, sql.ptr(), "table_id", aux_id));
  ASSERT_TRUE(is_valid_id(aux_id));
  ASSERT_NE(aux_id, table_id);

  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, erase_tg_kvcache_(tablegroup_id));
  ASSERT_EQ(OB_SUCCESS, od().get_table_ids_in_tablegroup(
      guard, R.tenant_id_, tablegroup_id, api_ids));
  ASSERT_EQ(1, api_ids.count());
  ASSERT_TRUE(contains_id(api_ids, table_id));
  ASSERT_FALSE(contains_id(api_ids, aux_id));

  ASSERT_EQ(OB_SUCCESS, read_tg_publish_snapshot_(snapshot));
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tablegroup_table_id_cache().get(
      R.tenant_id_, snapshot.cache_generations_.tablegroup_, tablegroup_id, cache_ids));
  ASSERT_TRUE(same_id_set(api_ids, cache_ids));
}

TEST_F(TestOnDemandSchemaScan, strong_get_table_ids_in_tenant_ignores_later_create)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(R.tenant_id_));

  ObSchemaGetterGuard guard;
  ObArray<uint64_t> before_ids;
  ObArray<uint64_t> after_ids;
  ObArray<uint64_t> fresh_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS, guard.get_table_ids_in_tenant(R.tenant_id_, before_ids));
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "drop table if exists od_strong_new"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_strong_new(id int primary key)"));
  ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
  uint64_t new_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, get_table_id_by_name(sys, R.tenant_id_, "od_strong_new", new_id));
  ASSERT_NE(OB_INVALID_ID, new_id);
  ASSERT_FALSE(contains_id(before_ids, new_id));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_ids_in_tenant(R.tenant_id_, after_ids));
  ASSERT_FALSE(contains_id(after_ids, new_id))
      << "STRONG snapshot must not pick up a table created after first scan";
  ObSchemaGetterGuard fresh;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, fresh));
  fresh.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS, fresh.get_table_ids_in_tenant(R.tenant_id_, fresh_ids));
  ASSERT_TRUE(contains_id(fresh_ids, new_id));
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
