/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * OnDemand Guard pin reuse / pin lifetime / miss×rename-before-publish.
 */

#include <atomic>
#include <stdio.h>
#include <thread>
#include <unistd.h>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_simple_schema_kv_cache.h"
#include "share/ob_debug_sync.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"

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
  uint64_t database_id_ = OB_INVALID_ID;
  uint64_t table_id_ = OB_INVALID_ID;
};
RunCtx R;

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

int get_latest_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard);
}

int get_name_case_mode(const uint64_t tenant_id, ObNameCaseMode &name_case_mode)
{
  ObSchemaGetterGuard guard;
  int ret = get_latest_guard(tenant_id, guard);
  if (OB_SUCC(ret)) {
    ret = guard.get_tenant_name_case_mode(tenant_id, name_case_mode);
  }
  return ret;
}

int count_pinned(const ObSchemaGetterGuard &guard, const uint64_t table_id)
{
  int64_t n = 0;
  for (int64_t i = 0; i < guard.schema_objs_.count(); ++i) {
    if (table_id == guard.schema_objs_.at(i).schema_id_) {
      ++n;
    }
  }
  return static_cast<int>(n);
}

int erase_name_resolve(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const char *table_name)
{
  int ret = OB_SUCCESS;
  ObTenantSchemaPublishState *state = NULL;
  ObSchemaPublishSnapshot snap;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
          tenant_id, state))) {
  } else if (OB_ISNULL(state)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(state->read_stable(snap))) {
  } else if (OB_FAIL(get_name_case_mode(tenant_id, name_case_mode))) {
  } else {
    ObSchemaNameResolveKey key(tenant_id, snap.cache_generations_.name_resolve_, database_id,
                               0 /*session*/, NS_NORMAL_TABLE, name_case_mode,
                               ObString::make_string(table_name));
    ret = GCTX.schema_service_->get_schema_resolve_cache().erase(key);
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool name_resolve_load_in_flight_(const ObSchemaLoadKey &key)
{
  uint64_t flight_id = 0;
  ObSchemaLoadCoordinator &coord =
      GCTX.schema_service_->get_simple_schema_load_coordinator();
  ObSpinLockGuard lock_guard(coord.registry_lock_);
  return OB_SUCCESS == coord.loading_flights_.get_refactored(key, flight_id);
}

enum class ResolveFollowerCase
{
  EXIST,
  NOT_EXIST,
  NO_CACHE,
  OTHER_SNAPSHOT,
  STALE_TOKEN,
  EXPIRED_DEADLINE,
  WAIT_TIMEOUT
};

void check_resolve_follower_(const bool by_name,
                             const ResolveFollowerCase test_case,
                             const uint64_t session_id = 0)
{
  SCOPED_TRACE(by_name ? "name follower" : "id follower");
  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  int64_t snapshot_version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, guard.get_schema_version(R.tenant_id_, snapshot_version));
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  ASSERT_EQ(OB_SUCCESS, guard.get_tenant_name_case_mode(R.tenant_id_, name_case_mode));

  // Isolate synthetic resolve ranges from the live tenant's cache generations.
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(R.tenant_id_, snapshot_version, 1ULL << 40));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  const ObString name = ObString::make_string("od_follower_probe");
  const ObSchemaIDResolveKey id_key(
      R.tenant_id_, token.cache_generations_.id_resolve_, R.table_id_);
  const ObSchemaNameResolveKey name_key(
      R.tenant_id_, token.cache_generations_.name_resolve_, R.database_id_,
      session_id, NS_NORMAL_TABLE, name_case_mode, name);
  ObSchemaResolveCache &cache = GCTX.schema_service_->get_schema_resolve_cache();
  ASSERT_EQ(OB_SUCCESS, by_name ? cache.erase(name_key) : cache.erase(id_key));
  const ObSchemaLoadKey load_key(
      by_name ? LOAD_NAME_RESOLVE : LOAD_ID_RESOLVE, R.tenant_id_,
      by_name ? token.cache_generations_.name_resolve_ : token.cache_generations_.id_resolve_,
      by_name ? name_key.hash() : id_key.hash());
  ObSchemaLoadGuard leader;
  bool is_leader = false;
  uint64_t flight_id = 0;
  ASSERT_EQ(OB_SUCCESS, leader.acquire(
      GCTX.schema_service_->get_simple_schema_load_coordinator(),
      load_key, is_leader, flight_id));
  ASSERT_TRUE(is_leader);

  ObArenaAllocator sync_allocator("ResolveFollow");
  ObDSSessionActions point_actions;
  ObDSSessionActions control_actions;
  ASSERT_EQ(OB_SUCCESS, point_actions.init(1024, sync_allocator));
  ASSERT_EQ(OB_SUCCESS, control_actions.init(1024, sync_allocator));
  std::thread follower;
  ObSchemaResolveRange actual(snapshot_version, snapshot_version, R.table_id_,
                               snapshot_version, 0, NS_NORMAL_TABLE, true);
  std::atomic<int> follower_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> follower_done(false);
  std::atomic<bool> follower_hit_sync_point(false);
  struct Cleanup
  {
    ObSchemaLoadGuard &leader_;
    ObDSSessionActions &actions_;
    std::thread &follower_;
    const int64_t old_debug_timeout_;
    ~Cleanup()
    {
      leader_.reset();
      (void)GDS.add_debug_sync("now signal od_follower_go", false, actions_);
      if (follower_.joinable()) {
        follower_.join();
      }
      (void)GDS.add_debug_sync("reset", false, actions_);
      char timeout[64] = {};
      snprintf(timeout, sizeof(timeout), "%ldus", old_debug_timeout_);
      GCONF.debug_sync_timeout.set_value(timeout);
    }
  } cleanup{leader, control_actions, follower, GCONF.debug_sync_timeout};
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "BEFORE_ON_DEMAND_RESOLVE_FOLLOWER_WAIT signal od_follower_ready "
      "wait_for od_follower_go timeout 10000000 execute 1",
      false, point_actions));

  const bool timeout_case = ResolveFollowerCase::EXPIRED_DEADLINE == test_case
      || ResolveFollowerCase::WAIT_TIMEOUT == test_case;
  const int64_t deadline = ObTimeUtility::current_time()
      + (timeout_case ? 2L : 10L) * 1000L * 1000L;
  follower = std::thread([&, deadline]() {
    int ret = GDS.set_thread_local_actions(point_actions);
    auto &coordinator = GCTX.schema_service_->simple_schema_on_demand_coordinator_;
    if (OB_SUCCESS == ret) {
      ret = by_name
          ? coordinator.get_version_range_by_name_with_session_id_(
                guard, name_key, name, snapshot_version, OB_LOWERCASE_AND_INSENSITIVE,
                deadline, state, token, actual)
          : coordinator.get_version_range_by_id_once_(
                guard, R.tenant_id_, R.table_id_, snapshot_version,
                deadline, state, token, actual);
    }
    ObDSActionArray *actions = GDS.thread_local_actions();
    follower_hit_sync_point.store(NULL != actions
        && !actions->is_active(BEFORE_ON_DEMAND_RESOLVE_FOLLOWER_WAIT));
    follower_ret.store(ret);
    follower_done.store(true);
  });
  // This handshake proves the initial lookup missed and joined the existing
  // flight. Publishing earlier could accidentally test only the first cache hit.
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "now wait_for od_follower_ready timeout 10000000", false, control_actions));
  ASSERT_FALSE(follower_done.load());

  const bool negative = ResolveFollowerCase::NOT_EXIST == test_case;
  const bool other_snapshot = ResolveFollowerCase::OTHER_SNAPSHOT == test_case;
  const ObSchemaResolveRange published(
      snapshot_version - 2, other_snapshot ? snapshot_version - 1 : snapshot_version,
      negative ? OB_INVALID_ID : R.table_id_,
      negative ? OB_INVALID_VERSION : snapshot_version - 2,
      0, by_name ? NS_NORMAL_TABLE : share::schema::NS_MAX, !negative);
  if (ResolveFollowerCase::NO_CACHE != test_case
      && ResolveFollowerCase::WAIT_TIMEOUT != test_case) {
    ASSERT_EQ(OB_SUCCESS, by_name
        ? cache.publish_if_current(name_key, published, state, token)
        : cache.publish_if_current(id_key, published, state, token));
  }
  if (ResolveFollowerCase::STALE_TOKEN == test_case) {
    ObTenantSchemaPublicationGuard publication;
    ASSERT_EQ(OB_SUCCESS, publication.begin(state));
    ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
    ASSERT_EQ(OB_SUCCESS, publication.commit(snapshot_version));
  }
  if (ResolveFollowerCase::EXPIRED_DEADLINE == test_case) {
    // wait() can observe an already finished flight even after its deadline.
    // The cache read following that successful wait must still return timeout.
    while (ObTimeUtility::current_time() < deadline) {
      usleep(1000);
    }
  }
  if (ResolveFollowerCase::WAIT_TIMEOUT != test_case) {
    leader.reset();
  }
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync(
      "now signal od_follower_go", false, control_actions));
  follower.join();
  ASSERT_EQ(OB_SUCCESS, by_name ? cache.erase(name_key) : cache.erase(id_key));
  ASSERT_TRUE(follower_hit_sync_point.load());

  if (ResolveFollowerCase::EXIST == test_case || negative) {
    // Assert the single resolve attempt itself succeeds: the old implementation
    // returned OB_SCHEMA_EAGAIN here and forced the caller's 50us backoff.
    ASSERT_EQ(OB_SUCCESS, follower_ret.load());
    ASSERT_TRUE(actual.is_valid());
    ASSERT_EQ(published.start_version_, actual.start_version_);
    ASSERT_EQ(published.end_version_, actual.end_version_);
    ASSERT_EQ(published.schema_id_, actual.schema_id_);
    ASSERT_EQ(published.precise_schema_version_, actual.precise_schema_version_);
    ASSERT_EQ(published.table_namespace_, actual.table_namespace_);
    ASSERT_EQ(published.is_exist_, actual.is_exist_);
  } else {
    ASSERT_EQ(timeout_case ? OB_TIMEOUT : OB_SCHEMA_EAGAIN, follower_ret.load());
    ASSERT_FALSE(actual.is_valid());
    ASSERT_FALSE(actual.is_exist_);
    ASSERT_EQ(OB_INVALID_ID, actual.schema_id_);
    ASSERT_EQ(OB_INVALID_VERSION, actual.precise_schema_version_);
  }
}

} // namespace

class TestOnDemandSchemaGuardCache : public ObSimpleClusterTestBase
{
public:
  TestOnDemandSchemaGuardCache()
      : ObSimpleClusterTestBase("test_on_demand_schema_guard_cache_", "10G", "20G")
  {}
};

TEST_F(TestOnDemandSchemaGuardCache, prepare_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("od_guard_mysql", "3G", "3G", false, 2));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "od_guard_mysql"));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("od_guard_mysql", "test"));
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_pin(id int primary key, v int)"));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id from oceanbase.__all_virtual_table "
      "where tenant_id = %lu and table_name = 'od_pin' limit 1", R.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "table_id", R.table_id_));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select database_id from oceanbase.__all_virtual_table "
      "where tenant_id = %lu and table_id = %lu", R.tenant_id_, R.table_id_));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "database_id", R.database_id_));
  ASSERT_NE(OB_INVALID_ID, R.table_id_);
  ASSERT_NE(OB_INVALID_ID, R.database_id_);
}

TEST_F(TestOnDemandSchemaGuardCache, pin_reuse_same_guard)
{
  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *s1 = NULL;
  const ObSimpleTableSchemaV2 *s2 = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(R.tenant_id_, R.table_id_, s1));
  ASSERT_NE(nullptr, s1);
  const int64_t after_first = guard.schema_objs_.count();
  ASSERT_GE(count_pinned(guard, R.table_id_), 1);
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(R.tenant_id_, R.table_id_, s2));
  ASSERT_EQ(s1, s2);
  ASSERT_EQ(after_first, guard.schema_objs_.count());
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_returns_positive_range_without_retry)
{
  check_resolve_follower_(false, ResolveFollowerCase::EXIST);
  check_resolve_follower_(true, ResolveFollowerCase::EXIST);
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_returns_negative_range_without_retry)
{
  check_resolve_follower_(false, ResolveFollowerCase::NOT_EXIST);
  check_resolve_follower_(true, ResolveFollowerCase::NOT_EXIST);
  check_resolve_follower_(true, ResolveFollowerCase::NOT_EXIST, 77 /*private session*/);
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_without_matching_range_retries)
{
  for (const bool by_name : {false, true}) {
    check_resolve_follower_(by_name, ResolveFollowerCase::NO_CACHE);
    check_resolve_follower_(by_name, ResolveFollowerCase::OTHER_SNAPSHOT);
  }
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_rejects_stale_publication)
{
  check_resolve_follower_(false, ResolveFollowerCase::STALE_TOKEN);
  check_resolve_follower_(true, ResolveFollowerCase::STALE_TOKEN);
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_checks_deadline_after_completed_flight)
{
  check_resolve_follower_(false, ResolveFollowerCase::EXPIRED_DEADLINE);
  check_resolve_follower_(true, ResolveFollowerCase::EXPIRED_DEADLINE);
}

TEST_F(TestOnDemandSchemaGuardCache, resolve_follower_preserves_wait_timeout)
{
  check_resolve_follower_(false, ResolveFollowerCase::WAIT_TIMEOUT);
  check_resolve_follower_(true, ResolveFollowerCase::WAIT_TIMEOUT);
}

TEST_F(TestOnDemandSchemaGuardCache, pin_reuse_after_metadata_publication_and_cache_erase)
{
  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *first = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(
      R.tenant_id_, R.table_id_, first));
  ASSERT_NE(nullptr, first);
  const int64_t precise_version = first->get_schema_version();
  const int64_t pinned_count = guard.schema_objs_.count();
  const int table_pin_count = count_pinned(guard, R.table_id_);
  const int64_t pin_cache_size = guard.pin_cache_size_;
  ASSERT_EQ(1, table_pin_count);
  ASSERT_GT(pin_cache_size, 0);

  const ObSimpleSchemaCacheKey key(R.tenant_id_, R.table_id_, precise_version);
  ObSimpleSchemaObjectCache &cache = GCTX.schema_service_->get_simple_schema_object_cache();
  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, cache.get(key, cached, handle));
  ASSERT_EQ(first, cached);
  ASSERT_TRUE(handle.is_valid());
  handle.reset();

  ObTenantSchemaPublishState *state = NULL;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
      R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(*state));
  const ObSchemaPublishSnapshot before = publication.before();
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  // Finish publication before the public lookup re-resolves this Guard's
  // snapshot. Keep the frontier unchanged: only metadata generations change.
  ASSERT_EQ(OB_SUCCESS, publication.commit(before.refreshed_version_));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(after));
  ASSERT_GT(after.cache_generations_.id_resolve_, before.cache_generations_.id_resolve_);
  ASSERT_GT(after.cache_generations_.name_resolve_, before.cache_generations_.name_resolve_);
  ASSERT_GT(after.cache_generations_.tablegroup_, before.cache_generations_.tablegroup_);

  ASSERT_EQ(OB_SUCCESS, cache.erase(key));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, cache.get(key, cached, handle));
  ASSERT_EQ(nullptr, cached);
  ASSERT_FALSE(handle.is_valid());

  // Re-resolving the ID under the new metadata generation must reuse the
  // pre-publication pin without loading or pinning another simple object.
  const ObSimpleTableSchemaV2 *again = NULL;
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(
      R.tenant_id_, R.table_id_, again));
  ASSERT_EQ(first, again);
  ASSERT_EQ(precise_version, again->get_schema_version());
  ASSERT_EQ(pinned_count, guard.schema_objs_.count());
  ASSERT_EQ(table_pin_count, count_pinned(guard, R.table_id_));
  ASSERT_EQ(pin_cache_size, guard.pin_cache_size_);
}

TEST_F(TestOnDemandSchemaGuardCache, exact_object_load_ignores_metadata_publication)
{
  ObSchemaGetterGuard warm_guard;
  const ObSimpleTableSchemaV2 *warm_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, warm_guard));
  ASSERT_EQ(OB_SUCCESS, warm_guard.get_simple_table_schema(
      R.tenant_id_, R.table_id_, warm_schema));
  ASSERT_NE(nullptr, warm_schema);
  const int64_t precise_version = warm_schema->get_schema_version();
  const ObSimpleSchemaCacheKey key(R.tenant_id_, R.table_id_, precise_version);
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_simple_schema_object_cache().erase(key));

  ObSchemaGetterGuard load_guard;
  ObSchemaGetterGuard hit_guard;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, load_guard));
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, hit_guard));
  ObTenantSchemaPublishState *state = NULL;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
      R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(*state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());

  // ID resolution has already selected a precise version. The object stage
  // must finish its SQL load and pin even while all metadata caches refresh.
  const ObSimpleTableSchemaV2 *loaded = NULL;
  auto &coordinator = GCTX.schema_service_->simple_schema_on_demand_coordinator_;
  ASSERT_EQ(OB_SUCCESS, coordinator.load_simple_schema_from_cache_or_sql_(
      load_guard, R.tenant_id_, R.table_id_, precise_version,
      ObTimeUtility::current_time() + 10L * 1000L * 1000L, loaded));
  ASSERT_NE(nullptr, loaded);
  ASSERT_EQ(precise_version, loaded->get_schema_version());
  ASSERT_STREQ("od_pin", loaded->get_table_name());

  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_simple_schema_object_cache().get(
      key, cached, handle));
  ASSERT_EQ(loaded, cached);
  ASSERT_EQ(OB_SUCCESS, coordinator.load_simple_schema_from_cache_or_sql_(
      hit_guard, R.tenant_id_, R.table_id_, precise_version,
      ObTimeUtility::current_time() + 10L * 1000L * 1000L, cached));
  ASSERT_EQ(loaded, cached);
  ASSERT_EQ(OB_SUCCESS, hit_guard.reset());
  const int64_t pinned_count = load_guard.schema_objs_.count();
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_simple_schema_object_cache().erase(key));
  handle.reset();
  const ObSimpleTableSchemaV2 *again = NULL;
  ASSERT_EQ(OB_SUCCESS, coordinator.load_simple_schema_from_cache_or_sql_(
      load_guard, R.tenant_id_, R.table_id_, precise_version,
      ObTimeUtility::current_time() + 10L * 1000L * 1000L, again));
  ASSERT_EQ(loaded, again);
  ASSERT_EQ(pinned_count, load_guard.schema_objs_.count());

  // A pin for the same table must not satisfy a different precise version.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, coordinator.get_local_simple_schema_(
      load_guard, R.tenant_id_, R.table_id_, precise_version + 1, again));
  ASSERT_EQ(nullptr, again);
  ASSERT_EQ(OB_SUCCESS, publication.commit(publication.before().refreshed_version_));
}

TEST_F(TestOnDemandSchemaGuardCache, pin_survives_other_guard_reset)
{
  ObSchemaGetterGuard *a = new ObSchemaGetterGuard();
  ObSchemaGetterGuard b;
  const ObSimpleTableSchemaV2 *sa = NULL;
  const ObSimpleTableSchemaV2 *sb = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, *a));
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, b));
  ASSERT_EQ(OB_SUCCESS, a->get_simple_table_schema(R.tenant_id_, R.table_id_, sa));
  ASSERT_EQ(OB_SUCCESS, b.get_simple_table_schema(R.tenant_id_, R.table_id_, sb));
  ASSERT_NE(nullptr, sa);
  ASSERT_NE(nullptr, sb);
  delete a;
  ASSERT_STREQ("od_pin", sb->get_table_name());
  ASSERT_EQ(R.table_id_, sb->get_table_id());
}

TEST_F(TestOnDemandSchemaGuardCache, full_schema_lookup_exists_and_missing)
{
  ObSchemaGetterGuard guard;
  const ObTableSchema *table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(R.tenant_id_, R.table_id_, table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(R.table_id_, table_schema->get_table_id());
  ASSERT_EQ(1, count_pinned(guard, R.table_id_));

  ObTenantSchemaPublishState *state = NULL;
  ObSchemaPublishSnapshot snap;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
      R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ASSERT_EQ(OB_SUCCESS, state->read_stable(snap));
  const ObSchemaIDResolveKey id_key(
      R.tenant_id_, snap.cache_generations_.id_resolve_, R.table_id_);
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_schema_resolve_cache().erase(id_key));
  ObSchemaResolveResult cached;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, GCTX.schema_service_->get_schema_resolve_cache().get(
      id_key, snap.refreshed_version_, cached));

  const ObTableSchema *table_schema_again = NULL;
  const int64_t pinned_count = guard.schema_objs_.count();
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
      R.tenant_id_, R.table_id_, table_schema_again));
  ASSERT_EQ(table_schema, table_schema_again);
  ASSERT_EQ(pinned_count, guard.schema_objs_.count());
  ASSERT_TRUE(state->can_publish(snap));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, GCTX.schema_service_->get_schema_resolve_cache().get(
      id_key, snap.refreshed_version_, cached));

  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_gone(id int primary key)"));
  uint64_t dropped_table_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id from oceanbase.__all_virtual_table "
      "where tenant_id = %lu and table_name = 'od_gone' limit 1", R.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "table_id", dropped_table_id));
  ASSERT_NE(OB_INVALID_ID, dropped_table_id);
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "drop table od_gone"));

  ObSchemaGetterGuard dropped_guard;
  const ObTableSchema *dropped_schema = NULL;
  ObArray<ObAuxTableMetaInfo> index_meta_infos;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, dropped_guard));
  ASSERT_EQ(OB_SUCCESS, dropped_guard.get_table_schema(
      R.tenant_id_, dropped_table_id, dropped_schema));
  ASSERT_EQ(nullptr, dropped_schema);
  ASSERT_EQ(OB_TABLE_NOT_EXIST, dropped_guard.get_index_meta_infos_with_data_table_id(
      R.tenant_id_, dropped_table_id, index_meta_infos));
  ASSERT_TRUE(index_meta_infos.empty());
}

TEST_F(TestOnDemandSchemaGuardCache, full_schema_fast_path_enforces_tenant_guard)
{
  ObSchemaGetterGuard guard;
  const ObTableSchema *table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
      R.tenant_id_, R.table_id_, table_schema));
  ASSERT_NE(nullptr, table_schema);

  const uint64_t other_tenant_id = R.tenant_id_ + 1000;
  SchemaObj foreign_obj;
  foreign_obj.schema_type_ = TABLE_SCHEMA;
  foreign_obj.tenant_id_ = other_tenant_id;
  foreign_obj.schema_id_ = R.table_id_;
  foreign_obj.schema_ = const_cast<ObTableSchema *>(table_schema);
  ASSERT_EQ(OB_SUCCESS, guard.schema_objs_.push_back(foreign_obj));

  const ObTableSchema *foreign_schema = NULL;
  ASSERT_EQ(OB_OP_NOT_ALLOW, guard.get_table_schema(
      other_tenant_id, R.table_id_, foreign_schema));
  ASSERT_EQ(nullptr, foreign_schema);

  const ObTableSchema *invalid_schema = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT, guard.get_table_schema(
      OB_INVALID_TENANT_ID, R.table_id_, invalid_schema));
  ASSERT_EQ(nullptr, invalid_schema);
  ASSERT_EQ(OB_INVALID_ARGUMENT, guard.get_table_schema(
      R.tenant_id_, OB_INVALID_ID, invalid_schema));
  ASSERT_EQ(nullptr, invalid_schema);
}

TEST_F(TestOnDemandSchemaGuardCache, historical_guard_sees_precise_version_before_end_sign)
{
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(
      proxy2, "create table od_hist(id int primary key, c1 int)"));

  uint64_t table_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id from oceanbase.__all_virtual_table "
      "where tenant_id = %lu and table_name = 'od_hist' limit 1", R.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "table_id", table_id));
  ASSERT_NE(OB_INVALID_ID, table_id);

  // Prewarm the ID resolve key so the following DDL exercises incremental
  // cache repair instead of a history-table cache miss.
  ObSchemaGetterGuard warm_guard;
  const ObTableSchema *warm_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, warm_guard));
  ASSERT_EQ(OB_SUCCESS, warm_guard.get_table_schema(
      R.tenant_id_, table_id, warm_schema));
  ASSERT_NE(nullptr, warm_schema);

  ObTenantSchemaPublishState *state = NULL;
  ObSchemaPublishSnapshot before_publish;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
      R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ASSERT_EQ(OB_SUCCESS, state->read_stable(before_publish));

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "alter table od_hist add column c2 int"));

  uint64_t precise_version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select max(schema_version) as schema_version "
      "from oceanbase.__all_table_history "
      "where tenant_id = 0 and table_id = %lu and is_deleted = 0", table_id));
  ASSERT_EQ(OB_SUCCESS, query_uint(
      proxy2, sql.ptr(), "schema_version", precise_version));
  ASSERT_NE(OB_INVALID_VERSION, precise_version);

  uint64_t end_sign_version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select min(schema_version) as schema_version "
      "from oceanbase.__all_ddl_operation "
      "where tenant_id = 0 and operation_type = %ld and schema_version > %lu",
      static_cast<int64_t>(OB_DDL_END_SIGN), precise_version));
  ASSERT_EQ(OB_SUCCESS, query_uint(
      proxy2, sql.ptr(), "schema_version", end_sign_version));
  ASSERT_GT(end_sign_version, precise_version);

  ObSchemaPublishSnapshot after_publish;
  const int64_t wait_deadline = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
  do {
    ASSERT_EQ(OB_SUCCESS, state->read_stable(after_publish));
    if (after_publish.refreshed_version_ >= static_cast<int64_t>(end_sign_version)) {
      break;
    }
    usleep(50L * 1000L);
  } while (ObTimeUtility::current_time() < wait_deadline);
  ASSERT_GE(after_publish.refreshed_version_, static_cast<int64_t>(end_sign_version));
  ASSERT_EQ(before_publish.cache_generations_.id_resolve_,
            after_publish.cache_generations_.id_resolve_);
  ASSERT_EQ(before_publish.cache_generations_.name_resolve_,
            after_publish.cache_generations_.name_resolve_);

  ObSchemaGetterGuard historical_guard;
  const ObTableSchema *historical_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(
      R.tenant_id_, historical_guard, precise_version));
  ASSERT_EQ(OB_SUCCESS, historical_guard.get_table_schema(
      R.tenant_id_, table_id, historical_schema));
  ASSERT_NE(nullptr, historical_schema);
  ASSERT_EQ(precise_version,
            static_cast<uint64_t>(historical_schema->get_schema_version()));
  ASSERT_NE(nullptr, historical_schema->get_column_schema(
      ObString::make_string("c2")));
}

TEST_F(TestOnDemandSchemaGuardCache, miss_then_rename_before_publish)
{
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "create table od_race(id int primary key)"));
  uint64_t race_table_id = OB_INVALID_ID;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "select table_id from oceanbase.__all_virtual_table "
      "where tenant_id = %lu and table_name = 'od_race' limit 1", R.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "table_id", race_table_id));
  ASSERT_NE(OB_INVALID_ID, race_table_id);
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  ASSERT_EQ(OB_SUCCESS, get_name_case_mode(R.tenant_id_, name_case_mode));
  ASSERT_EQ(OB_SUCCESS, erase_name_resolve(R.tenant_id_, R.database_id_, "od_race"));

  ObTenantSchemaPublishState *state = NULL;
  ObSchemaPublishSnapshot pre_snap;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
      R.tenant_id_, state));
  ASSERT_NE(nullptr, state);
  ASSERT_EQ(OB_SUCCESS, state->read_stable(pre_snap));
  const ObSchemaNameResolveKey pre_name_key(
      R.tenant_id_, pre_snap.cache_generations_.name_resolve_, R.database_id_,
      0 /*session*/, NS_NORMAL_TABLE, name_case_mode,
      ObString::make_string("od_race"));
  const ObSchemaLoadKey load_key(
      LOAD_NAME_RESOLVE, R.tenant_id_, pre_snap.cache_generations_.name_resolve_,
      pre_name_key.hash());

  ASSERT_EQ(OB_SUCCESS, exec_sql(sys, "alter system set debug_sync_timeout = '30s'"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(sys,
      "set ob_global_debug_sync = "
      "'AFTER_ON_DEMAND_RESOLVE_BEFORE_PUBLISH wait_for miss_go execute 1'"));

  struct RaceLookup
  {
    std::atomic<int> ret{OB_SUCCESS};
    std::atomic<uint64_t> table_id{OB_INVALID_ID};
    char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {};
    std::atomic<bool> done{false};
  } race;

  std::thread miss_th([&]() {
    ObSchemaGetterGuard guard;
    const ObSimpleTableSchemaV2 *schema = NULL;
    int ret = get_latest_guard(R.tenant_id_, guard);
    if (OB_SUCC(ret)) {
      ret = guard.get_simple_table_schema(R.tenant_id_, R.database_id_,
                                          ObString::make_string("od_race"),
                                          false, schema);
    }
    race.ret = ret;
    if (OB_SUCCESS == ret && NULL != schema) {
      race.table_id = schema->get_table_id();
      snprintf(race.table_name, sizeof(race.table_name), "%s", schema->get_table_name());
    }
    race.done = true;
  });
  struct Cleanup
  {
    ObMySQLProxy &sys_;
    std::thread &th_;
    ~Cleanup()
    {
      int64_t affected = 0;
      (void)sys_.write("set ob_global_debug_sync = 'now signal miss_go'", affected);
      (void)sys_.write("set ob_global_debug_sync = 'reset'", affected);
      (void)sys_.write("alter system set debug_sync_timeout = 0", affected);
      if (th_.joinable()) {
        th_.join();
      }
    }
  } cleanup{sys, miss_th};

  bool hit_before_publish = false;
  const int64_t wait_beg = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - wait_beg < 15 * 1000 * 1000) {
    if (race.done.load()) {
      break;
    }
    if (name_resolve_load_in_flight_(load_key)) {
      hit_before_publish = true;
      break;
    }
    usleep(50 * 1000);
  }
  ASSERT_TRUE(hit_before_publish)
      << "lookup did not stop at AFTER_ON_DEMAND_RESOLVE_BEFORE_PUBLISH";
  ASSERT_FALSE(race.done.load());

  // SQL RENAME resolves od_race by name. The miss thread is the LOAD_NAME_RESOLVE
  // leader and is parked at debug_sync, so a name-cache miss would wait on the
  // same flight and hit OB_TIMEOUT (-4012). Mittest is one observer+RS process;
  // a remote RS would use its own coordinator. Load by table_id (different
  // flight) and publish exist so RENAME does not join the blocked flight.
  {
    ObSchemaGetterGuard id_guard;
    const ObSimpleTableSchemaV2 *id_schema = NULL;
    int64_t snapshot_version = OB_INVALID_VERSION;
    ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, id_guard));
    ASSERT_EQ(OB_SUCCESS, id_guard.get_simple_table_schema(
        R.tenant_id_, race_table_id, id_schema));
    ASSERT_NE(nullptr, id_schema);
    ASSERT_EQ(OB_SUCCESS, id_guard.get_schema_version(R.tenant_id_, snapshot_version));
    const ObSchemaResolveRange exist_range(
        id_schema->get_schema_version(), snapshot_version, race_table_id,
        id_schema->get_schema_version(), 0, NS_NORMAL_TABLE, true);
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_schema_resolve_cache().
        publish_if_current(pre_name_key, exist_range, *state, pre_snap));
  }

  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "rename table od_race to od_race_new"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(sys, "set ob_global_debug_sync = 'now signal miss_go'"));
  miss_th.join();

  ASSERT_EQ(OB_SUCCESS, race.ret.load());
  ASSERT_NE(OB_INVALID_ID, race.table_id.load());
  ASSERT_STREQ("od_race", race.table_name);

  ObSchemaPublishSnapshot post_snap;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(post_snap));
  ASSERT_FALSE(state->can_publish(pre_snap));
  ObSchemaNameResolveKey cur_name_key(
      R.tenant_id_, post_snap.cache_generations_.name_resolve_, R.database_id_,
      0 /*session*/, NS_NORMAL_TABLE, name_case_mode,
      ObString::make_string("od_race"));
  ObSchemaResolveResult cached;
  const int cache_ret = GCTX.schema_service_->get_schema_resolve_cache().get(
      cur_name_key, post_snap.refreshed_version_, cached);
  if (OB_SUCCESS == cache_ret) {
    ASSERT_FALSE(cached.is_exist_)
        << "stale resolve-before-publish must not leave od_race exist on the current generation";
  } else {
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, cache_ret);
  }

  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *old_s = NULL;
  const ObSimpleTableSchemaV2 *new_s = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(R.tenant_id_, R.database_id_,
                                                      ObString::make_string("od_race"),
                                                      false, old_s));
  ASSERT_EQ(nullptr, old_s);
  ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(R.tenant_id_, R.database_id_,
                                                      ObString::make_string("od_race_new"),
                                                      false, new_s));
  ASSERT_NE(nullptr, new_s);
  ASSERT_EQ(race.table_id.load(), new_s->get_table_id());
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
