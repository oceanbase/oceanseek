/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Regression: name resolve false-negative across namespaces.
 *
 * Real SQL writes create the same stored table_name under NS_HIDDEN_TABLE and
 * NS_NORMAL_TABLE. The critical window is while offline DDL still keeps a live
 * hidden object; we race a normal CREATE against that name so both namespaces
 * are non-deleted at the same snapshot, with the normal row newer.
 *
 * Without table_mode in resolve SQL, ORDER BY schema_version DESC LIMIT 1 returns
 * the normal row and NS_HIDDEN_TABLE post-match falsely reports not-exist.
 *
 * Strong asserts run inside that live window. After offline DDL finishes, names
 * may be swapped/tombstoned, so post-DDL checks are soft.
 */

#include <atomic>
#include <thread>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_simple_schema_kv_cache.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace share::schema::schema_resolve_util;

namespace
{

struct RunCtx
{
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t database_id_ = OB_INVALID_ID;
  uint64_t parent_table_id_ = OB_INVALID_ID;
  uint64_t hidden_table_id_ = OB_INVALID_ID;
  uint64_t normal_collide_table_id_ = OB_INVALID_ID;
  bool live_hidden_during_collide_ = false;
  bool strong_window_passed_ = false;
  char hidden_stored_name_[OB_MAX_TABLE_NAME_LENGTH] = {0};
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

int query_varchar(ObMySQLProxy &proxy, const char *sql, const char *col,
                  char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObString value;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    buf[0] = '\0';
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_varchar(col, value))) {
      } else if (value.length() >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        MEMCPY(buf, value.ptr(), value.length());
        buf[value.length()] = '\0';
      }
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
      "WHERE tenant_id = %lu AND table_name = '%s' "
      "ORDER BY schema_version DESC LIMIT 1",
      tenant_id, table_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int get_database_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                            const char *db_name, uint64_t &database_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT database_id FROM oceanbase.__all_virtual_database "
      "WHERE tenant_id = %lu AND database_name = '%s' LIMIT 1",
      tenant_id, db_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "database_id", database_id);
  }
  return ret;
}

int get_latest_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard);
}

int resolve_name_with_retry(ObSchemaGetterGuard &guard,
                            const uint64_t tenant_id,
                            const ObTableNameNamespace table_namespace,
                            const uint64_t database_id,
                            const ObString &table_name,
                            ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  const int64_t max_retry = 60;
  for (int64_t i = 0; i < max_retry; ++i) {
    version_range.reset();
    if (OB_FAIL(get_latest_guard(tenant_id, guard))) {
    } else {
      ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
      if (OB_FAIL(guard.get_tenant_name_case_mode(tenant_id, name_case_mode))) {
      } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_range_by_name(
          guard, tenant_id, table_namespace, database_id, table_name,
          0 /*session_id*/, name_case_mode, version_range))) {
        if (OB_SCHEMA_EAGAIN == ret) {
          usleep(200 * 1000);
          ret = OB_SUCCESS;
          continue;
        }
      } else if (!version_range.is_exist_) {
        // Publish may lag right after CREATE; retry a few times before accepting miss.
        if (i + 1 < max_retry) {
          usleep(200 * 1000);
          continue;
        }
      }
    }
    break;
  }
  return ret;
}

int fetch_normal_collide_id_(ObMySQLProxy &sys_proxy, uint64_t &table_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
      "AND ((table_mode >> 12) & 2) = 0 "
      "ORDER BY schema_version DESC LIMIT 1",
      R.tenant_id_, R.database_id_, R.hidden_stored_name_);
  if (OB_SUCC(ret)) {
    ret = query_uint(sys_proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

bool hidden_still_live_(ObMySQLProxy &sys_proxy)
{
  uint64_t live_id = OB_INVALID_ID;
  ObSqlString sql;
  if (OB_SUCCESS != sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
      "AND table_id = %lu AND ((table_mode >> 12) & 2) != 0 "
      "LIMIT 1",
      R.tenant_id_, R.database_id_, R.hidden_stored_name_, R.hidden_table_id_)) {
    return false;
  }
  return OB_SUCCESS == query_uint(sys_proxy, sql.ptr(), "table_id", live_id)
      && live_id == R.hidden_table_id_;
}

} // namespace

class TestOnDemandNameResolveNamespace : public ObSimpleClusterTestBase
{
public:
  TestOnDemandNameResolveNamespace()
      : ObSimpleClusterTestBase("test_on_demand_name_resolve_namespace_", "10G", "20G")
  {}
};

TEST_F(TestOnDemandNameResolveNamespace, prepare_tenant_and_cross_namespace_rows)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("od_ns_mysql", "3G", "3G", false /*oracle_mode*/, 2));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "od_ns_mysql"));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("od_ns_mysql", "test"));
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "use test"));

  ASSERT_EQ(OB_SUCCESS,
            exec_sql(proxy2, "create table t_ns_parent(id int primary key, c2 tinyint)"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "insert into t_ns_parent values (1, 1)"));
  usleep(1 * 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS,
            get_table_id_by_name(sys_proxy, R.tenant_id_, "t_ns_parent", R.parent_table_id_));
  ASSERT_NE(OB_INVALID_ID, R.parent_table_id_);
  ASSERT_EQ(OB_SUCCESS, get_database_id_by_name(sys_proxy, R.tenant_id_, "test", R.database_id_));
  ASSERT_NE(OB_INVALID_ID, R.database_id_);

  // Race: while offline DDL still holds a live hidden object, create a normal
  // table that reuses the hidden stored name so both namespaces are alive.
  std::atomic<int> alter_ret(OB_SUCCESS);
  std::thread alter_th([&]() {
    alter_ret.store(exec_sql(proxy2, "alter table t_ns_parent drop primary key"));
  });

  ObSqlString sql;
  bool created_collide = false;
  for (int64_t i = 0; i < 400 && !created_collide; ++i) {
    if (OB_SUCCESS != sql.assign_fmt(
        "SELECT table_name, table_id FROM oceanbase.__all_virtual_table "
        "WHERE tenant_id = %lu AND database_id = %lu "
        "AND table_name LIKE '__hidden%%' "
        "AND association_table_id = %lu "
        "AND ((table_mode >> 12) & 2) != 0 "
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.database_id_, R.parent_table_id_)) {
      break;
    }
    int ret = OB_SUCCESS;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObString name;
      uint64_t hidden_id = OB_INVALID_ID;
      ret = sys_proxy.read(res, sql.ptr());
      if (OB_SUCC(ret) && OB_NOT_NULL(result = res.get_result())
          && OB_SUCC(result->next())
          && OB_SUCC(result->get_varchar("table_name", name))
          && OB_SUCC(result->get_uint("table_id", hidden_id))
          && name.length() > 0 && name.length() < sizeof(R.hidden_stored_name_)) {
        MEMCPY(R.hidden_stored_name_, name.ptr(), name.length());
        R.hidden_stored_name_[name.length()] = '\0';
        R.hidden_table_id_ = hidden_id;
        if (OB_SUCCESS == sql.assign_fmt(
                "create table `%s`(id int primary key, note varchar(64))",
                R.hidden_stored_name_)
            && OB_SUCCESS == exec_sql(proxy2, sql.ptr())) {
          created_collide = true;
          R.live_hidden_during_collide_ = true;
        }
      }
    }
    usleep(20 * 1000);
  }

  // Strong regression must run in the live window, before offline DDL cleanup
  // renames/tombstones the hidden object.
  if (created_collide && R.live_hidden_during_collide_) {
    ASSERT_EQ(OB_SUCCESS, fetch_normal_collide_id_(sys_proxy, R.normal_collide_table_id_));
    ASSERT_NE(OB_INVALID_ID, R.normal_collide_table_id_);
    ASSERT_NE(R.hidden_table_id_, R.normal_collide_table_id_);

    uint64_t newest_unfiltered_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table_history "
        "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
        "AND is_deleted = 0 "
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.database_id_, R.hidden_stored_name_));
    ASSERT_EQ(OB_SUCCESS,
              query_uint(sys_proxy, sql.ptr(), "table_id", newest_unfiltered_id));
    ASSERT_EQ(R.normal_collide_table_id_, newest_unfiltered_id)
        << "unfiltered LIMIT 1 prefers the newer normal row";

    ObSqlString normal_filter;
    ObSqlString hidden_filter;
    ASSERT_EQ(OB_SUCCESS, append_table_namespace_filter(NS_NORMAL_TABLE, normal_filter));
    ASSERT_EQ(OB_SUCCESS, append_table_namespace_filter(NS_HIDDEN_TABLE, hidden_filter));

    uint64_t filtered_normal_id = OB_INVALID_ID;
    uint64_t filtered_hidden_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table_history "
        "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
        "AND is_deleted = 0 %s"
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.database_id_, R.hidden_stored_name_, normal_filter.ptr()));
    ASSERT_EQ(OB_SUCCESS,
              query_uint(sys_proxy, sql.ptr(), "table_id", filtered_normal_id));
    ASSERT_EQ(R.normal_collide_table_id_, filtered_normal_id);

    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table_history "
        "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
        "AND is_deleted = 0 %s"
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.database_id_, R.hidden_stored_name_, hidden_filter.ptr()));
    ASSERT_EQ(OB_SUCCESS,
              query_uint(sys_proxy, sql.ptr(), "table_id", filtered_hidden_id));
    ASSERT_EQ(R.hidden_table_id_, filtered_hidden_id);
    ASSERT_NE(R.normal_collide_table_id_, filtered_hidden_id);

    ObSchemaGetterGuard guard;
    ObSchemaResolveRange normal_range;
    ObSchemaResolveRange hidden_range;
    const ObString table_name(R.hidden_stored_name_);
    ASSERT_EQ(OB_SUCCESS, resolve_name_with_retry(
        guard, R.tenant_id_, NS_NORMAL_TABLE, R.database_id_, table_name, normal_range));
    ASSERT_TRUE(normal_range.is_exist_);
    ASSERT_EQ(R.normal_collide_table_id_, normal_range.schema_id_);

    ASSERT_EQ(OB_SUCCESS, resolve_name_with_retry(
        guard, R.tenant_id_, NS_HIDDEN_TABLE, R.database_id_, table_name, hidden_range));
    ASSERT_TRUE(hidden_range.is_exist_)
        << "NS_HIDDEN_TABLE must not false-negative when a newer same-name normal row exists";
    ASSERT_EQ(R.hidden_table_id_, hidden_range.schema_id_);
    R.strong_window_passed_ = true;
  }

  alter_th.join();
  ASSERT_EQ(OB_SUCCESS, alter_ret.load());
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, "insert into t_ns_parent values (2, 2)"));
  usleep(1 * 1000 * 1000);

  // Fallback: DDL finished too fast for the race. Still create same-name normal
  // table against the historical hidden stored name for SQL-filter coverage.
  if (!created_collide) {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_name FROM oceanbase.__all_virtual_table_history "
        "WHERE tenant_id = %lu AND table_name LIKE '__hidden%%' "
        "AND association_table_id = %lu "
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.parent_table_id_));
    ASSERT_EQ(OB_SUCCESS,
              query_varchar(sys_proxy, sql.ptr(), "table_name",
                            R.hidden_stored_name_, sizeof(R.hidden_stored_name_)));
    ASSERT_GT(STRLEN(R.hidden_stored_name_), 0);
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table_history "
        "WHERE tenant_id = %lu AND table_name = '%s' "
        "AND ((table_mode >> 12) & 2) != 0 "
        "ORDER BY schema_version DESC LIMIT 1",
        R.tenant_id_, R.hidden_stored_name_));
    ASSERT_EQ(OB_SUCCESS,
              query_uint(sys_proxy, sql.ptr(), "table_id", R.hidden_table_id_));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "create table `%s`(id int primary key, note varchar(64))",
        R.hidden_stored_name_));
    ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, sql.ptr()));
    created_collide = true;
    R.live_hidden_during_collide_ = false;
  }
  ASSERT_TRUE(created_collide);
  ASSERT_NE(OB_INVALID_ID, R.hidden_table_id_);
  ASSERT_GT(STRLEN(R.hidden_stored_name_), 0);

  if (OB_INVALID_ID == R.normal_collide_table_id_) {
    ASSERT_EQ(OB_SUCCESS, fetch_normal_collide_id_(sys_proxy, R.normal_collide_table_id_));
  }
  ASSERT_NE(OB_INVALID_ID, R.normal_collide_table_id_);
  ASSERT_NE(R.hidden_table_id_, R.normal_collide_table_id_);

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "insert into `%s` values (1, 'normal_same_stored_name')",
      R.hidden_stored_name_));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2, sql.ptr()));

  // After DDL cleanup, hidden may already be renamed/tombstoned.
  if (R.live_hidden_during_collide_ && !hidden_still_live_(sys_proxy)) {
    LOG_INFO("hidden no longer live after offline DDL cleanup",
             "hidden_stored_name", R.hidden_stored_name_,
             "hidden_table_id", R.hidden_table_id_,
             "strong_window_passed", R.strong_window_passed_);
    R.live_hidden_during_collide_ = false;
  }
}

TEST_F(TestOnDemandNameResolveNamespace, resolve_both_namespaces_for_same_stored_name)
{
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ASSERT_NE(OB_INVALID_ID, R.database_id_);
  ASSERT_GT(STRLEN(R.hidden_stored_name_), 0);

  // Strong window already covered both namespaces while hidden was live.
  if (R.strong_window_passed_) {
    LOG_INFO("strong cross-namespace resolve already verified in prepare window");
    return;
  }

  ObSchemaGetterGuard guard;
  ObSchemaResolveRange normal_range;
  ObSchemaResolveRange hidden_range;
  const ObString table_name(R.hidden_stored_name_);

  ASSERT_EQ(OB_SUCCESS, resolve_name_with_retry(
      guard, R.tenant_id_, NS_NORMAL_TABLE, R.database_id_, table_name, normal_range));
  ASSERT_TRUE(normal_range.is_exist_);
  ASSERT_EQ(R.normal_collide_table_id_, normal_range.schema_id_);

  ASSERT_EQ(OB_SUCCESS, resolve_name_with_retry(
      guard, R.tenant_id_, NS_HIDDEN_TABLE, R.database_id_, table_name, hidden_range));
  if (R.live_hidden_during_collide_) {
    ASSERT_TRUE(hidden_range.is_exist_)
        << "NS_HIDDEN_TABLE must not false-negative when a newer same-name normal row exists";
    ASSERT_EQ(R.hidden_table_id_, hidden_range.schema_id_);
  } else {
    LOG_INFO("skip live hidden assert; offline DDL cleaned hidden before collide",
             "hidden_stored_name", R.hidden_stored_name_,
             "hidden_table_id", R.hidden_table_id_,
             "hidden_exist", hidden_range.is_exist_);
  }
}

TEST_F(TestOnDemandNameResolveNamespace, sql_namespace_filter_matches_history_rows)
{
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ASSERT_GT(STRLEN(R.hidden_stored_name_), 0);
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();

  // Strong window already verified filters against live cross-namespace rows.
  if (R.strong_window_passed_) {
    LOG_INFO("sql namespace filter already verified in prepare window");
    return;
  }

  ObSqlString normal_filter;
  ObSqlString hidden_filter;
  ASSERT_EQ(OB_SUCCESS, append_table_namespace_filter(NS_NORMAL_TABLE, normal_filter));
  ASSERT_EQ(OB_SUCCESS, append_table_namespace_filter(NS_HIDDEN_TABLE, hidden_filter));

  ObSqlString sql;
  uint64_t filtered_normal_id = OB_INVALID_ID;
  uint64_t filtered_hidden_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table_history "
      "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
      "AND is_deleted = 0 %s"
      "ORDER BY schema_version DESC LIMIT 1",
      R.tenant_id_, R.database_id_, R.hidden_stored_name_, normal_filter.ptr()));
  ASSERT_EQ(OB_SUCCESS,
            query_uint(sys_proxy, sql.ptr(), "table_id", filtered_normal_id));
  ASSERT_EQ(R.normal_collide_table_id_, filtered_normal_id);

  // Hidden predicate must not return the newer normal table_id.
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table_history "
      "WHERE tenant_id = %lu AND database_id = %lu AND table_name = '%s' "
      "AND is_deleted = 0 %s"
      "ORDER BY schema_version DESC LIMIT 1",
      R.tenant_id_, R.database_id_, R.hidden_stored_name_, hidden_filter.ptr()));
  int hidden_ret = query_uint(sys_proxy, sql.ptr(), "table_id", filtered_hidden_id);
  if (OB_SUCCESS == hidden_ret) {
    ASSERT_NE(R.normal_collide_table_id_, filtered_hidden_id)
        << "hidden filter must not accidentally hit the normal collide row";
  } else {
    ASSERT_EQ(OB_ITER_END, hidden_ret)
        << "hidden filter must not accidentally hit the normal collide row";
  }
}

TEST_F(TestOnDemandNameResolveNamespace, built_in_index_coexists_with_data_table_resolve)
{
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();

  // Mirror mysqltest step 8d Part B: SQL-engine resolve + aux schemas +
  // namespace filter. Cross-namespace C++ false-negative is covered in prepare's
  // live window; post-DDL C++ name resolve can lag publish/snapshot here.
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "create table t_fts_ns(id int primary key, content varchar(128), "
      "fulltext index ft_ns_idx(content))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(proxy2,
      "insert into t_fts_ns values (1, 'oceanbase on demand name resolve')"));
  int64_t fts_visible = -1;
  for (int64_t i = 0; i < 60; ++i) {
    int ret = OB_SUCCESS;
    fts_visible = -1;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy2.read(res, "select count(*) cnt from t_fts_ns"))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("cnt", fts_visible))) {
      }
    }
    if (OB_SUCCESS == ret && 1 == fts_visible) {
      break;
    }
    usleep(200 * 1000);
  }
  ASSERT_EQ(1, fts_visible);

  uint64_t fts_table_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS,
            get_table_id_by_name(sys_proxy, R.tenant_id_, "t_fts_ns", fts_table_id));
  ASSERT_NE(OB_INVALID_ID, fts_table_id);

  ObSqlString sql;
  uint64_t aux_table_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND data_table_id = %lu AND table_type = 5 "
      "LIMIT 1",
      R.tenant_id_, fts_table_id));
  ASSERT_EQ(OB_SUCCESS, query_uint(sys_proxy, sql.ptr(), "table_id", aux_table_id));
  ASSERT_NE(OB_INVALID_ID, aux_table_id);

  ObSqlString normal_filter;
  ASSERT_EQ(OB_SUCCESS, append_table_namespace_filter(NS_NORMAL_TABLE, normal_filter));
  uint64_t filtered_data_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table_history "
      "WHERE tenant_id = %lu AND database_id = %lu AND table_name = 't_fts_ns' "
      "AND is_deleted = 0 %s"
      "ORDER BY schema_version DESC LIMIT 1",
      R.tenant_id_, R.database_id_, normal_filter.ptr()));
  ASSERT_EQ(OB_SUCCESS,
            query_uint(sys_proxy, sql.ptr(), "table_id", filtered_data_id));
  ASSERT_EQ(fts_table_id, filtered_data_id);
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
