/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Verify that SQL issued by an OnDemand SchemaGuard uses the intended index.
 * The SQL is captured from SQL audit after a real cache miss, instead of being
 * duplicated in this test, and is explained in the same tenant that executed it.
 */

#include <gtest/gtest.h>
#include <cctype>
#include <fstream>
#include <functional>
#include <limits.h>
#include <set>
#include <string>
#include <vector>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/schema/ob_simple_schema_kv_cache.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_sql_string.h"
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
  uint64_t database_id_ = OB_INVALID_ID;
  uint64_t table_id_ = OB_INVALID_ID;
};
RunCtx R;

const std::string &plan_report_file()
{
  static std::string path;
  if (path.empty()) {
    char executable[PATH_MAX] = {};
    const ssize_t len = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    if (len > 0) {
      executable[len] = '\0';
      path.assign(executable);
      const size_t slash = path.find_last_of('/');
      path.resize(std::string::npos == slash ? 0 : slash + 1);
    }
    path.append("test_on_demand_schema_sql_plan.readable.log");
  }
  return path;
}

struct PlanRow
{
  std::string op_;
  std::string name_;
};

struct AuditTrace
{
  AuditTrace()
  {
    trace_id_.init(GCTX.self_addr());
    char buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {};
    const int64_t len = trace_id_.to_string(buf, sizeof(buf));
    trace_id_str_.assign(buf, len);
  }

  ObCurTraceId::TraceId trace_id_;
  std::string trace_id_str_;
};

int exec_sql(ObMySQLProxy &proxy, const char *sql)
{
  int64_t affected = 0;
  return proxy.write(sql, affected);
}

int query_uint(ObMySQLProxy &proxy, const char *sql, const char *column, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy.read(res, sql))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
    } else if (OB_FAIL(result->get_uint(column, value))) {
    }
  }
  return ret;
}

int get_latest_guard(ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(R.tenant_id_, guard);
}

int read_publish_snapshot(ObSchemaPublishSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObTenantSchemaPublishState *state = NULL;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_publish_state_map().get(
          R.tenant_id_, state))) {
  } else if (OB_ISNULL(state)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(state->read_stable(snapshot))) {
  }
  return ret;
}

int erase_id_resolve_cache()
{
  ObSchemaPublishSnapshot snapshot;
  int ret = read_publish_snapshot(snapshot);
  if (OB_SUCC(ret)) {
    const ObSchemaIDResolveKey key(
        R.tenant_id_, snapshot.cache_generations_.id_resolve_, R.table_id_);
    ret = GCTX.schema_service_->get_schema_resolve_cache().erase(key);
  }
  return ret;
}

int erase_name_resolve_cache()
{
  ObSchemaPublishSnapshot snapshot;
  int ret = read_publish_snapshot(snapshot);
  if (OB_SUCC(ret)) {
    const ObSchemaNameResolveKey key(
        R.tenant_id_, snapshot.cache_generations_.name_resolve_, R.database_id_,
        0 /*session_id*/, NS_NORMAL_TABLE, ObString::make_string("od_plan_target"));
    ret = GCTX.schema_service_->get_schema_resolve_cache().erase(key);
  }
  return ret;
}

int erase_simple_object_cache()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *schema = NULL;
  if (OB_FAIL(get_latest_guard(guard))) {
  } else if (OB_FAIL(guard.get_simple_table_schema(
                 R.tenant_id_, R.table_id_, schema))) {
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObSimpleSchemaCacheKey key(
        R.tenant_id_, R.table_id_, schema->get_schema_version());
    ret = GCTX.schema_service_->get_simple_schema_object_cache().erase(key);
  }
  return ret;
}

int get_audit_watermark(ObMySQLProxy &proxy, uint64_t &request_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT IFNULL(MAX(request_id), 0) AS request_id "
      "FROM oceanbase.__all_virtual_sql_audit WHERE tenant_id = %lu",
      R.tenant_id_);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "request_id", request_id);
  }
  return ret;
}

int read_schema_inner_sqls(ObMySQLProxy &proxy,
                           const uint64_t after_request_id,
                           const std::string &trace_id,
                           std::vector<std::string> &sqls)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqls.clear();
  if (OB_FAIL(sql.assign_fmt(
      "SELECT query_sql FROM oceanbase.__all_virtual_sql_audit "
      "WHERE tenant_id = %lu AND request_id > %lu AND is_inner_sql = 1 "
      "AND trace_id = '%.*s' "
      "AND (query_sql LIKE 'SELECT%%__all_%%' OR query_sql LIKE '(SELECT%%__all_%%') "
      "AND query_sql NOT LIKE '%%__all_ddl_operation%%' "
      "AND query_sql NOT LIKE '%%__all_core_table%%' "
      "AND query_sql NOT LIKE '%%__all_table_stat%%' "
      "ORDER BY request_id",
      R.tenant_id_, after_request_id,
      static_cast<int>(trace_id.length()), trace_id.data()))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int next_ret = OB_SUCCESS;
        while (OB_SUCCESS == (next_ret = result->next())) {
          ObString query_sql;
          if (OB_FAIL(result->get_varchar("query_sql", query_sql))) {
            ret = OB_ERR_UNEXPECTED;
            break;
          } else {
            sqls.push_back(std::string(query_sql.ptr(), query_sql.length()));
          }
        }
        if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
          ret = next_ret;
        }
      }
    }
  }
  return ret;
}

bool match_id_resolve_sql(const std::string &sql)
{
  return std::string::npos != sql.find("range_side")
      && std::string::npos != sql.find("UNION ALL");
}

bool match_name_resolve_sql(const std::string &sql)
{
  return std::string::npos != sql.find(
             "SELECT table_id, table_name, schema_version")
      && std::string::npos != sql.find("database_id");
}

bool match_next_name_binding_sql(const std::string &sql)
{
  return std::string::npos != sql.find("SELECT table_name, schema_version")
      && std::string::npos != sql.find("schema_version >")
      && std::string::npos != sql.find("is_deleted = 0");
}

bool match_foreign_key_name_sql(const std::string &sql)
{
  return std::string::npos != sql.find("FROM __all_foreign_key_history fk")
      && std::string::npos != sql.find("INNER JOIN __all_table_history t");
}

bool match_constraint_name_sql(const std::string &sql)
{
  return std::string::npos != sql.find("FROM __all_constraint_history cst")
      && std::string::npos != sql.find("INNER JOIN __all_table_history t");
}

bool match_full_schema_completion_sql(const std::string &sql)
{
  return std::string::npos != sql.find("FROM __all_column_history");
}

bool match_database_scan_sql(const std::string &sql)
{
  return std::string::npos != sql.find("FROM __all_table WHERE")
      && std::string::npos != sql.find("database_id");
}

bool wait_for_schema_sqls(ObMySQLProxy &proxy,
                          const uint64_t after_request_id,
                          const std::string &trace_id,
                          const std::function<bool(const std::string &)> &matches,
                          std::vector<std::string> &sqls)
{
  const int64_t deadline = ObTimeUtility::current_time() + 5L * 1000L * 1000L;
  do {
    std::vector<std::string> current_sqls;
    if (OB_SUCCESS == read_schema_inner_sqls(
            proxy, after_request_id, trace_id, current_sqls)) {
      bool found = false;
      for (const std::string &sql : current_sqls) {
        if (matches(sql)) {
          found = true;
          break;
        }
      }
      sqls.swap(current_sqls);
      if (found) {
        return true;
      }
    }
    usleep(20L * 1000L);
  } while (ObTimeUtility::current_time() < deadline);
  return false;
}

const std::string *find_sql(const std::vector<std::string> &sqls,
                            const char *needle1,
                            const char *needle2 = NULL)
{
  for (const std::string &sql : sqls) {
    if (std::string::npos != sql.find(needle1)
        && (NULL == needle2 || std::string::npos != sql.find(needle2))) {
      return &sql;
    }
  }
  return NULL;
}

std::string trim_copy(const std::string &input)
{
  const size_t begin = input.find_first_not_of(" \t\r\n");
  if (std::string::npos == begin) {
    return std::string();
  }
  const size_t end = input.find_last_not_of(" \t\r\n");
  return input.substr(begin, end - begin + 1);
}

// Inner schema SQL is captured from audit with an implicit oceanbase database.
// Tenant proxy2 is connected to `test`, so EXPLAIN must qualify __all_* tables.
std::string qualify_inner_tables(const std::string &sql)
{
  std::string out = sql;
  size_t pos = 0;
  while (std::string::npos != (pos = out.find("__all_", pos))) {
    const bool already_qualified =
        pos >= 10 && 0 == out.compare(pos - 10, 10, "oceanbase.");
    if (already_qualified) {
      pos += 6;
    } else {
      out.insert(pos, "oceanbase.");
      pos += 16;
    }
  }
  return out;
}

void parse_explain_line(const std::string &line, std::vector<PlanRow> &plan)
{
  if (line.empty() || '|' != line[0]
      || std::string::npos != line.find("OPERATOR")
      || std::string::npos != line.find("---")
      || std::string::npos != line.find("===")) {
    return;
  }
  std::vector<std::string> fields;
  size_t start = 0;
  while (start <= line.size()) {
    const size_t bar = line.find('|', start);
    if (std::string::npos == bar) {
      fields.push_back(line.substr(start));
      break;
    }
    fields.push_back(line.substr(start, bar - start));
    start = bar + 1;
  }
  if (fields.size() < 4) {
    return;
  }
  const std::string id = trim_copy(fields[1]);
  if (id.empty() || !isdigit(static_cast<unsigned char>(id[0]))) {
    return;
  }
  plan.push_back(PlanRow{trim_copy(fields[2]), trim_copy(fields[3])});
}

int explain_sql(ObMySQLProxy &proxy,
                const std::string &query_sql,
                std::vector<PlanRow> &plan,
                std::string &full_plan)
{
  int ret = OB_SUCCESS;
  ObSqlString explain;
  plan.clear();
  full_plan.clear();
  const std::string qualified = qualify_inner_tables(query_sql);
  if (OB_FAIL(explain.assign("EXPLAIN "))) {
  } else if (OB_FAIL(explain.append(qualified.data(), qualified.length()))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, explain.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int next_ret = OB_SUCCESS;
        while (OB_SUCCESS == (next_ret = result->next())) {
          ObString line;
          // OceanBase EXPLAIN returns a single "Query Plan" text column.
          if (OB_FAIL(result->get_varchar(0L, line))) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          const std::string plan_line(line.ptr(), line.length());
          full_plan.append(plan_line).append("\n");
          parse_explain_line(plan_line, plan);
        }
        if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
          ret = next_ret;
        }
      }
    }
  }
  LOG_INFO("explain_sql: schema SQL and full execution plan",
      "query_sql", query_sql.c_str(),
      "qualified_sql", qualified.c_str(),
      "full_plan", full_plan.c_str(),
      K(ret));
  return ret;
}

bool has_full_table_scan(const std::vector<PlanRow> &plan)
{
  for (const PlanRow &row : plan) {
    if (std::string::npos != row.op_.find("TABLE FULL SCAN")) {
      return true;
    }
  }
  return false;
}

bool has_index_access(const std::vector<PlanRow> &plan, const char *index_name)
{
  for (const PlanRow &row : plan) {
    const bool indexed_op = std::string::npos != row.op_.find("TABLE GET")
        || std::string::npos != row.op_.find("TABLE RANGE SCAN")
        || std::string::npos != row.op_.find("TABLE SCAN");
    if (indexed_op && std::string::npos != row.name_.find(index_name)) {
      return true;
    }
  }
  return false;
}

std::string plan_access_summary(const std::vector<PlanRow> &plan)
{
  std::string out;
  for (const PlanRow &row : plan) {
    if (!out.empty()) {
      out.append(", ");
    }
    out.append(row.op_);
    if (!row.name_.empty()) {
      out.append(" [").append(row.name_).append("]");
    }
  }
  return out;
}

int init_plan_report()
{
  int ret = OB_SUCCESS;
  std::ofstream report(plan_report_file(), std::ios::out | std::ios::trunc);
  if (!report.is_open()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    report << "OnDemand Schema SQL execution-plan report\n"
           << "Generated from SQL captured in __all_virtual_sql_audit after real API calls.\n"
           << "Each entry contains: SQL category, occurrence scenario, SQL, plan, and verdict.\n\n";
    report.close();
    if (report.fail()) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

const char *classify_schema_sql(const std::string &sql)
{
  if (std::string::npos != sql.find("range_side")
      && std::string::npos != sql.find("UNION ALL")) {
    return "SQL-1: L1 id-resolve version range";
  } else if (std::string::npos != sql.find("FROM __all_foreign_key_history fk")
             && std::string::npos != sql.find("INNER JOIN __all_table_history t")) {
    return "SQL-6: foreign-key-name lookup";
  } else if (std::string::npos != sql.find("FROM __all_constraint_history cst")
             && std::string::npos != sql.find("INNER JOIN __all_table_history t")) {
    return "SQL-7: constraint-name lookup";
  } else if (std::string::npos != sql.find("SELECT table_id, table_name, schema_version")) {
    return "SQL-3: L1 name-resolve";
  } else if (std::string::npos != sql.find("SELECT table_name, schema_version")
             && std::string::npos != sql.find("schema_version >")) {
    return "SQL-5: next name binding";
  } else if (std::string::npos != sql.find("SELECT table_name, database_id, session_id")) {
    return "SQL-4: precise name identity check";
  } else if (std::string::npos != sql.find("FROM __all_part_history")
             || std::string::npos != sql.find("FROM __all_def_sub_part_history")
             || std::string::npos != sql.find("FROM __all_sub_part_history")) {
    return "SQL-2a/SQL-8: partition history";
  } else if (std::string::npos != sql.find("SELECT is_deleted, foreign_key_id")
             && std::string::npos != sql.find("FROM __all_foreign_key_history")) {
    return "SQL-2b: simple foreign-key summary";
  } else if (std::string::npos != sql.find("SELECT * FROM __all_constraint_history")) {
    return "SQL-2c/SQL-8: constraint history";
  } else if (std::string::npos != sql.find("SELECT * FROM __all_table_history")
             && std::string::npos != sql.find("schema_version =")) {
    return "SQL-2: precise SimpleSchema object";
  } else if (std::string::npos != sql.find("FROM __all_table WHERE")) {
    return "SQL-9: current-state bypass scan";
  }
  return "SQL-8: Full TableSchema auxiliary query";
}

int append_plan_report(const char *scenario,
                       const std::string &sql,
                       const std::string &full_plan,
                       const std::vector<PlanRow> &plan)
{
  int ret = OB_SUCCESS;
  static int64_t entry_no = 0;
  const bool full_scan = has_full_table_scan(plan);
  std::ofstream report(plan_report_file(), std::ios::out | std::ios::app);
  if (!report.is_open()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t next_entry_no = entry_no + 1;
    report << "================================================================================\n"
           << "Entry: " << next_entry_no << "\n"
           << "Category: " << classify_schema_sql(sql) << "\n"
           << "Scenario: " << scenario << "\n"
           << "SQL:\n" << sql << "\n\n"
           << "Execution plan:\n" << full_plan << "\n"
           << "Access summary: " << plan_access_summary(plan) << "\n"
           << "Verdict: " << (full_scan ? "FAIL - TABLE FULL SCAN" : "PASS - no TABLE FULL SCAN")
           << "\n\n";
    report.close();
    if (report.fail()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      entry_no = next_entry_no;
    }
  }
  return ret;
}

void explain_captured_sqls(ObMySQLProxy &proxy,
                           const char *scenario,
                           const std::vector<std::string> &sqls)
{
  std::set<std::string> explained;
  for (const std::string &sql : sqls) {
    if (!explained.insert(sql).second) {
      continue;
    }
    std::vector<PlanRow> plan;
    std::string full_plan;
    ASSERT_EQ(OB_SUCCESS, explain_sql(proxy, sql, plan, full_plan))
        << qualify_inner_tables(sql);
    ASSERT_FALSE(plan.empty()) << sql << "\nplan:\n" << full_plan;
    ASSERT_EQ(OB_SUCCESS, append_plan_report(scenario, sql, full_plan, plan));
    ASSERT_FALSE(has_full_table_scan(plan)) << sql << "\nplan:\n" << full_plan;
  }
}

void assert_plan_uses(ObMySQLProxy &proxy,
                      const std::string &sql,
                      const char *expected_index)
{
  std::vector<PlanRow> plan;
  std::string full_plan;
  ASSERT_EQ(OB_SUCCESS, explain_sql(proxy, sql, plan, full_plan))
      << qualify_inner_tables(sql);
  ASSERT_FALSE(plan.empty()) << sql << "\nplan:\n" << full_plan;
  ASSERT_FALSE(has_full_table_scan(plan)) << sql << "\nplan:\n" << full_plan;
  ASSERT_TRUE(has_index_access(plan, expected_index))
      << "expected index " << expected_index << ", sql: " << sql
      << "\nplan:\n" << full_plan;
}

} // namespace

class TestOnDemandSchemaSqlPlan : public ObSimpleClusterTestBase
{
public:
  TestOnDemandSchemaSqlPlan()
      : ObSimpleClusterTestBase("test_on_demand_schema_sql_plan_", "10G", "20G")
  {}

  void SetUp() override
  {
    if (!is_started_) {
      GCONF._enable_simple_schema_on_demand = true;
    }
    ObSimpleClusterTestBase::SetUp();
    ASSERT_NE(nullptr, GCTX.schema_service_);
    ASSERT_EQ(ObSimpleSchemaMode::ON_DEMAND, GCTX.schema_service_->get_schema_mode());

    // The simple cluster is shared by all TEST_F instances in this process.
    // Prepare the tenant and schema lazily so every case can also be selected
    // and run on its own with --gtest_filter.
    static bool schema_prepared = false;
    if (!schema_prepared) {
      prepare_tenant_and_schema_history();
      ASSERT_FALSE(HasFatalFailure());
      schema_prepared = true;
    }
  }

private:
  void prepare_tenant_and_schema_history()
  {
    ASSERT_EQ(OB_SUCCESS, init_plan_report());
    ASSERT_EQ(OB_SUCCESS, create_tenant("od_plan", "3G", "3G", false, 2));
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, "od_plan"));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("od_plan", "test"));
    ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
    ObMySQLProxy &sys = get_curr_simple_server().get_sql_proxy();
    ASSERT_EQ(OB_SUCCESS, exec_sql(proxy,
        "create table od_plan_parent(id bigint primary key)"));
    ASSERT_EQ(OB_SUCCESS, exec_sql(proxy,
        "create table od_plan_target("
        "id bigint, parent_id bigint, c1 int, c2 varchar(32), primary key(id), "
        "constraint od_plan_fk foreign key(parent_id) references od_plan_parent(id), "
        "constraint od_plan_ck check(c1 >= 0)) partition by hash(id) partitions 4"));

    // Keep the tested predicates selective enough that a cost-based full scan is
    // not accidentally preferred merely because the inner table has two rows.
    for (int64_t i = 0; i < 32; ++i) {
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
          "create table od_plan_seed_%ld(id int primary key)", i));
      ASSERT_EQ(OB_SUCCESS, exec_sql(proxy, sql.ptr()));
    }

    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table "
        "WHERE tenant_id = %lu AND table_name = 'od_plan_target' LIMIT 1",
        R.tenant_id_));
    ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "table_id", R.table_id_));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT database_id FROM oceanbase.__all_virtual_table "
        "WHERE tenant_id = %lu AND table_id = %lu",
        R.tenant_id_, R.table_id_));
    ASSERT_EQ(OB_SUCCESS, query_uint(sys, sql.ptr(), "database_id", R.database_id_));
    ASSERT_NE(OB_INVALID_ID, R.table_id_);
    ASSERT_NE(OB_INVALID_ID, R.database_id_);
  }
};

TEST_F(TestOnDemandSchemaSqlPlan, id_resolve_uses_history_primary_key)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, erase_simple_object_cache());
  ASSERT_EQ(OB_SUCCESS, erase_id_resolve_cache());
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));

  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(
        R.tenant_id_, R.table_id_, schema));
  }
  ASSERT_NE(nullptr, schema);

  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_id_resolve_sql, sqls));
  explain_captured_sqls(proxy,
      "SQL-1 + SQL-2/2a/2b/2c: table_id lookup after L1 and L2 cache miss",
      sqls);
  const std::string *id_sql = find_sql(sqls, "range_side", "UNION ALL");
  ASSERT_NE(nullptr, id_sql);
  assert_plan_uses(proxy, *id_sql, "__all_table_history");
  LOG_INFO("getSimpleTableSchema by id, using primary index", "sql", id_sql->c_str());
}

TEST_F(TestOnDemandSchemaSqlPlan, name_resolve_uses_name_history_index)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, erase_simple_object_cache());
  ASSERT_EQ(OB_SUCCESS, erase_id_resolve_cache());
  ASSERT_EQ(OB_SUCCESS, erase_name_resolve_cache());
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));

  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(
        R.tenant_id_, R.database_id_, ObString::make_string("od_plan_target"),
        false /*is_index*/, schema));
  }
  ASSERT_NE(nullptr, schema);

  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_name_resolve_sql, sqls));
  explain_captured_sqls(proxy,
      "SQL-3 + SQL-1/4 + SQL-2 family: table-name lookup after name-cache miss",
      sqls);
  const std::string *name_sql = find_sql(
      sqls, "SELECT table_id, table_name, schema_version", "database_id");
  ASSERT_NE(nullptr, name_sql);
  assert_plan_uses(proxy, *name_sql, "idx_table_name_history");
  LOG_INFO("getSimpleTableSchema by name, using name history index", "sql", name_sql->c_str());
}

TEST_F(TestOnDemandSchemaSqlPlan, missing_name_resolve_uses_next_binding_index)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));

  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2 *schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_simple_table_schema(
        R.tenant_id_, R.database_id_, ObString::make_string("od_plan_never_exists"),
        false /*is_index*/, schema));
  }
  ASSERT_EQ(nullptr, schema);

  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_next_name_binding_sql, sqls));
  explain_captured_sqls(proxy,
      "SQL-3 + SQL-5: unresolved table name and next-name-binding boundary lookup",
      sqls);
}

TEST_F(TestOnDemandSchemaSqlPlan, foreign_key_name_lookup_has_no_full_scan)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));
  ObSchemaGetterGuard guard;
  uint64_t foreign_key_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_foreign_key_id(
        R.tenant_id_, R.database_id_, ObString::make_string("od_plan_fk"),
        foreign_key_id));
  }
  ASSERT_NE(OB_INVALID_ID, foreign_key_id);
  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_foreign_key_name_sql, sqls));
  explain_captured_sqls(proxy,
      "SQL-6: direct foreign-key-name lookup (bypasses L1/L2)", sqls);
}

TEST_F(TestOnDemandSchemaSqlPlan, constraint_name_lookup_has_no_full_scan)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));
  ObSchemaGetterGuard guard;
  uint64_t constraint_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_constraint_id(
        R.tenant_id_, R.database_id_, ObString::make_string("od_plan_ck"),
        constraint_id));
  }
  ASSERT_NE(OB_INVALID_ID, constraint_id);
  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_constraint_name_sql, sqls));
  explain_captured_sqls(proxy,
      "SQL-7: direct constraint-name lookup (bypasses L1/L2)", sqls);
}

TEST_F(TestOnDemandSchemaSqlPlan, full_schema_completion_has_no_full_scan)
{
  // Warm only the ID range. A fresh Guard still has to complete the full
  // ObTableSchema from all relevant history tables.
  ObSchemaGetterGuard warm_guard;
  const ObSimpleTableSchemaV2 *simple_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(warm_guard));
  ASSERT_EQ(OB_SUCCESS, warm_guard.get_simple_table_schema(
      R.tenant_id_, R.table_id_, simple_schema));
  ASSERT_NE(nullptr, simple_schema);

  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));
  ObSchemaGetterGuard guard;
  const ObTableSchema *full_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(
        R.tenant_id_, R.table_id_, full_schema));
  }
  ASSERT_NE(nullptr, full_schema);

  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_full_schema_completion_sql, sqls));
  ASSERT_GT(sqls.size(), 1);
  explain_captured_sqls(proxy,
      "SQL-8: Full TableSchema completion after Guard/schema-cache miss",
      sqls);
  const int64_t sql_count = sqls.size();
  LOG_INFO("getFullTableSchema by id, has no full scan", K(sql_count));
}

TEST_F(TestOnDemandSchemaSqlPlan, bypass_database_scan_uses_db_name_index)
{
  ObMySQLProxy &proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &audit_proxy = get_curr_simple_server().get_sql_proxy();
  uint64_t watermark = 0;
  ASSERT_EQ(OB_SUCCESS, get_audit_watermark(audit_proxy, watermark));
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> table_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(guard));
  AuditTrace audit_trace;
  {
    ObTraceIdGuard trace_guard(audit_trace.trace_id_);
    ASSERT_EQ(OB_SUCCESS, guard.get_table_ids_in_database(
        R.tenant_id_, R.database_id_, table_ids));
  }
  ASSERT_FALSE(table_ids.empty());

  std::vector<std::string> sqls;
  ASSERT_TRUE(wait_for_schema_sqls(
      audit_proxy, watermark, audit_trace.trace_id_str_,
      match_database_scan_sql, sqls));
  const std::string *scan_sql = find_sql(
      sqls, "FROM __all_table WHERE", "database_id");
  ASSERT_NE(nullptr, scan_sql);
  assert_plan_uses(proxy, *scan_sql, "idx_db_tb_name");
  explain_captured_sqls(proxy,
      "SQL-9: bypass scan for get_table_ids_in_database",
      sqls);
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
