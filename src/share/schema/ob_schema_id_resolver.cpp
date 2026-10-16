/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_id_resolver.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObSchemaIDResolver::get_schema_version_range_by_id(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    ObServerSchemaService *schema_service,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t snapshot_version,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *history_table = NULL;
  bool has_predecessor = false;
  bool is_deleted = false;
  int64_t predecessor = OB_INVALID_VERSION;
  int64_t successor = OB_INVALID_VERSION;
  version_range.reset();
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == table_id
      || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(snapshot_version));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(schema_status.tenant_id_);
    const uint64_t extract_tenant_id =
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    const uint64_t extract_table_id =
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
    if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
            exec_tenant_id, history_table, schema_service))) {
      LOG_WARN("failed to get all table history name", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(sql.assign_fmt(
        "(SELECT schema_version, is_deleted, 0 AS range_side FROM %s "
        "WHERE tenant_id = %lu AND table_id = %lu AND schema_version <= %ld "
        "ORDER BY schema_version DESC LIMIT 1) UNION ALL "
        "(SELECT schema_version, is_deleted, 1 AS range_side FROM %s "
        "WHERE tenant_id = %lu AND table_id = %lu AND schema_version > %ld "
        "ORDER BY schema_version ASC LIMIT 1)",
        history_table, extract_tenant_id, extract_table_id, snapshot_version,
        history_table, extract_tenant_id, extract_table_id, snapshot_version))) {
      LOG_WARN("failed to build id resolve SQL", KR(ret), K(tenant_id), K(table_id));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObSQLClientRetryWeak sql_client_retry(
            &sql_client, false, schema_status.snapshot_timestamp_);
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_client_retry.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to query table version boundaries", KR(ret), K(tenant_id),
                   K(table_id), K(snapshot_version));
          LOG_TRACE("id resolve sql", K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("id resolve result is null", KR(ret), K(tenant_id), K(table_id),
                   K(snapshot_version));
          LOG_TRACE("id resolve sql", K(sql));
        } else {
          int next_ret = OB_SUCCESS;
          while (OB_SUCC(ret) && OB_SUCCESS == (next_ret = result->next())) {
            int64_t side = -1;
            int64_t version = OB_INVALID_VERSION;
            bool row_deleted = false;
            EXTRACT_INT_FIELD_MYSQL(*result, "range_side", side, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", version, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "is_deleted", row_deleted, bool);
            if (OB_SUCC(ret) && 0 == side) {
              has_predecessor = true;
              predecessor = version;
              is_deleted = row_deleted;
            } else if (OB_SUCC(ret) && 1 == side) {
              successor = version;
            } else if (OB_SUCC(ret)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected range_side", KR(ret), K(side));
            }
          }
          if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
            ret = next_ret;
            LOG_WARN("failed to iterate id resolve result", KR(ret), K(tenant_id), K(table_id));
            LOG_TRACE("id resolve sql", K(sql));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t range_start = has_predecessor ? predecessor : 0;
    const int64_t range_end = OB_INVALID_VERSION == successor
        ? OB_SCHEMA_VERSION_INF : successor - 1;
    if (range_end < range_start) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid table version boundaries", KR(ret), K(tenant_id), K(table_id),
                K(snapshot_version), K(predecessor), K(successor));
    } else if (has_predecessor && !is_deleted) {
      version_range = ObSchemaResolveRange(range_start, range_end, table_id, predecessor,
                                           0 /*admission_seq*/, NS_MAX, true);
    } else {
      version_range = ObSchemaResolveRange(range_start, range_end, OB_INVALID_ID,
                                           OB_INVALID_VERSION, 0 /*admission_seq*/, NS_MAX, false);
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
