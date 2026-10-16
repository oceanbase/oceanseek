/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_name_resolver.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_id_resolver.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

using schema_resolve_util::append_table_namespace_filter;
using schema_resolve_util::is_table_namespace_matched;
using schema_resolve_util::read_table_resolve_row_by_name;
using schema_resolve_util::weak_read_sql;
using schema_resolve_util::check_is_oracle_index;
using schema_resolve_util::append_name_predicate;
using schema_resolve_util::stored_table_name_matches_resolve_name;

namespace
{

int build_not_exist_at_snapshot_range_(const int64_t snapshot_version,
                                       const ObTableNameNamespace table_namespace,
                                       ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  version_range = ObSchemaResolveRange(snapshot_version, snapshot_version, OB_INVALID_ID,
                                       OB_INVALID_VERSION, 0, table_namespace, false);
  if (!version_range.is_valid() || !version_range.contains(snapshot_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid not-exist name resolve range", KR(ret), K(version_range),
             K(snapshot_version));
  }
  return ret;
}

int get_next_name_binding_version_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const ObTableNameNamespace table_namespace,
    const uint64_t database_id,
    const ObString &normalized_name,
    const uint64_t session_id,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaService &schema_service,
    int64_t &next_binding_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString namespace_filter;
  const char *history_table = NULL;
  const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
  const uint64_t exec_tenant_id = ObSchemaServiceSQLImpl::fill_exec_tenant_id(schema_status);
  const uint64_t extract_tenant_id =
      ObSchemaServiceSQLImpl::fill_extract_tenant_id(schema_status, tenant_id);
  const uint64_t extract_database_id =
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id);
  const bool case_compare = (OB_ORIGIN_AND_SENSITIVE != name_case_mode);
  const bool compare_with_collation = true;
  next_binding_version = OB_SCHEMA_VERSION_INF;
  bool is_oracle_index = false;
  if (OB_FAIL(check_is_oracle_index(
          tenant_id, database_id, table_namespace, is_oracle_index))) {
    LOG_WARN("failed to check oracle index", KR(ret), K(tenant_id),
             K(database_id), K(table_namespace));
  } else if (OB_FAIL(append_table_namespace_filter(table_namespace, namespace_filter))) {
    LOG_WARN("failed to build table namespace filter", KR(ret), K(table_namespace));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
      exec_tenant_id, history_table, GCTX.schema_service_))) {
    LOG_WARN("failed to get table history name", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.append_fmt(
      "SELECT table_name, schema_version FROM %s "
      "WHERE tenant_id = %lu AND database_id = %lu AND ",
      history_table, extract_tenant_id, extract_database_id))) {
    LOG_WARN("failed to append next name binding sql prefix", KR(ret), K(tenant_id),
             K(database_id), K(session_id));
  } else if (OB_FAIL(append_name_predicate(
                 sql, is_oracle_index, normalized_name))) {
    LOG_WARN("failed to append name predicate", KR(ret), K(normalized_name),
             K(is_oracle_index));
  } else if (OB_FAIL(sql.append_fmt(
      " %sAND session_id = %lu AND is_deleted = 0 AND schema_version > %ld "
      "ORDER BY schema_version ASC",
      namespace_filter.ptr(), session_id, snapshot_version))) {
    LOG_WARN("failed to append next name binding sql", KR(ret), K(tenant_id),
             K(database_id), K(normalized_name), K(session_id), K(snapshot_version));
  } else if (!is_oracle_index
             && OB_ORIGIN_AND_SENSITIVE != name_case_mode
             && OB_FAIL(sql.append(" LIMIT 1"))) {
    LOG_WARN("failed to append next name binding sql limit", KR(ret),
             K(name_case_mode));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(weak_read_sql(
          sql_client, snapshot_timestamp, exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("failed to read next name binding", KR(ret), K(tenant_id),
                 K(database_id), K(normalized_name), K(session_id));
        LOG_TRACE("next name binding sql", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("next name binding result is null", KR(ret), K(tenant_id),
                 K(database_id), K(normalized_name), K(session_id));
      } else {
        while (OB_SUCC(ret) && OB_SCHEMA_VERSION_INF == next_binding_version) {
          ObString row_table_name;
          int64_t row_schema_version = OB_INVALID_VERSION;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next name binding row", KR(ret), K(tenant_id),
                       K(database_id), K(normalized_name), K(session_id));
            }
          } else {
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", row_table_name);
            EXTRACT_INT_FIELD_MYSQL(
                *result, "schema_version", row_schema_version, int64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to extract next name binding row", KR(ret), K(tenant_id),
                       K(database_id), K(normalized_name), K(session_id));
            } else {
              bool name_matched = false;
              if (OB_FAIL(stored_table_name_matches_resolve_name(
                      normalized_name, row_table_name, is_oracle_index,
                      schema_service, case_compare, compare_with_collation,
                      name_matched))) {
                LOG_WARN("failed to match next name binding row", KR(ret),
                         K(normalized_name), K(row_table_name),
                         K(is_oracle_index));
              } else if (name_matched) {
                next_binding_version = row_schema_version;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int build_not_exist_name_range_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const ObTableNameNamespace table_namespace,
    const uint64_t database_id,
    const ObString &normalized_name,
    const uint64_t session_id,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaService &schema_service,
    const int64_t range_start,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  int64_t next_binding_version = OB_SCHEMA_VERSION_INF;
  if (OB_FAIL(get_next_name_binding_version_(
      sql_client, schema_status, tenant_id, table_namespace, database_id,
      normalized_name, session_id, snapshot_version, name_case_mode,
      schema_service, next_binding_version))) {
    LOG_WARN("failed to get next name binding version", KR(ret), K(tenant_id),
             K(database_id), K(normalized_name), K(session_id), K(snapshot_version));
  } else {
    const int64_t range_end = OB_SCHEMA_VERSION_INF == next_binding_version
        ? OB_SCHEMA_VERSION_INF : next_binding_version - 1;
    version_range = ObSchemaResolveRange(
        range_start, range_end, OB_INVALID_ID, OB_INVALID_VERSION,
        0 /*admission_seq*/, table_namespace, false);
    if (!version_range.is_valid() || !version_range.contains(snapshot_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid not-exist name resolve range", KR(ret), K(version_range),
               K(snapshot_version), K(next_binding_version));
    }
  }
  return ret;
}

int check_precise_table_identity_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableNameNamespace table_namespace,
    const uint64_t database_id,
    const ObString &table_name,
    const uint64_t session_id,
    const int64_t precise_schema_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaService &schema_service,
    bool &identity_matched)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *history_table = NULL;
  const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
  const uint64_t exec_tenant_id = ObSchemaServiceSQLImpl::fill_exec_tenant_id(schema_status);
  const uint64_t extract_tenant_id =
      ObSchemaServiceSQLImpl::fill_extract_tenant_id(schema_status, tenant_id);
  const uint64_t extract_table_id =
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
  const uint64_t extract_database_id =
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id);
  const bool case_compare = (OB_ORIGIN_AND_SENSITIVE != name_case_mode);
  const bool compare_with_collation = true;
  identity_matched = false;
  if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
      exec_tenant_id, history_table, GCTX.schema_service_))) {
    LOG_WARN("failed to get table history name", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT table_name, database_id, session_id, is_deleted, "
      "table_type, table_mode, index_type FROM %s "
      "WHERE tenant_id = %lu AND table_id = %lu AND schema_version = %ld",
      history_table, extract_tenant_id, extract_table_id, precise_schema_version))) {
    LOG_WARN("failed to build precise table identity sql", KR(ret), K(tenant_id),
             K(table_id), K(precise_schema_version));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(weak_read_sql(
          sql_client, snapshot_timestamp, exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("failed to read precise table identity", KR(ret), K(tenant_id),
                 K(table_id), K(precise_schema_version));
        LOG_TRACE("precise table identity sql", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("precise table identity result is null", KR(ret), K(tenant_id),
                 K(table_id), K(precise_schema_version));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to get precise table identity row", KR(ret), K(tenant_id),
                 K(table_id), K(precise_schema_version));
        LOG_TRACE("precise table identity sql", K(sql));
      } else {
        uint64_t row_database_id = OB_INVALID_ID;
        uint64_t row_session_id = OB_INVALID_ID;
        bool is_deleted = false;
        ObTableType table_type = MAX_TABLE_TYPE;
        ObTableMode table_mode;
        ObIndexType index_type = INDEX_TYPE_IS_NOT;
        ObString row_table_name;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", row_table_name);
        EXTRACT_INT_FIELD_MYSQL(*result, "database_id", row_database_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "session_id", row_session_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "is_deleted", is_deleted, bool);
        EXTRACT_INT_FIELD_MYSQL(*result, "table_type", table_type, ObTableType);
        EXTRACT_INT_FIELD_MYSQL(*result, "table_mode", table_mode.mode_, uint32_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "index_type", index_type, ObIndexType);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to extract precise table identity", KR(ret), K(tenant_id),
                   K(table_id), K(precise_schema_version));
        } else if (is_deleted) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("existing id range points to deleted precise row", KR(ret),
                   K(tenant_id), K(table_id), K(precise_schema_version));
        } else {
          identity_matched = extract_database_id == row_database_id
              && session_id == row_session_id
              && is_table_namespace_matched(
                  table_namespace, table_type, table_mode, index_type);
          if (OB_SUCC(ret) && identity_matched) {
            bool name_matched = false;
            bool is_oracle_index = false;
            if (OB_FAIL(check_is_oracle_index(
                    tenant_id, database_id, table_namespace, is_oracle_index))) {
              LOG_WARN("failed to check oracle index", KR(ret),
                       K(tenant_id), K(database_id), K(table_namespace));
            } else if (OB_FAIL(stored_table_name_matches_resolve_name(
                           table_name, row_table_name, is_oracle_index,
                           schema_service, case_compare, compare_with_collation,
                           name_matched))) {
              LOG_WARN("failed to match precise table name", KR(ret), K(table_name),
                       K(row_table_name), K(is_oracle_index));
            } else {
              identity_matched = name_matched;
            }
          }
          LOG_TRACE("[OnDemandNameResolve] precise table identity checked",
                    K(tenant_id), K(table_id), K(precise_schema_version),
                    K(database_id), K(row_database_id), K(table_name),
                    K(row_table_name), K(session_id), K(row_session_id),
                    K(table_namespace), K(table_type), "table_mode", table_mode.mode_,
                    K(index_type), K(identity_matched));
        }
      }
    }
  }
  return ret;
}

} // namespace

int ObSchemaNameResolver::resolve_table_by_name_for_session_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const ObSchemaNameResolveKey &key,
    const ObString &table_name,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaService &schema_service,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString namespace_filter;
  const uint64_t tenant_id = key.tenant_id_;
  const ObTableNameNamespace table_namespace = key.table_namespace_;
  const uint64_t database_id = key.database_id_;
  const ObString &normalized_name = key.normalized_name_;
  const uint64_t session_id = key.exact_session_id_;
  const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
  const uint64_t exec_tenant_id = ObSchemaServiceSQLImpl::fill_exec_tenant_id(schema_status);
  const bool case_compare = (OB_ORIGIN_AND_SENSITIVE != name_case_mode);
  const bool compare_with_collation = true;
  const char *history_table = NULL;
  const uint64_t extract_tenant_id =
      ObSchemaServiceSQLImpl::fill_extract_tenant_id(schema_status, tenant_id);
  const uint64_t extract_database_id =
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id);
  bool is_oracle_index = false;
  version_range.reset();
  LOG_TRACE("[OnDemandNameResolve] name backsource input",
           K(tenant_id), K(database_id), K(table_namespace), K(table_name),
           K(normalized_name), K(session_id), K(snapshot_version), K(name_case_mode));
  if (OB_FAIL(check_is_oracle_index(
          tenant_id, database_id, table_namespace, is_oracle_index))) {
    LOG_WARN("failed to check oracle index", KR(ret), K(tenant_id),
             K(database_id), K(table_namespace));
  } else if (OB_FAIL(append_table_namespace_filter(table_namespace, namespace_filter))) {
    LOG_WARN("failed to build table namespace filter", KR(ret), K(table_namespace));
  } else if (normalized_name.empty()
             || name_case_mode <= OB_NAME_CASE_INVALID
             || name_case_mode >= OB_NAME_CASE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid normalized table name or case mode", KR(ret), K(table_name),
             K(normalized_name), K(name_case_mode));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
      exec_tenant_id, history_table, GCTX.schema_service_))) {
    LOG_WARN("failed to get table history name", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(sql.append_fmt(
      "SELECT table_id, table_name, schema_version, is_deleted, "
      "table_type, table_mode, index_type FROM %s "
      "WHERE tenant_id = %lu AND database_id = %lu AND ",
      history_table, extract_tenant_id, extract_database_id))) {
    LOG_WARN("failed to append resolve sql prefix", KR(ret), K(tenant_id), K(database_id),
             K(table_name), K(session_id));
  } else if (OB_FAIL(append_name_predicate(
                 sql, is_oracle_index, normalized_name))) {
    LOG_WARN("failed to append name predicate", KR(ret), K(table_name),
             K(is_oracle_index));
  } else if (OB_FAIL(sql.append_fmt(
      " %sAND session_id = %lu AND schema_version <= %ld "
      "ORDER BY schema_version DESC",
      namespace_filter.ptr(),
      session_id,
      snapshot_version))) {
    LOG_WARN("failed to append resolve sql", KR(ret), K(tenant_id), K(database_id),
             K(table_name), K(session_id));
  } else if (!is_oracle_index
             && OB_ORIGIN_AND_SENSITIVE != name_case_mode
             && OB_FAIL(sql.append(" LIMIT 1"))) {
    LOG_WARN("failed to append resolve sql limit", KR(ret), K(name_case_mode));
  } else {
    bool name_row_matched = false;
    bool got_history_row = false;
    uint64_t matched_table_id = OB_INVALID_ID;
    ObTableNameNamespace matched_namespace = table_namespace;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(weak_read_sql(sql_client, snapshot_timestamp, exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("failed to read resolve sql", KR(ret), K(tenant_id), K(database_id),
                 K(table_name), K(session_id));
        LOG_TRACE("name resolve sql", K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolve result is null", KR(ret), K(tenant_id), K(database_id),
                 K(table_name), K(session_id));
        LOG_TRACE("name resolve sql", K(sql));
      } else {
        while (OB_SUCC(ret) && !name_row_matched) {
          if (OB_FAIL(mysql_result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get resolve row", KR(ret), K(tenant_id), K(database_id),
                       K(table_name), K(session_id));
              LOG_TRACE("name resolve sql", K(sql));
            }
          } else {
            got_history_row = true;
            if (OB_FAIL(read_table_resolve_row_by_name(
                *mysql_result, schema_service, table_namespace, database_id, table_name,
                case_compare, compare_with_collation, is_oracle_index,
                version_range, name_row_matched))) {
              LOG_WARN("failed to read resolve row", KR(ret), K(tenant_id), K(database_id),
                       K(table_name), K(table_namespace), K(session_id));
            } else if (name_row_matched) {
              matched_table_id = version_range.schema_id_;
              matched_namespace = version_range.table_namespace_;
              LOG_TRACE("[OnDemandNameResolve] name backsource phase-a hit",
                       K(tenant_id), K(database_id), K(table_name), K(normalized_name),
                       K(table_namespace), K(matched_table_id), K(matched_namespace),
                       "name_row_exist", version_range.is_exist_, K(session_id),
                       K(snapshot_version));
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (name_row_matched && OB_INVALID_ID != matched_table_id) {
      // Phase B: object-level versions come from table_id history only.
      version_range.schema_id_ = matched_table_id;
      version_range.table_namespace_ = matched_namespace;
      if (OB_FAIL(fill_version_range_by_table_id_(
              sql_client, schema_status, key, table_name, snapshot_version,
              name_case_mode, schema_service, version_range))) {
        LOG_WARN("failed to fill version range by table id", KR(ret), K(tenant_id),
                 K(database_id), K(table_name), K(session_id),
                 "table_id", matched_table_id);
      } else if (!version_range.is_valid() || !version_range.contains(snapshot_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid bound name resolve range", KR(ret), K(version_range),
                 K(snapshot_version));
      }
    } else {
      // In mode 0, the case-insensitive inner-table collation may return only
      // case-mismatched candidates. Treat that as a normal not-exist result.
      if (got_history_row && !name_row_matched) {
        LOG_TRACE("[OnDemandNameResolve] no exact name match after SQL filter",
                  K(tenant_id), K(database_id), K(table_name), K(normalized_name),
                  K(table_namespace), K(session_id), K(snapshot_version), K(sql));
      }
      if (OB_FAIL(got_history_row
          ? build_not_exist_at_snapshot_range_(
              snapshot_version, table_namespace, version_range)
          : build_not_exist_name_range_(
              sql_client, schema_status, tenant_id, table_namespace, database_id,
              normalized_name, session_id, snapshot_version, name_case_mode,
              schema_service, 0 /*range_start*/, version_range))) {
        LOG_WARN("failed to build not exist name range", KR(ret), K(tenant_id),
                 K(database_id), K(table_name), K(session_id), K(snapshot_version));
      }
    }
  }
  return ret;
}

// Phase A first finds a history row carrying table_name to obtain table_id. DROP TABLE
// history rows do not retain table_name, so Phase B resolves the object by table_id to
// include those tombstone rows and determine is_exist / precise / start / end.
int ObSchemaNameResolver::fill_version_range_by_table_id_(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const ObSchemaNameResolveKey &key,
    const ObString &table_name,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaService &schema_service,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSchemaResolveRange id_version_range;
  const uint64_t tenant_id = key.tenant_id_;
  const ObTableNameNamespace table_namespace = key.table_namespace_;
  const uint64_t database_id = key.database_id_;
  const ObString &normalized_name = key.normalized_name_;
  const uint64_t session_id = key.exact_session_id_;
  const uint64_t table_id = version_range.schema_id_;
  bool identity_matched = false;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", KR(ret), K(tenant_id), K(snapshot_version));
  } else if (OB_FAIL(ObSchemaIDResolver::get_schema_version_range_by_id(
      sql_client, schema_status, GCTX.schema_service_, tenant_id, table_id,
      snapshot_version, id_version_range))) {
    LOG_WARN("failed to resolve by table id", KR(ret), K(tenant_id), K(table_id),
             K(snapshot_version));
  } else if (id_version_range.is_exist_
             && OB_FAIL(check_precise_table_identity_(
                 sql_client, schema_status, tenant_id, table_id, table_namespace,
                 database_id, table_name, session_id,
                 id_version_range.precise_schema_version_, name_case_mode,
                 schema_service, identity_matched))) {
    LOG_WARN("failed to check precise table identity", KR(ret), K(tenant_id),
             K(table_id), K(database_id), K(table_name), K(session_id),
             K(snapshot_version));
  } else if (id_version_range.is_exist_ && identity_matched) {
    version_range = id_version_range;
    version_range.table_namespace_ = table_namespace;
  } else if (OB_FAIL(build_not_exist_name_range_(
      sql_client, schema_status, tenant_id, table_namespace, database_id,
      normalized_name, session_id, snapshot_version, name_case_mode,
      schema_service, id_version_range.start_version_, version_range))) {
    LOG_WARN("failed to build not-exist name range", KR(ret), K(tenant_id),
             K(table_id), K(database_id), K(table_name), K(session_id),
             K(snapshot_version));
  }
  return ret;
}

int ObSchemaNameResolver::get_schema_version_range_by_name_for_session(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const ObSchemaNameResolveKey &key,
    const ObString &table_name,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;
  version_range.reset();
  if (OB_INVALID_TENANT_ID == key.tenant_id_
      || key.table_namespace_ >= NS_MAX
      || OB_INVALID_ID == key.database_id_
      || table_name.empty()
      || key.normalized_name_.empty()
      || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key), K(table_name), K(snapshot_version));
  } else if (OB_ISNULL(GCTX.schema_service_)
             || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(resolve_table_by_name_for_session_(
      sql_client, schema_status, key, table_name, snapshot_version, name_case_mode,
      *schema_service, version_range))) {
    LOG_WARN("failed to resolve table for exact session", KR(ret), K(key), K(table_name));
  }
  return ret;
}

int ObSchemaNameResolver::get_schema_version_range_by_name(
    ObISQLClient &sql_client,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const ObTableNameNamespace table_namespace,
    const uint64_t database_id,
    const ObString &table_name,
    const uint64_t session_id,
    const int64_t snapshot_version,
    const ObNameCaseMode name_case_mode,
    ObSchemaResolveRange &version_range)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;
  ObSchemaResolveRange session_version_range;
  ObArenaAllocator allocator("SchemaNameNorm");
  ObString normalized_name;
  version_range.reset();
  if (OB_INVALID_TENANT_ID == tenant_id
      || table_namespace >= NS_MAX
      || OB_INVALID_ID == database_id
      || table_name.empty()
      || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(table_name), K(snapshot_version));
  } else if (OB_ISNULL(GCTX.schema_service_)
             || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (name_case_mode <= OB_NAME_CASE_INVALID
             || name_case_mode >= OB_NAME_CASE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name case mode", KR(ret), K(name_case_mode));
  } else {
    if (OB_ORIGIN_AND_SENSITIVE == name_case_mode) {
      if (OB_FAIL(ob_write_string(allocator, table_name, normalized_name))) {
        LOG_WARN("failed to copy case-sensitive table name", KR(ret), K(table_name));
      }
    } else if (OB_FAIL(ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI,
                                         table_name, normalized_name, allocator))) {
      LOG_WARN("failed to normalize table name", KR(ret), K(table_name), K(name_case_mode));
    }
    // Prefer current-session temporary table, then fall back to session_id=0.
    // Each call runs phase A (name -> table_id) then phase B (table_id -> version range).
    // cache_generation is unused by SQL backsource.
    const ObSchemaNameResolveKey session_key(
        tenant_id, 0 /*cache_generation*/, database_id, session_id,
        table_namespace, normalized_name);
    const ObSchemaNameResolveKey global_key(
        tenant_id, 0 /*cache_generation*/, database_id, 0 /*exact_session_id*/,
        table_namespace, normalized_name);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resolve_table_by_name_for_session_(
        sql_client, schema_status, session_key, table_name, snapshot_version,
        name_case_mode, *schema_service, session_version_range))) {
      LOG_WARN("failed to resolve table for session", KR(ret), K(tenant_id), K(database_id),
               K(table_name), K(session_id));
    } else if (session_version_range.is_exist_ || 0 == session_id) {
      version_range = session_version_range;
    } else if (OB_FAIL(resolve_table_by_name_for_session_(
        sql_client, schema_status, global_key, table_name, snapshot_version,
        name_case_mode, *schema_service, version_range))) {
      LOG_WARN("failed to resolve non-temporary table", KR(ret), K(tenant_id), K(database_id),
               K(table_name), K(session_id));
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
