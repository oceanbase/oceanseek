/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_simple_schema_lookup_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_get_compat_mode.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

using schema_resolve_util::weak_read_sql;

namespace {

// Align FK/constraint name SQL with Original HashMap (MySQL CI / Oracle CS) and
// table-name resolve: push case semantics into the predicate. Exact "=" plus
// post schema_name_is_equal cannot recover rows filtered by a CS history column.
int append_object_name_eq_predicate_(const bool is_oracle_mode,
                                     const char *column_expr,
                                     const char *escaped_name,
                                     ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_expr) || OB_ISNULL(escaped_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(column_expr), KP(escaped_name));
  } else if (is_oracle_mode) {
    if (OB_FAIL(sql.append_fmt("CAST(%s AS BINARY) = '%s'", column_expr, escaped_name))) {
      LOG_WARN("failed to append binary name predicate", KR(ret), K(column_expr));
    }
  } else if (OB_FAIL(sql.append_fmt("LOWER(%s) = LOWER('%s')", column_expr, escaped_name))) {
    LOG_WARN("failed to append folded name predicate", KR(ret), K(column_expr));
  }
  return ret;
}

} // namespace

int ObSimpleSchemaLookupService::fetch_simple_table_schema(
    ObISQLClient &sql_client,
    ObSchemaService &schema_service,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t precise_schema_version,
    ObIAllocator &allocator,
    const ObSimpleTableSchemaV2 *&schema)
{
  int ret = OB_SUCCESS;
  ObSimpleTableSchemaV2 *table_schema = NULL;
  schema = NULL;
  if (!is_valid_tenant_id(tenant_id)
      || OB_INVALID_ID == table_id
      || precise_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(precise_schema_version));
  } else if (INT64_MAX != precise_schema_version
             && OB_FAIL(schema_service.can_read_schema_version(
                 schema_status, precise_schema_version))) {
    // Let the request-level retry controller handle OB_SCHEMA_EAGAIN with a
    // deadline-aware short backoff instead of blocking the lookup thread here.
    LOG_WARN("schema version is not readable now", KR(ret), K(precise_schema_version));
  } else if (OB_FAIL(schema_service.get_simple_table_by_precise_version(
      schema_status, sql_client, allocator, tenant_id, table_id,
      precise_schema_version, table_schema))) {
    LOG_WARN("failed to get simple table by precise version", KR(ret), K(tenant_id),
             K(table_id), K(precise_schema_version));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("simple table schema not exist", KR(ret), K(tenant_id), K(table_id),
             K(precise_schema_version));
  } else {
    schema = table_schema;
  }
  return ret;
}

int ObSimpleSchemaLookupService::get_foreign_key_info_by_name(
    ObISQLClient &sql_client,
    ObSchemaService &schema_service,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &foreign_key_name,
    const int64_t snapshot_version,
    ObSimpleForeignKeyInfo &info)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
  const uint64_t exec_tenant_id = ObSchemaServiceSQLImpl::fill_exec_tenant_id(schema_status);
  const uint64_t extract_tenant_id = ObSchemaServiceSQLImpl::fill_extract_tenant_id(schema_status, tenant_id);
  const uint64_t extract_database_id =
      ObSchemaServiceSQLImpl::fill_extract_schema_id(schema_status, database_id);
  const char *history_table = NULL;
  bool is_oracle_mode = false;
  const bool skip_escape = false;
  const bool use_oracle_mode = false;
  ObCStringHelper helper;
  const char *fk_name = helper.convert(
      ObHexEscapeSqlStr(foreign_key_name, skip_escape, use_oracle_mode));
  info.reset();
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == database_id
      || foreign_key_name.empty()
      || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(foreign_key_name),
             K(snapshot_version));
  } else if (OB_ISNULL(fk_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to escape foreign key name", KR(ret), K(foreign_key_name));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
      tenant_id, is_oracle_mode))) {
    LOG_WARN("failed to check oracle mode", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
      exec_tenant_id, history_table, GCTX.schema_service_))) {
    LOG_WARN("failed to get table history name", KR(ret), K(exec_tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *result = NULL;
      const bool case_compare = !is_oracle_mode;
      const bool compare_with_collation = true;
      // Restrict name candidates first, then use history rowkeys to resolve each
      // candidate at the guard snapshot. Avoid aggregating the whole tenant.
      if (OB_FAIL(sql.assign_fmt(
          "SELECT fk.foreign_key_id, fk.foreign_key_name, fk.child_table_id, "
          "t.table_mode, t.table_type "
          "FROM %s fk "
          "INNER JOIN %s t ON fk.tenant_id = t.tenant_id AND fk.child_table_id = t.table_id "
          "WHERE fk.tenant_id = %lu AND ",
          OB_ALL_FOREIGN_KEY_HISTORY_TNAME,
          history_table,
          extract_tenant_id))) {
        LOG_WARN("failed to assign foreign key sql prefix", KR(ret), K(tenant_id),
                 K(database_id), K(foreign_key_name), K(snapshot_version));
      } else if (OB_FAIL(append_object_name_eq_predicate_(
          is_oracle_mode, "fk.foreign_key_name", fk_name, sql))) {
        LOG_WARN("failed to append foreign key name predicate", KR(ret), K(tenant_id),
                 K(foreign_key_name), K(is_oracle_mode));
      } else if (OB_FAIL(sql.append_fmt(
          " AND fk.schema_version <= %ld AND fk.schema_version = ("
          "SELECT /*+ NO_UNNEST */ MAX(fk_latest.schema_version) FROM %s fk_latest "
          "WHERE fk_latest.tenant_id = fk.tenant_id "
          "AND fk_latest.foreign_key_id = fk.foreign_key_id "
          "AND fk_latest.schema_version <= %ld) "
          "AND fk.is_deleted = 0 AND t.database_id = %lu "
          "AND t.schema_version <= %ld AND t.schema_version = ("
          "SELECT /*+ NO_UNNEST */ MAX(t_latest.schema_version) FROM %s t_latest "
          "WHERE t_latest.tenant_id = t.tenant_id AND t_latest.table_id = t.table_id "
          "AND t_latest.schema_version <= %ld) AND t.is_deleted = 0",
          snapshot_version,
          OB_ALL_FOREIGN_KEY_HISTORY_TNAME,
          snapshot_version,
          extract_database_id,
          snapshot_version,
          history_table,
          snapshot_version))) {
        LOG_WARN("failed to append foreign key sql", KR(ret), K(tenant_id), K(database_id),
                 K(foreign_key_name), K(snapshot_version));
      } else if (OB_FAIL(weak_read_sql(
          sql_client, snapshot_timestamp, exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("failed to read foreign key sql", KR(ret), K(tenant_id), K(database_id),
                 K(foreign_key_name));
        LOG_TRACE("foreign key lookup sql", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("foreign key result is null", KR(ret), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          uint64_t foreign_key_id = OB_INVALID_ID;
          uint64_t child_table_id = OB_INVALID_ID;
          ObString tmp_foreign_key_name;
          ObTableType tmp_table_type = MAX_TABLE_TYPE;
          ObTableMode tmp_table_mode;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next foreign key row", KR(ret), K(tenant_id),
                       K(database_id), K(foreign_key_name));
              LOG_TRACE("foreign key lookup sql", K(sql));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result, "foreign_key_id", foreign_key_id, uint64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "foreign_key_name", tmp_foreign_key_name);
            EXTRACT_INT_FIELD_MYSQL(*result, "child_table_id", child_table_id, uint64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_type", tmp_table_type, ObTableType);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_mode", tmp_table_mode.mode_, uint32_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to extract foreign key row", KR(ret));
            } else if (tmp_table_mode.is_user_hidden_table()
                       || is_index_table(tmp_table_type)
                       || is_aux_lob_table(tmp_table_type)) {
              // Skip non-user-table children; FK name resolve only targets visible user tables.
            } else if (schema_service.schema_name_is_equal(
                foreign_key_name, tmp_foreign_key_name, case_compare, compare_with_collation)) {
              info.tenant_id_ = tenant_id;
              info.database_id_ = database_id;
              info.table_id_ = child_table_id;
              info.foreign_key_id_ = foreign_key_id;
              info.foreign_key_name_.assign_ptr(const_cast<char *>(foreign_key_name.ptr()),
                                                foreign_key_name.length());
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaLookupService::get_constraint_info_by_name(
    ObISQLClient &sql_client,
    ObSchemaService &schema_service,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &constraint_name,
    const int64_t snapshot_version,
    ObSimpleConstraintInfo &info)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_timestamp = schema_status.snapshot_timestamp_;
  const uint64_t exec_tenant_id = ObSchemaServiceSQLImpl::fill_exec_tenant_id(schema_status);
  const uint64_t extract_tenant_id = ObSchemaServiceSQLImpl::fill_extract_tenant_id(schema_status, tenant_id);
  const uint64_t extract_database_id =
      ObSchemaServiceSQLImpl::fill_extract_schema_id(schema_status, database_id);
  const char *history_table = NULL;
  bool is_oracle_mode = false;
  const bool skip_escape = false;
  const bool use_oracle_mode = false;
  ObCStringHelper helper;
  const char *cst_name = helper.convert(
      ObHexEscapeSqlStr(constraint_name, skip_escape, use_oracle_mode));
  info.reset();
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == database_id
      || constraint_name.empty()
      || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(database_id), K(constraint_name),
             K(snapshot_version));
  } else if (OB_ISNULL(cst_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to escape constraint name", KR(ret), K(constraint_name));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
      tenant_id, is_oracle_mode))) {
    LOG_WARN("failed to check oracle mode", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSchemaUtils::get_all_table_history_name(
      exec_tenant_id, history_table, GCTX.schema_service_))) {
    LOG_WARN("failed to get table history name", KR(ret), K(exec_tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *result = NULL;
      const bool case_compare = !is_oracle_mode;
      const bool compare_with_collation = true;
      // Restrict name candidates first, then use history rowkeys to resolve each
      // candidate at the guard snapshot. Avoid aggregating the whole tenant.
      if (OB_FAIL(sql.assign_fmt(
          "SELECT cst.constraint_id, cst.constraint_name, cst.table_id, "
          "t.table_mode, t.table_type "
          "FROM %s cst "
          "INNER JOIN %s t ON cst.tenant_id = t.tenant_id AND cst.table_id = t.table_id "
          "WHERE cst.tenant_id = %lu AND ",
          OB_ALL_CONSTRAINT_HISTORY_TNAME,
          history_table,
          extract_tenant_id))) {
        LOG_WARN("failed to assign constraint sql prefix", KR(ret), K(tenant_id),
                 K(database_id), K(constraint_name), K(snapshot_version));
      } else if (OB_FAIL(append_object_name_eq_predicate_(
          is_oracle_mode, "cst.constraint_name", cst_name, sql))) {
        LOG_WARN("failed to append constraint name predicate", KR(ret), K(tenant_id),
                 K(constraint_name), K(is_oracle_mode));
      } else if (OB_FAIL(sql.append_fmt(
          " AND cst.schema_version <= %ld AND cst.schema_version = ("
          "SELECT /*+ NO_UNNEST */ MAX(cst_latest.schema_version) FROM %s cst_latest "
          "WHERE cst_latest.tenant_id = cst.tenant_id "
          "AND cst_latest.table_id = cst.table_id "
          "AND cst_latest.constraint_id = cst.constraint_id "
          "AND cst_latest.schema_version <= %ld) "
          "AND cst.is_deleted = 0 AND t.database_id = %lu "
          "AND t.schema_version <= %ld AND t.schema_version = ("
          "SELECT /*+ NO_UNNEST */ MAX(t_latest.schema_version) FROM %s t_latest "
          "WHERE t_latest.tenant_id = t.tenant_id AND t_latest.table_id = t.table_id "
          "AND t_latest.schema_version <= %ld) AND t.is_deleted = 0",
          snapshot_version,
          OB_ALL_CONSTRAINT_HISTORY_TNAME,
          snapshot_version,
          extract_database_id,
          snapshot_version,
          history_table,
          snapshot_version))) {
        LOG_WARN("failed to append constraint sql", KR(ret), K(tenant_id), K(database_id),
                 K(constraint_name), K(snapshot_version));
      } else if (OB_FAIL(weak_read_sql(
          sql_client, snapshot_timestamp, exec_tenant_id, sql.ptr(), res))) {
        LOG_WARN("failed to read constraint sql", KR(ret), K(tenant_id), K(database_id),
                 K(constraint_name));
        LOG_TRACE("constraint lookup sql", K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constraint result is null", KR(ret), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          uint64_t constraint_id = OB_INVALID_ID;
          uint64_t table_id = OB_INVALID_ID;
          ObString tmp_constraint_name;
          ObTableType tmp_table_type = MAX_TABLE_TYPE;
          ObTableMode tmp_table_mode;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next constraint row", KR(ret), K(tenant_id),
                       K(database_id), K(constraint_name));
              LOG_TRACE("constraint lookup sql", K(sql));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result, "constraint_id", constraint_id, uint64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "constraint_name", tmp_constraint_name);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_type", tmp_table_type, ObTableType);
            EXTRACT_INT_FIELD_MYSQL(*result, "table_mode", tmp_table_mode.mode_, uint32_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to extract constraint row", KR(ret));
            } else if (tmp_table_mode.is_user_hidden_table()
                       || is_index_table(tmp_table_type)
                       || is_aux_lob_table(tmp_table_type)
                       || is_mysql_tmp_table(tmp_table_type)) {
              // Skip non-user-table owners; constraint name resolve only targets visible user tables.
            } else if (schema_service.schema_name_is_equal(
                constraint_name, tmp_constraint_name, case_compare, compare_with_collation)) {
              info.tenant_id_ = tenant_id;
              info.database_id_ = database_id;
              info.table_id_ = table_id;
              info.constraint_id_ = constraint_id;
              info.constraint_name_.assign_ptr(const_cast<char *>(constraint_name.ptr()),
                                               constraint_name.length());
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSimpleSchemaLookupService::post_process_simple_table_schema(
    ObSchemaGetterGuard &guard,
    const uint64_t tenant_id,
    ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  bool need_read_sys_variable = false;
  if (OB_FAIL(ObMultiVersionSchemaService::get_tenant_name_case_mode_by_compat_mode(
          tenant_id, mode, need_read_sys_variable))) {
    LOG_WARN("failed to get tenant name case mode by compat mode", KR(ret),
             K(tenant_id), "table_id", table_schema.get_table_id());
  } else if (need_read_sys_variable) {
    const ObSysVariableSchema *sys_variable = NULL;
    if (OB_FAIL(guard.get_sys_variable_schema(tenant_id, sys_variable))) {
      LOG_WARN("failed to get sys variable schema for name case mode", KR(ret),
               K(tenant_id), "table_id", table_schema.get_table_id());
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", KR(ret), K(tenant_id),
               "table_id", table_schema.get_table_id());
    } else {
      mode = sys_variable->get_name_case_mode();
    }
  }
  if (OB_SUCC(ret)) {
    table_schema.set_name_case_mode(mode);
    // Align with ObSchemaMgr::add_table: recyclebin indexes keep the generated
    // __recycle_$_ / RECYCLE_$_ name and must not be parsed as __idx_*.
    // generate_origin_index_name() returns OB_SCHEMA_ERROR (-4029) on those
    // names and breaks PURGE RECYCLEBIN in ON_DEMAND mode.
    if (table_schema.is_index_table()
        && !table_schema.is_in_recyclebin()
        && OB_FAIL(table_schema.generate_origin_index_name())) {
      LOG_WARN("failed to generate origin index name", KR(ret),
               K(tenant_id), "table_id", table_schema.get_table_id());
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
