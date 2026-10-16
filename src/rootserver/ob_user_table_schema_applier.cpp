/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ob_user_table_schema_applier.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_rpc_struct.h"
#include "common/ob_timeout_ctx.h"
#include "observer/ob_server.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_define.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

namespace
{

int append_column_default_and_on_update_sql_(const ObColumnSchemaV2 &column, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (!(ObNullType == column.get_cur_default_value().get_type() && !column.is_nullable())) {
    if (IS_DEFAULT_NOW_OBJ(column.get_cur_default_value())) {
      const int16_t scale = column.get_data_scale();
      if (0 == scale) {
        if (OB_FAIL(sql.append(" DEFAULT CURRENT_TIMESTAMP"))) {
          LOG_WARN("append default current_timestamp failed", KR(ret));
        }
      } else if (OB_FAIL(sql.append_fmt(" DEFAULT CURRENT_TIMESTAMP(%d)", scale))) {
        LOG_WARN("append default current_timestamp failed", KR(ret), K(scale));
      }
    } else if (column.is_default_expr_v2_column()) {
      const ObString default_value = column.get_cur_default_value().get_string();
      if (OB_FAIL(sql.append_fmt(" DEFAULT (%.*s)",
              static_cast<int>(default_value.length()),
              default_value.ptr()))) {
        LOG_WARN("append default expression failed", KR(ret), K(default_value));
      }
    } else {
      ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
      ObObj default_value = column.get_cur_default_value();
      char *buf = NULL;
      int64_t buf_len = 0;
      int64_t pos = 0;
      default_value.set_scale(column.get_data_scale());
      if (OB_FAIL(sql.append(" DEFAULT"))) {
        LOG_WARN("append default keyword failed", KR(ret));
      } else if (OB_FAIL(default_value.print_sql_literal(buf, buf_len, pos, allocator))) {
        LOG_WARN("print default sql literal failed", KR(ret), K(default_value));
      } else if (OB_FAIL(sql.append_fmt(" %.*s", static_cast<int>(pos), buf))) {
        LOG_WARN("append default sql literal failed", KR(ret), K(pos));
      }
    }
  }

  if (OB_SUCC(ret) && column.is_on_update_current_timestamp()) {
    const int16_t scale = column.get_data_scale();
    if (0 == scale) {
      if (OB_FAIL(sql.append(" ON UPDATE CURRENT_TIMESTAMP"))) {
        LOG_WARN("append on update current_timestamp failed", KR(ret));
      }
    } else if (OB_FAIL(sql.append_fmt(" ON UPDATE CURRENT_TIMESTAMP(%d)", scale))) {
      LOG_WARN("append on update current_timestamp failed", KR(ret), K(scale));
    }
  }
  return ret;
}

int column_schema_equal_(const ObColumnSchemaV2 &expected_column,
                         const ObColumnSchemaV2 &existing_column,
                         bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (expected_column.get_data_type() != existing_column.get_data_type()
      || expected_column.is_nullable() != existing_column.is_nullable()
      || expected_column.get_data_length() != existing_column.get_data_length()
      || expected_column.get_data_precision() != existing_column.get_data_precision()
      || expected_column.get_data_scale() != existing_column.get_data_scale()) {
    // not equal
  } else {
    ObSqlString expected_options;
    ObSqlString existing_options;
    if (OB_FAIL(append_column_default_and_on_update_sql_(expected_column, expected_options))) {
      LOG_WARN("append expected column options failed", KR(ret), K(expected_column));
    } else if (OB_FAIL(append_column_default_and_on_update_sql_(existing_column, existing_options))) {
      LOG_WARN("append existing column options failed", KR(ret), K(existing_column));
    } else {
      is_equal = (0 == expected_options.string().compare(existing_options.string()));
    }
  }
  return ret;
}

int append_column_def_sql_(const ObColumnSchemaV2 &column, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char *nullable = column.is_nullable() ? " NULL" : " NOT NULL";
  switch (column.get_data_type()) {
    case ObIntType:
      if (OB_FAIL(sql.append_fmt(" `%.*s` bigint%s",
              static_cast<int>(column.get_column_name_str().length()),
              column.get_column_name_str().ptr(), nullable))) {
        LOG_WARN("append int col failed", KR(ret));
      }
      break;
    case ObUInt64Type:
      if (OB_FAIL(sql.append_fmt(" `%.*s` bigint unsigned%s",
              static_cast<int>(column.get_column_name_str().length()),
              column.get_column_name_str().ptr(), nullable))) {
        LOG_WARN("append uint col failed", KR(ret));
      }
      break;
    case ObVarcharType:
      if (OB_FAIL(sql.append_fmt(" `%.*s` varchar(%d)%s",
              static_cast<int>(column.get_column_name_str().length()),
              column.get_column_name_str().ptr(),
              column.get_data_length(), nullable))) {
        LOG_WARN("append varchar col failed", KR(ret));
      }
      break;
    case ObTimestampType: {
      const int16_t scale = column.get_data_scale();
      if (0 == scale) {
        if (OB_FAIL(sql.append_fmt(" `%.*s` timestamp%s",
                static_cast<int>(column.get_column_name_str().length()),
                column.get_column_name_str().ptr(), nullable))) {
          LOG_WARN("append timestamp col failed", KR(ret));
        }
      } else if (OB_FAIL(sql.append_fmt(" `%.*s` timestamp(%d)%s",
              static_cast<int>(column.get_column_name_str().length()),
              column.get_column_name_str().ptr(), scale, nullable))) {
        LOG_WARN("append timestamp col failed", KR(ret));
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported type for ddl", KR(ret), "type", column.get_data_type());
      break;
  }
  if (OB_SUCC(ret) && OB_FAIL(append_column_default_and_on_update_sql_(column, sql))) {
    LOG_WARN("append column options failed", KR(ret), K(column));
  }
  return ret;
}

} // namespace

ObUserTableSchemaApplier::ObUserTableSchemaApplier()
  : is_inited_(false),
    schema_service_(NULL),
    sql_proxy_(NULL),
    timeout_ctx_(NULL)
{
}

ObUserTableSchemaApplier::~ObUserTableSchemaApplier()
{
}

int ObUserTableSchemaApplier::init(ObMultiVersionSchemaService &schema_service,
                                   ObMySQLProxy &sql_proxy,
                                   const ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    timeout_ctx_ = &timeout_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObUserTableSchemaApplier::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(timeout_ctx_)) {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObUserTableSchemaApplier::get_database_id_(ObSchemaGetterGuard &guard,
                                               const uint64_t tenant_id,
                                               const ObString &database_name,
                                               uint64_t &database_id) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *db_schema = NULL;
  database_id = OB_INVALID_ID;
  if (OB_FAIL(guard.get_database_schema(tenant_id, database_name, db_schema))) {
    LOG_WARN("get database schema failed", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database not exist", KR(ret), K(tenant_id), K(database_name));
  } else {
    database_id = db_schema->get_database_id();
  }
  return ret;
}

int ObUserTableSchemaApplier::build_builtin_table_schema_(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObUserTableBuiltinSchemaEntry &entry,
    ObTableSchema &table_schema) const
{
  int ret = OB_SUCCESS;
  table_schema.reset();
  if (OB_ISNULL(entry.create_schema_) || OB_ISNULL(entry.database_name_) || OB_ISNULL(entry.table_name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid builtin schema entry", KR(ret), KP(entry.create_schema_));
  } else {
    // create_schema_ uses ADD_COLUMN_SCHEMA; add_column needs tenant_id_ for compat mode check.
    // table_id_ stays invalid: we only turn schema into CREATE DDL by name, not catalog ids.
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_database_id(database_id);
    if (OB_FAIL(entry.create_schema_(table_schema))) {
      LOG_WARN("create builtin table schema failed", KR(ret), K(tenant_id), "table", entry.table_name_);
    } else if (!table_schema.get_tablegroup_name().empty()) {
      ObString tablegroup_name = table_schema.get_tablegroup_name();
      if (OB_FAIL(table_schema.set_tablegroup_name(tablegroup_name))) {
        LOG_WARN("set tablegroup name failed", KR(ret), K(tenant_id), K(tablegroup_name));
      }
    }
  }
  return ret;
}

int ObUserTableSchemaApplier::check_table_exists_(ObSchemaGetterGuard &guard,
                                                  const uint64_t tenant_id,
                                                  const uint64_t database_id,
                                                  const ObString &table_name,
                                                  bool &exists,
                                                  const ObTableSchema *&exist_schema) const
{
  int ret = OB_SUCCESS;
  exists = false;
  exist_schema = NULL;
  if (OB_FAIL(guard.get_table_schema(tenant_id, database_id, table_name, false, exist_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_NOT_NULL(exist_schema)) {
    exists = true;
  }
  return ret;
}

int ObUserTableSchemaApplier::check_and_get_column_diff_(
    const ObTableSchema &exist_schema,
    const ObTableSchema &expect_schema,
    ObIArray<ObString> &add_columns,
    ObIArray<ObString> &modify_columns) const
{
  int ret = OB_SUCCESS;
  add_columns.reset();
  modify_columns.reset();
  // expect_schema is a DDL template (no table_id / schema_version); compare columns by name only.
  for (int64_t i = 0; OB_SUCC(ret) && i < expect_schema.get_column_count(); ++i) {
    const ObColumnSchemaV2 *expect_col = expect_schema.get_column_schema_by_idx(i);
    const ObColumnSchemaV2 *exist_col = NULL;
    bool is_equal = false;
    if (OB_ISNULL(expect_col)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(exist_col = exist_schema.get_column_schema(expect_col->get_column_name()))) {
      if (OB_FAIL(add_columns.push_back(expect_col->get_column_name_str()))) {
        LOG_WARN("push add column failed", KR(ret));
      }
    } else if (OB_FAIL(column_schema_equal_(*expect_col, *exist_col, is_equal))) {
      LOG_WARN("compare column schema failed", KR(ret), KPC(expect_col), KPC(exist_col));
    } else if (!is_equal) {
      if (OB_FAIL(modify_columns.push_back(expect_col->get_column_name_str()))) {
        LOG_WARN("push modify column failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObUserTableSchemaApplier::generate_alter_table_ddl_(
    const ObString &database_name,
    const ObString &table_name,
    const ObTableSchema &expect_schema,
    const ObIArray<ObString> &add_columns,
    const ObIArray<ObString> &modify_columns,
    ObSqlString &ddl_sql) const
{
  int ret = OB_SUCCESS;
  if (add_columns.empty() && modify_columns.empty()) {
    // no ddl
  } else if (OB_FAIL(ddl_sql.assign_fmt("ALTER TABLE `%.*s`.`%.*s`",
          static_cast<int>(database_name.length()), database_name.ptr(),
          static_cast<int>(table_name.length()), table_name.ptr()))) {
    LOG_WARN("assign alter prefix failed", KR(ret));
  } else {
    bool first = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < add_columns.count(); ++i) {
      const ObColumnSchemaV2 *col = expect_schema.get_column_schema(add_columns.at(i));
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ddl_sql.append_fmt("%s ADD COLUMN",
              first ? "" : ","))) {
        LOG_WARN("append add keyword failed", KR(ret));
      } else if (OB_FAIL(append_column_def_sql_(*col, ddl_sql))) {
        LOG_WARN("append add column def failed", KR(ret));
      } else {
        first = false;
      }
    }
    // We intentionally do not pre-check whether every detected schema drift is supported by
    // ALTER TABLE ... MODIFY COLUMN. The applier only emits the target column definition here;
    // unsupported changes are left to the downstream DDL layer to reject with a concrete error.
    for (int64_t i = 0; OB_SUCC(ret) && i < modify_columns.count(); ++i) {
      const ObColumnSchemaV2 *col = expect_schema.get_column_schema(modify_columns.at(i));
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ddl_sql.append_fmt("%s MODIFY COLUMN",
              first ? "" : ","))) {
        LOG_WARN("append modify keyword failed", KR(ret));
      } else if (OB_FAIL(append_column_def_sql_(*col, ddl_sql))) {
        LOG_WARN("append modify column def failed", KR(ret));
      } else {
        first = false;
      }
    }
  }
  return ret;
}

int ObUserTableSchemaApplier::execute_parallel_create_table_(
    const uint64_t tenant_id,
    const ObString &database_name,
    const ObTableSchema &table_schema,
    const bool force_local)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateTableArg arg;
  obrpc::ObCreateTableRes res;
  common::ObAddr rs_addr;
  arg.exec_tenant_id_ = tenant_id;
  arg.is_inner_ = true;
  arg.is_parallel_ = true;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.rs_mgr_) || OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr or rs rpc proxy is null", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_FAIL(arg.schema_.assign(table_schema))) {
    LOG_WARN("assign table schema failed", KR(ret), K(tenant_id), K(database_name));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rs leader failed", KR(ret));
  } else {
    // share the create-tenant deadline so a single create-table RPC may use the
    // whole remaining budget instead of a fixed rpc_timeout, consistent with the
    // other steps of ObParallelCreateTenantExecutor.
    const int64_t timeout = timeout_ctx_->get_timeout();
    arg.db_name_ = database_name;
    if (force_local || rs_addr == GCONF.self_addr_ || GCTX.in_bootstrap_) {
      if (OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("root service is null", KR(ret));
      } else if (OB_FAIL(GCTX.root_service_->parallel_create_table(arg, res))) {
        LOG_WARN("parallel create table failed", KR(ret), K(tenant_id), K(database_name),
            "table", table_schema.get_table_name_str());
      }
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).timeout(timeout).parallel_create_table(arg, res))) {
      LOG_WARN("rpc parallel create table failed", KR(ret), K(tenant_id), K(database_name),
          K(rs_addr), "table", table_schema.get_table_name_str());
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[USER_TABLE_DEF] parallel create table success", K(tenant_id), K(database_name),
          "table", table_schema.get_table_name_str(), K(res), K(rs_addr));
    }
  }
  return ret;
}

int ObUserTableSchemaApplier::execute_ddl_sql_(const uint64_t tenant_id, const ObString &ddl_sql)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  bool is_oracle_mode = false;
  common::ObAddr sql_exec_addr;
  if (ddl_sql.empty()) {
    // skip
  } else if (OB_ISNULL(GCTX.rs_mgr_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr or sql proxy is null", KR(ret), KP(GCTX.rs_mgr_), KP(sql_proxy_));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("check oracle mode failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(sql_exec_addr))) {
    LOG_WARN("get rs leader failed", KR(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id, ddl_sql,
          affected_rows, is_oracle_mode ? 1 : 0, nullptr, &sql_exec_addr))) {
    LOG_WARN("execute ddl sql failed", KR(ret), K(tenant_id), K(ddl_sql), K(sql_exec_addr));
  } else {
    FLOG_INFO("[USER_TABLE_DEF] execute ddl success", K(tenant_id), K(ddl_sql), K(affected_rows),
        K(sql_exec_addr));
  }
  return ret;
}

int ObUserTableSchemaApplier::apply_one_builtin_table_(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &guard,
    const ObUserTableBuiltinSchemaEntry &entry,
    const bool force_local)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = OB_INVALID_ID;
  bool exists = false;
  const ObTableSchema *exist_schema = NULL;
  const ObString database_name(entry.database_name_);
  const ObString table_name(entry.table_name_);
  if (OB_FAIL(get_database_id_(guard, tenant_id, database_name, database_id))) {
    LOG_WARN("get database id failed", KR(ret), K(database_name), K(table_name));
  } else if (OB_FAIL(check_table_exists_(guard, tenant_id, database_id,
          table_name, exists, exist_schema))) {
    LOG_WARN("check table exists failed", KR(ret), K(database_name), K(table_name));
  } else if (!exists) {
    HEAP_VAR(ObTableSchema, expect_schema) {
      if (OB_FAIL(build_builtin_table_schema_(tenant_id, database_id, entry, expect_schema))) {
        LOG_WARN("build builtin table schema failed", KR(ret), K(database_name), K(table_name));
      } else if (OB_FAIL(execute_parallel_create_table_(tenant_id, database_name, expect_schema,
              force_local))) {
        LOG_WARN("execute parallel create table failed", KR(ret), K(database_name), K(table_name));
      }
    }
  } else {
    HEAP_VAR(ObTableSchema, expect_schema) {
      ObSqlString ddl_sql;
      ObArray<ObString> add_columns;
      ObArray<ObString> modify_columns;
      if (OB_ISNULL(exist_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exist schema is null", KR(ret), K(database_name), K(table_name));
      } else if (OB_FAIL(build_builtin_table_schema_(tenant_id, database_id, entry, expect_schema))) {
        LOG_WARN("build builtin table schema failed", KR(ret), K(database_name), K(table_name));
      } else if (OB_FAIL(check_and_get_column_diff_(*exist_schema, expect_schema,
              add_columns, modify_columns))) {
        LOG_WARN("get column diff failed", KR(ret), K(database_name), K(table_name));
      } else if (add_columns.empty() && modify_columns.empty()) {
        FLOG_INFO("[USER_TABLE_DEF] table schema already match, skip", K(tenant_id), K(table_name));
      } else if (OB_FAIL(generate_alter_table_ddl_(database_name, table_name, expect_schema,
              add_columns, modify_columns, ddl_sql))) {
        LOG_WARN("generate alter ddl failed", KR(ret), K(database_name), K(table_name));
      } else if (OB_FAIL(execute_ddl_sql_(tenant_id, ddl_sql.string()))) {
        LOG_WARN("execute alter ddl failed", KR(ret), K(database_name), K(table_name), K(ddl_sql));
      }
    }
  }
  return ret;
}

int ObUserTableSchemaApplier::apply_builtin_user_table_schemas(const uint64_t tenant_id,
                                                               const bool force_local)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObUserTableBuiltinSchemaEntry *entries = get_user_table_builtin_schema_entries();
  const int64_t cnt = get_user_table_builtin_schema_count();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || (!is_sys_tenant(tenant_id) && !is_meta_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support apply builtin user table schemas in sys/meta tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(entries) || cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builtin user table schema empty", KR(ret), KP(entries), K(cnt));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
          tenant_id, schema_guard))) {
    LOG_WARN("get schema guard from inner table failed", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      if (OB_FAIL(apply_one_builtin_table_(tenant_id, schema_guard, entries[i], force_local))) {
        LOG_WARN("apply one builtin table failed", KR(ret), K(tenant_id), "idx", i,
            "table", entries[i].table_name_);
      }
    }
  }
  FLOG_INFO("[USER_TABLE_DEF] apply finished", KR(ret), K(tenant_id), "table_cnt", cnt);
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
