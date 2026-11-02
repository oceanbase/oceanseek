/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_lite_scan.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_define.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_original_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

namespace
{
int parse_lite_table_storage_cache_policy_type_(
    const ObString &policy_str,
    storage::ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  policy_type = storage::ObStorageCachePolicyType::MAX_POLICY;
  if (policy_str.empty()) {
    // Inner tables and DDL-not-ready tables keep MAX_POLICY; callers fall back to Full if needed.
  } else if (OB_FAIL(storage::get_storage_cache_policy_type_from_str(policy_str, policy_type))) {
    LOG_WARN("fail to parse table storage cache policy from lite row", KR(ret), K(policy_str));
  }
  return ret;
}

int extract_lite_part_storage_cache_policy_type_(
    sqlclient::ObMySQLResult &result,
    storage::ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  ObString tmp_policy;
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "part_storage_cache_policy", tmp_policy);
  if (OB_FAIL(ret)) {
  } else if (tmp_policy.empty()) {
    policy_type = storage::ObStorageCachePolicyType::NONE_POLICY;
  } else if (OB_FAIL(storage::get_storage_cache_policy_type_from_part_str(tmp_policy, policy_type))) {
    LOG_WARN("fail to parse part storage cache policy from join row", KR(ret), K(tmp_policy));
  }
  return ret;
}

ObInnerSqlWaitTypeId get_lite_part_scan_wait_type_(const ObSchemaLiteJoinSide join_side)
{
  return ObSchemaLiteJoinSide::SUBPART == join_side
      ? ObInnerSqlWaitTypeId::SCHEMA_LITE_SUBPART_SCAN
      : ObInnerSqlWaitTypeId::SCHEMA_LITE_PART_SCAN;
}
} // namespace

ObSchemaLiteTableIterator::ObSchemaLiteTableIterator()
  : is_inited_(false),
    by_simple_schemas_(false),
    path_(PathKind::INVALID),
    spec_(),
    sql_client_(NULL),
    result_(),
    string_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "SchemaLiteStr")),
    table_res_(NULL),
    table_result_(NULL),
    table_res_eof_(false),
    simple_schemas_(),
    simple_idx_(0)
{
}

bool ObSchemaLiteTableResult::has_partition() const
{
  // Prefer 0-arg members; free functions collide with member names under unqualified lookup.
  return !(VIRTUAL_TABLE == table_type_
           || is_view_table()
           || (is_index_table() && schema::is_index_local_storage(index_type_))
           || AUX_VERTIAL_PARTITION_TABLE == table_type_
           || schema::is_aux_lob_table(table_type_));
}

bool ObSchemaLiteTableResult::has_tablet() const
{
  return !(VIRTUAL_TABLE == table_type_
           || is_view_table()
           || AUX_VERTIAL_PARTITION_TABLE == table_type_
           || is_virtual_table(table_id_)
           || EXTERNAL_TABLE == table_type_
           || TMP_TABLE_ORA_SESS_V2 == table_type_
           || TMP_TABLE_ORA_TRX_V2 == table_type_);
}

bool ObSchemaLiteTableResult::is_user_hidden_table() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return mode.is_user_hidden_table();
}

bool ObSchemaLiteTableResult::is_table_with_hidden_pk_column() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return (TOM_HEAP_ORGANIZED == (enum ObTableOrganizationMode)mode.table_organization_mode_
          || (TOM_INDEX_ORGANIZED == (enum ObTableOrganizationMode)mode.table_organization_mode_
              && TOM_TABLE_WITHOUT_PK == (enum ObTablePrimaryKeyExistsMode)mode.pk_exists_));
}

bool ObSchemaLiteTableResult::is_extended_rowid_mode() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return ROWID_EXTENDED == (ObTableRowidMode)mode.rowid_mode_;
}

bool ObSchemaLiteTableResult::check_can_do_ddl() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return TABLE_STATE_NORMAL == (ObTableStateFlag)mode.state_flag_;
}

bool ObSchemaLiteTableResult::mv_container_table() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return IS_MV_CONTAINER_TABLE == (ObMVContainerTableFlag)mode.mv_container_table_flag_;
}

bool ObSchemaLiteTableResult::mv_available() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return IS_MV_AVAILABLE == (ObMVAvailableFlag)mode.mv_available_flag_;
}

void ObSchemaLiteTableIterator::reset()
{
  if (NULL != table_res_) {
    table_res_->close();
    table_res_->~ReadResult();
    ob_free(table_res_);
    table_res_ = NULL;
  }
  table_result_ = NULL;
  table_res_eof_ = false;
  simple_schemas_.reset();
  result_.reset();
  string_allocator_.reset();
  simple_idx_ = 0;
  sql_client_ = NULL;
  spec_.reset();
  path_ = PathKind::INVALID;
  by_simple_schemas_ = false;
  is_inited_ = false;
}

int ObSchemaLiteTableIterator::check_spec_(const bool for_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSchemaScanOperator::check_spec(spec_.table_spec_))) {
    LOG_WARN("invalid table_spec", KR(ret), K_(spec));
  } else if (for_count) {
    if (ObSchemaLiteAggKind::NONE == spec_.agg_kind_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("count() requires COUNT_TABLES or COUNT_PART_NUM", KR(ret), K_(spec));
    } else if (0 != spec_.part_flags_ || 0 != spec_.subpart_flags_ || 0 != spec_.column_flags_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("count aggregation must not set part/column flags", KR(ret), K_(spec));
    }
  } else if (ObSchemaLiteAggKind::NONE != spec_.agg_kind_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan_lite_tables does not accept agg_kind; use count_lite_tables", KR(ret), K_(spec));
  } else if (!(spec_.table_spec_.select_flags_ & OB_SCHEMA_SCAN_SELECT_TABLE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id must be selected", KR(ret), K_(spec));
  } else if (0 != spec_.part_flags_ || 0 != spec_.subpart_flags_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("scan_lite_tables is table-only; use scan_lite_part_rows for part/subpart",
             KR(ret), K_(spec));
  } else if (0 != spec_.column_flags_) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("column_flags not implemented yet", KR(ret), K_(spec));
  } else {
    if (NULL != spec_.caller_tag_) {
      spec_.table_spec_.caller_tag_ = spec_.caller_tag_;
    }
  }
  if (OB_SUCC(ret) && NULL != spec_.caller_tag_) {
    spec_.table_spec_.caller_tag_ = spec_.caller_tag_;
  }
  return ret;
}

int ObSchemaLiteTableIterator::init(ObISQLClient &sql_client, const ObSchemaLiteScanSpec &spec)
{
  int ret = OB_SUCCESS;
  reset();
  spec_ = spec;
  sql_client_ = &sql_client;
  by_simple_schemas_ = false;
  if (OB_FAIL(check_spec_())) {
    LOG_WARN("invalid lite scan spec", KR(ret), K(spec));
  } else if (OB_FAIL(open_sql_scan_())) {
    LOG_WARN("fail to open lite table sql scan", KR(ret), K_(spec));
  } else {
    path_ = PathKind::SQL;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

bool ObSchemaLiteTableIterator::match_scan_pred_(const ObSimpleTableSchemaV2 &schema,
                                                 const ObSchemaLiteScanSpec &spec,
                                                 const ObIArray<uint64_t> *encrypted_ts_ids)
{
  bool matched = true;
  const ObSchemaScanSpec &ts = spec.table_spec_;
  switch (ts.kind_) {
    case ObSchemaScanKind::BY_TENANT:
      matched = true;
      break;
    case ObSchemaScanKind::BY_DATABASE:
      matched = (schema.get_database_id() == ts.filter_id_);
      break;
    case ObSchemaScanKind::BY_TABLEGROUP:
      matched = (schema.get_tablegroup_id() == ts.filter_id_)
          && is_tablegroup_listed_table_type(schema.get_table_type());
      break;
    case ObSchemaScanKind::BY_TABLESPACE:
      matched = (schema.get_tablespace_id() == ts.filter_id_);
      break;
    case ObSchemaScanKind::BY_DUPLICATE_SCOPE:
      matched = (0 != static_cast<int64_t>(schema.get_duplicate_scope()));
      break;
    case ObSchemaScanKind::BY_DYNAMIC_PARTITION:
      matched = schema.with_dynamic_partition_policy();
      break;
    case ObSchemaScanKind::BY_TABLE_TYPE: {
      matched = false;
      if (NULL != ts.enum_values_) {
        const int64_t tt = static_cast<int64_t>(schema.get_table_type());
        for (int64_t i = 0; !matched && i < ts.enum_values_->count(); ++i) {
          matched = (tt == ts.enum_values_->at(i));
        }
      }
      break;
    }
    case ObSchemaScanKind::BY_INDEX_TYPE: {
      matched = false;
      if (NULL != ts.enum_values_) {
        const int64_t it = static_cast<int64_t>(schema.get_index_type());
        for (int64_t i = 0; !matched && i < ts.enum_values_->count(); ++i) {
          matched = (it == ts.enum_values_->at(i));
        }
      }
      break;
    }
    case ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE: {
      // Align with get_encrypted_table_ids / scan_encrypted_tablespace_ids:
      // tablespace.encryption_name != '' (not need_encrypt()).
      matched = false;
      if (OB_NOT_NULL(encrypted_ts_ids)) {
        const uint64_t tablespace_id = schema.get_tablespace_id();
        for (int64_t i = 0; !matched && i < encrypted_ts_ids->count(); ++i) {
          matched = (tablespace_id == encrypted_ts_ids->at(i));
        }
      }
      break;
    }
    default:
      matched = false;
      break;
  }
  return matched;
}

int ObSchemaLiteTableIterator::load_encrypted_tablespace_ids_(
    ObIArray<uint64_t> &encrypted_ts_ids) const
{
  int ret = OB_SUCCESS;
  encrypted_ts_ids.reset();
  if (OB_ISNULL(sql_client_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql_client is null", KR(ret));
  } else if (OB_FAIL(ObSchemaScanOperator::scan_encrypted_tablespace_ids(
                 *sql_client_, spec_.table_spec_.tenant_id_, encrypted_ts_ids))) {
    LOG_WARN("fail to scan encrypted tablespace ids", KR(ret), K_(spec));
  }
  return ret;
}

int ObSchemaLiteTableIterator::collect_original_schemas_(
    ObSchemaGetterGuard &guard,
    ObIArray<const ObSimpleTableSchemaV2 *> &schemas)
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObArray<const ObSimpleTableSchemaV2 *> all_schemas;
  ObArray<uint64_t> encrypted_ts_ids;
  const ObIArray<uint64_t> *encrypted_ptr = NULL;
  const uint64_t tenant_id = spec_.table_spec_.tenant_id_;
  const ObSchemaScanKind kind = spec_.table_spec_.kind_;

  if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == kind) {
    ObArray<const ObTablespaceSchema *> ts_schemas;
    if (OB_FAIL(guard.get_tablespace_schemas_in_tenant(tenant_id, ts_schemas))) {
      LOG_WARN("fail to get tablespace schemas", KR(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(ts, ts_schemas, OB_SUCC(ret)) {
        const ObTablespaceSchema *ts_schema = *ts;
        if (OB_ISNULL(ts_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL tablespace schema", KR(ret));
        } else if (!ts_schema->get_encryption_name().empty()) {
          if (OB_FAIL(encrypted_ts_ids.push_back(ts_schema->get_tablespace_id()))) {
            LOG_WARN("push back tablespace id failed", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        encrypted_ptr = &encrypted_ts_ids;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObSchemaScanKind::BY_DATABASE == kind) {
    if (OB_FAIL(guard.get_table_schemas_in_database(
            tenant_id, spec_.table_spec_.filter_id_, all_schemas))) {
      LOG_WARN("fail to get schemas in database", KR(ret), K(tenant_id), K_(spec));
    }
  } else if (ObSchemaScanKind::BY_TABLEGROUP == kind) {
    if (OB_FAIL(guard.get_table_schemas_in_tablegroup(
            tenant_id, spec_.table_spec_.filter_id_, all_schemas))) {
      LOG_WARN("fail to get schemas in tablegroup", KR(ret), K(tenant_id), K_(spec));
    }
  } else if (OB_FAIL(guard.get_table_schemas_in_tenant(tenant_id, all_schemas))) {
    LOG_WARN("fail to get schemas in tenant", KR(ret), K(tenant_id), K_(spec));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < all_schemas.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = all_schemas.at(i);
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", KR(ret), K(i));
    } else if (!match_scan_pred_(*schema, spec_, encrypted_ptr)) {
      // skip
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("fail to push schema", KR(ret), KPC(schema));
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::append_core_schemas_from_mgr(ObSchemaGetterGuard &guard)
{
  return append_core_simple_schemas_from_mgr_(guard);
}

int ObSchemaLiteTableIterator::append_core_simple_schemas_from_mgr_(ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> schemas;
  ObArray<uint64_t> encrypted_ts_ids;
  const ObIArray<uint64_t> *encrypted_ptr = NULL;
  const uint64_t tenant_id = spec_.table_spec_.tenant_id_;
  int64_t appended = 0;

  simple_schemas_.reset();
  simple_idx_ = 0;
  if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec_.table_spec_.kind_) {
    if (OB_FAIL(load_encrypted_tablespace_ids_(encrypted_ts_ids))) {
      LOG_WARN("fail to load encrypted tablespace ids for core append", KR(ret), K_(spec));
    } else {
      encrypted_ptr = &encrypted_ts_ids;
    }
  }
  // Always read resident mgr via Original strategy (cores stay in mgr even when Guard is OnDemand).
  // __all_table omits is_core_table, so matching cores are appended after the SQL cursor.
  if (FAILEDx(ObOriginalSchemaGetterGuard::instance().get_simple_table_schemas_in_tenant(
          guard, tenant_id, schemas))) {
    LOG_WARN("fail to get simple schemas from mgr for lite core append", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    const ObSimpleTableSchemaV2 *schema = schemas.at(i);
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", KR(ret), K(i));
    } else {
      const uint64_t table_id = schema->get_table_id();
      if (!is_core_table(table_id) || !match_scan_pred_(*schema, spec_, encrypted_ptr)) {
        // skip
      } else if (OB_FAIL(simple_schemas_.push_back(schema))) {
        LOG_WARN("fail to push core schema", KR(ret), K(table_id));
      } else {
        ++appended;
      }
    }
  }
  if (OB_SUCC(ret) && appended > 0) {
    LOG_TRACE("lite scan appended core schemas from mgr", K_(spec), K(appended));
  }
  return ret;
}

int ObSchemaLiteTableIterator::init(ObSchemaGetterGuard &guard, const ObSchemaLiteScanSpec &spec)
{
  int ret = OB_SUCCESS;
  if (guard.is_on_demand_schema_mode()) {
    ObISQLClient *sql_client = NULL;
    if (OB_FAIL(guard.check_can_full_scan(spec.table_spec_.tenant_id_))) {
      LOG_WARN("full scan not allowed", KR(ret), K(spec));
    } else if (OB_FAIL(guard.prepare_schema_scan_sql_client_(
                   spec.table_spec_.tenant_id_, sql_client))) {
      LOG_WARN("fail to get schema scan sql client", KR(ret), K(spec));
    } else if (OB_ISNULL(sql_client)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema scan sql client is null", KR(ret), K(spec));
    } else {
      reset();
      spec_ = spec;
      sql_client_ = sql_client;
      by_simple_schemas_ = false;
    }
    // Append cores before opening the SQL cursor. BY_ENCRYPTED_TABLESPACE may
    // need one auxiliary query, which cannot run beside an active STRONG cursor.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_spec_())) {
      LOG_WARN("invalid lite scan spec", KR(ret), K(spec));
    } else if (OB_FAIL(append_core_simple_schemas_from_mgr_(guard))) {
      LOG_WARN("fail to append core schemas from mgr", KR(ret), K(spec));
    } else if (OB_FAIL(open_sql_scan_())) {
      LOG_WARN("fail to open on-demand lite scan", KR(ret), K(spec));
    } else {
      path_ = PathKind::SQL;
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  } else {
    // Original: fill from resident Simple schemas.
    ObArray<const ObSimpleTableSchemaV2 *> schemas;
    reset();
    spec_ = spec;
    if (OB_FAIL(check_spec_())) {
      LOG_WARN("invalid lite scan spec", KR(ret), K(spec));
    } else if (OB_FAIL(collect_original_schemas_(guard, schemas))) {
      LOG_WARN("fail to collect original schemas", KR(ret), K(spec));
    } else {
      by_simple_schemas_ = true;
      path_ = PathKind::SIMPLE_SCHEMA;
      if (OB_FAIL(simple_schemas_.assign(schemas))) {
        LOG_WARN("fail to assign schemas", KR(ret), "cnt", schemas.count());
      } else {
        simple_idx_ = 0;
        is_inited_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::init_by_simple_schemas(
    const ObIArray<const ObSimpleTableSchemaV2 *> &schemas,
    const ObSchemaLiteScanSpec &spec)
{
  int ret = OB_SUCCESS;
  reset();
  spec_ = spec;
  by_simple_schemas_ = true;
  path_ = PathKind::SIMPLE_SCHEMA;
  // Original mock: still validate flags.
  if (OB_FAIL(check_spec_())) {
    LOG_WARN("invalid lite scan spec", KR(ret), K(spec));
  } else if (OB_FAIL(simple_schemas_.assign(schemas))) {
    LOG_WARN("fail to assign schemas", KR(ret), "cnt", schemas.count());
  } else {
    simple_idx_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSchemaLiteTableIterator::count_from_simple_schemas_(
    const ObIArray<const ObSimpleTableSchemaV2 *> &schemas,
    int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  if (ObSchemaLiteAggKind::COUNT_TABLES == spec_.agg_kind_) {
    count = schemas.count();
  } else if (ObSchemaLiteAggKind::COUNT_PART_NUM == spec_.agg_kind_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *schema = schemas.at(i);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", KR(ret), K(i));
      } else {
        // Align get_tablet_count: get_all_part_num(); failed / incomplete contributes 0.
        const int64_t part_num = schema->get_all_part_num();
        if (part_num > 0) {
          count += part_num;
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported agg_kind", KR(ret), K_(spec));
  }
  return ret;
}

int ObSchemaLiteTableIterator::build_count_table_where_(ObSqlString &where_sql,
                                                        const char *table_alias)
{
  int ret = OB_SUCCESS;
  where_sql.reset();
  const bool with_alias = OB_NOT_NULL(table_alias) && '\0' != table_alias[0];
  const char *a = with_alias ? table_alias : "";
  const char *d = with_alias ? "." : "";
  if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec_.table_spec_.kind_) {
    ObArray<uint64_t> tablespace_ids;
    ObSqlString in_list;
    if (OB_FAIL(load_encrypted_tablespace_ids_(tablespace_ids))) {
      LOG_WARN("fail to load encrypted tablespace ids", KR(ret), K_(spec));
    } else if (tablespace_ids.empty()) {
      if (OB_FAIL(where_sql.assign_fmt("%s%stenant_id = 0 AND 1 = 0", a, d))) {
        LOG_WARN("fail to assign empty encrypted where", KR(ret));
      }
    } else if (OB_FAIL(ObSchemaScanOperator::build_uint_in_list(tablespace_ids, in_list))) {
      LOG_WARN("fail to build tablespace in list", KR(ret));
    } else if (OB_FAIL(where_sql.assign_fmt(
            "%s%stenant_id = 0 AND %s%stablespace_id IN %s",
            a, d, a, d, in_list.ptr()))) {
      LOG_WARN("fail to assign encrypted where", KR(ret));
    }
  } else if (!with_alias) {
    // COUNT_TABLES: single-table, reuse generic unprefixed where.
    if (OB_FAIL(ObSchemaScanOperator::build_where_clause(spec_.table_spec_, where_sql))) {
      LOG_WARN("fail to build where", KR(ret), K_(spec));
    }
  } else {
    // COUNT_PART_NUM JOIN: qualify __all_table columns locally; do not extend
    // ObSchemaScanOperator::build_where_clause (single-table helper).
    const ObSchemaScanSpec &ts = spec_.table_spec_;
    if (OB_FAIL(where_sql.assign_fmt("%s.tenant_id = 0", a))) {
      LOG_WARN("fail to assign aliased where", KR(ret));
    } else {
      switch (ts.kind_) {
        case ObSchemaScanKind::BY_TABLE_TYPE: {
          if (OB_ISNULL(ts.enum_values_) || ts.enum_values_->empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("empty table_type enum", KR(ret), K_(spec));
          } else if (OB_FAIL(where_sql.append_fmt(" AND %s.table_type IN (", a))) {
            LOG_WARN("fail to append table_type predicate", KR(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < ts.enum_values_->count(); ++i) {
              if (0 != i && OB_FAIL(where_sql.append(","))) {
                LOG_WARN("fail to append comma", KR(ret));
              } else if (OB_FAIL(where_sql.append_fmt("%ld", ts.enum_values_->at(i)))) {
                LOG_WARN("fail to append table_type", KR(ret), K(i));
              }
            }
            if (FAILEDx(where_sql.append(")"))) {
              LOG_WARN("fail to append table_type in end", KR(ret));
            }
          }
          break;
        }
        case ObSchemaScanKind::BY_DUPLICATE_SCOPE: {
          if (OB_FAIL(where_sql.append_fmt(" AND %s.duplicate_scope != 0", a))) {
            LOG_WARN("fail to append duplicate_scope predicate", KR(ret));
          }
          break;
        }
        case ObSchemaScanKind::BY_DYNAMIC_PARTITION: {
          if (OB_FAIL(where_sql.append_fmt(" AND %s.has_dynamic_partition_policy = 1", a))) {
            LOG_WARN("fail to append dynamic partition predicate", KR(ret));
          }
          break;
        }
        case ObSchemaScanKind::BY_INDEX_TYPE: {
          if (OB_ISNULL(ts.enum_values_) || ts.enum_values_->empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("empty index_type enum", KR(ret), K_(spec));
          } else if (OB_FAIL(where_sql.append_fmt(" AND %s.index_type IN (", a))) {
            LOG_WARN("fail to append index_type predicate", KR(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < ts.enum_values_->count(); ++i) {
              if (0 != i && OB_FAIL(where_sql.append(","))) {
                LOG_WARN("fail to append comma", KR(ret));
              } else if (OB_FAIL(where_sql.append_fmt("%ld", ts.enum_values_->at(i)))) {
                LOG_WARN("fail to append index_type", KR(ret), K(i));
              }
            }
            if (FAILEDx(where_sql.append(")"))) {
              LOG_WARN("fail to append index_type in end", KR(ret));
            }
          }
          break;
        }
        case ObSchemaScanKind::BY_TABLEGROUP: {
          if (OB_FAIL(where_sql.append_fmt(" AND %s.tablegroup_id = %lu", a, ts.filter_id_))) {
            LOG_WARN("fail to append tablegroup predicate", KR(ret));
          }
          break;
        }
        case ObSchemaScanKind::BY_TABLESPACE: {
          if (OB_FAIL(where_sql.append_fmt(" AND %s.tablespace_id = %lu", a, ts.filter_id_))) {
            LOG_WARN("fail to append tablespace predicate", KR(ret));
          }
          break;
        }
        case ObSchemaScanKind::BY_DATABASE: {
          if (OB_FAIL(where_sql.append_fmt(" AND %s.database_id = %lu", a, ts.filter_id_))) {
            LOG_WARN("fail to append database predicate", KR(ret));
          }
          break;
        }
        case ObSchemaScanKind::BY_TENANT:
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unknown kind for aliased count where", KR(ret), K_(spec));
          break;
      }
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::exec_count_sql_(const ObSqlString &sql, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql_client is null", KR(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client_->read(res, spec_.table_spec_.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute count sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to fetch count row", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
      }
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::count_from_inner_sql_(int64_t &count)
{
  int ret = OB_SUCCESS;
  ObASHSetInnerSqlWaitGuard ash_guard(ObInnerSqlWaitTypeId::SCHEMA_LITE_COUNT);
  count = 0;
  ObSqlString where_sql;
  ObSqlString sql;
  if (ObSchemaLiteAggKind::COUNT_TABLES == spec_.agg_kind_) {
    if (OB_FAIL(build_count_table_where_(where_sql))) {
      LOG_WARN("fail to build count where", KR(ret), K_(spec));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT COUNT(*) AS cnt FROM %s WHERE %s",
            OB_ALL_TABLE_TNAME, where_sql.ptr()))) {
      LOG_WARN("fail to assign table count sql", KR(ret));
    } else if (OB_FAIL(exec_count_sql_(sql, count))) {
      LOG_WARN("fail to exec table count", KR(ret), K(sql));
    }
  } else if (ObSchemaLiteAggKind::COUNT_PART_NUM == spec_.agg_kind_) {
    int64_t level0_cnt = 0;
    int64_t level1_cnt = 0;
    int64_t level2_cnt = 0;
    // Align ObPartitionSchema::get_all_part_num() on complete schemas:
    //   A: no normal __all_part row -> 1 (level0, including view/virtual)
    //   B: normal __all_part rows of tables with no normal __all_sub_part
    //   C: normal __all_sub_part rows
    // hidden split/merge rows live in hidden_* arrays and are excluded by get_all_part_num().
    // get_all_part_num() uses part_level_; if part_level is already set but not persisted yet,
    // the two paths（OnDemand/Original） may diverge — accepted for timeout.
    const int normal_part = static_cast<int>(PARTITION_TYPE_NORMAL);
    if (OB_FAIL(build_count_table_where_(where_sql, "t"))) {
      LOG_WARN("fail to build count where", KR(ret), K_(spec));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT COUNT(*) AS cnt FROM %s t WHERE %s AND NOT EXISTS "
            "(SELECT 1 FROM %s p WHERE p.tenant_id = t.tenant_id AND p.table_id = t.table_id "
            "AND p.partition_type = %d)",
            OB_ALL_TABLE_TNAME, where_sql.ptr(), OB_ALL_PART_TNAME, normal_part))) {
      LOG_WARN("fail to assign level0 count sql", KR(ret));
    } else if (OB_FAIL(exec_count_sql_(sql, level0_cnt))) {
      LOG_WARN("fail to exec level0 count", KR(ret), K(sql));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT COUNT(*) AS cnt FROM %s p INNER JOIN %s t "
            "ON p.tenant_id = t.tenant_id AND p.table_id = t.table_id "
            "WHERE %s AND p.partition_type = %d AND NOT EXISTS "
            "(SELECT 1 FROM %s s WHERE s.tenant_id = p.tenant_id AND s.table_id = p.table_id "
            "AND s.partition_type = %d)",
            OB_ALL_PART_TNAME, OB_ALL_TABLE_TNAME, where_sql.ptr(), normal_part,
            OB_ALL_SUB_PART_TNAME, normal_part))) {
      LOG_WARN("fail to assign level1 count sql", KR(ret));
    } else if (OB_FAIL(exec_count_sql_(sql, level1_cnt))) {
      LOG_WARN("fail to exec level1 count", KR(ret), K(sql));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT COUNT(*) AS cnt FROM %s s INNER JOIN %s t "
            "ON s.tenant_id = t.tenant_id AND s.table_id = t.table_id "
            "WHERE %s AND s.partition_type = %d",
            OB_ALL_SUB_PART_TNAME, OB_ALL_TABLE_TNAME, where_sql.ptr(), normal_part))) {
      LOG_WARN("fail to assign level2 count sql", KR(ret));
    } else if (OB_FAIL(exec_count_sql_(sql, level2_cnt))) {
      LOG_WARN("fail to exec level2 count", KR(ret), K(sql));
    } else {
      count = level0_cnt + level1_cnt + level2_cnt;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported agg_kind", KR(ret), K_(spec));
  }
  return ret;
}

int ObSchemaLiteTableIterator::count_core_part_num_(ObSchemaGetterGuard &guard, int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t core_cnt = 0;
  // Reuse append_core path: matching cores are appended from resident mgr.
  if (OB_FAIL(append_core_simple_schemas_from_mgr_(guard))) {
    LOG_WARN("fail to append core schemas for count", KR(ret), K_(spec));
  } else if (OB_FAIL(count_from_simple_schemas_(simple_schemas_, core_cnt))) {
    LOG_WARN("fail to count core schemas", KR(ret), K_(spec));
  } else {
    count += core_cnt;
  }
  return ret;
}

int ObSchemaLiteTableIterator::count(ObSchemaGetterGuard &guard,
                                     const ObSchemaLiteScanSpec &spec,
                                     int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  reset();
  spec_ = spec;
  if (OB_FAIL(check_spec_(true /*for_count*/))) {
    LOG_WARN("invalid lite count spec", KR(ret), K(spec));
  } else if (!guard.is_on_demand_schema_mode()) {
    ObArray<const ObSimpleTableSchemaV2 *> schemas;
    if (OB_FAIL(collect_original_schemas_(guard, schemas))) {
      LOG_WARN("fail to collect original schemas for count", KR(ret), K(spec));
    } else if (OB_FAIL(count_from_simple_schemas_(schemas, count))) {
      LOG_WARN("fail to count original schemas", KR(ret), K(spec));
    }
  } else {
    if (OB_FAIL(guard.check_can_full_scan(spec.table_spec_.tenant_id_))) {
      LOG_WARN("full scan not allowed", KR(ret), K(spec));
    } else if (OB_FAIL(guard.prepare_schema_scan_sql_client_(
                   spec.table_spec_.tenant_id_, sql_client_))) {
      LOG_WARN("fail to get lite count sql client", KR(ret), K(spec));
    } else if (OB_ISNULL(sql_client_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite count sql client is null", KR(ret), K(spec));
    } else if (OB_FAIL(count_from_inner_sql_(count))) {
      LOG_WARN("fail to count from inner sql", KR(ret), K(spec));
    } else if (OB_FAIL(count_core_part_num_(guard, count))) {
      LOG_WARN("fail to add core count", KR(ret), K(spec));
    }
  }
  reset();
  return ret;
}

int ObSchemaLiteTableIterator::build_table_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString select_list;
  ObSqlString where_sql;
  sql.reset();
  if (OB_FAIL(ObSchemaScanOperator::build_select_list(spec_.table_spec_.select_flags_, select_list))) {
    LOG_WARN("fail to build select list", KR(ret), K_(spec));
  } else if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec_.table_spec_.kind_) {
    // Two-step like ObSchemaScanOperator::scan_table_ids: encrypted tablespaces first.
    ObArray<uint64_t> tablespace_ids;
    ObSqlString in_list;
    if (OB_FAIL(load_encrypted_tablespace_ids_(tablespace_ids))) {
      LOG_WARN("fail to load encrypted tablespace ids", KR(ret), K_(spec));
    } else if (tablespace_ids.empty()) {
      if (OB_FAIL(sql.assign_fmt(
              "SELECT %s FROM %s WHERE 1 = 0 ORDER BY table_id ASC",
              select_list.ptr(), OB_ALL_TABLE_TNAME))) {
        LOG_WARN("fail to assign empty encrypted table sql", KR(ret));
      }
    } else if (OB_FAIL(ObSchemaScanOperator::build_uint_in_list(tablespace_ids, in_list))) {
      LOG_WARN("fail to build tablespace in list", KR(ret));
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT %s FROM %s WHERE tenant_id = 0 AND tablespace_id IN %s "
            "ORDER BY table_id ASC",
            select_list.ptr(), OB_ALL_TABLE_TNAME, in_list.ptr()))) {
      LOG_WARN("fail to assign encrypted table sql", KR(ret));
    }
  } else if (OB_FAIL(ObSchemaScanOperator::build_where_clause(spec_.table_spec_, where_sql))) {
    LOG_WARN("fail to build where", KR(ret), K_(spec));
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s WHERE %s ORDER BY table_id ASC",
          select_list.ptr(), OB_ALL_TABLE_TNAME, where_sql.ptr()))) {
    LOG_WARN("fail to assign table sql", KR(ret));
  }
  return ret;
}

int ObSchemaLiteTableIterator::open_sql_scan_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql_client is null", KR(ret));
  } else {
    ObASHSetInnerSqlWaitGuard ash_guard(ObInnerSqlWaitTypeId::SCHEMA_LITE_TABLE_SCAN);
    ObSqlString sql;
    void *buf = ob_malloc(sizeof(ObISQLClient::ReadResult),
                          ObMemAttr(OB_SERVER_TENANT_ID, "SchemaLiteRes"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ReadResult", KR(ret));
    } else if (FALSE_IT(table_res_ = new (buf) ObISQLClient::ReadResult())) {
    } else if (OB_FAIL(build_table_sql_(sql))) {
      LOG_WARN("fail to build table sql", KR(ret));
    } else if (OB_FAIL(sql_client_->read(*table_res_, spec_.table_spec_.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute table sql", KR(ret), K(sql));
    } else if (OB_ISNULL(table_result_ = table_res_->get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table result is null", KR(ret));
    } else {
      table_res_eof_ = false;
      LOG_INFO("schema lite table sql scan opened", K_(spec), K(sql));
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::assign_table_fields_from_lite_row_(const ObSchemaTableLiteRow &row)
{
  int ret = OB_SUCCESS;
  result_.table_id_ = row.table_id_;
  result_.table_type_ = row.table_type_;
  result_.index_type_ = row.index_type_;
  result_.tablegroup_id_ = row.tablegroup_id_;
  result_.database_id_ = row.database_id_;
  result_.data_table_id_ = row.data_table_id_;
  result_.tablespace_id_ = row.tablespace_id_;
  result_.duplicate_scope_ = row.duplicate_scope_;
  result_.has_dynamic_partition_policy_ = row.has_dynamic_partition_policy_;
  result_.part_level_ = row.part_level_;
  result_.tablet_id_ = row.tablet_id_;
  result_.session_id_ = row.session_id_;
  result_.index_status_ = row.index_status_;
  result_.schema_version_ = row.schema_version_;
  result_.object_status_ = row.object_status_;
  result_.table_mode_ = row.table_mode_;
  result_.duplicate_read_consistency_ = row.duplicate_read_consistency_;
  // Strings already owned by string_allocator_ for the current row.
  result_.table_name_ = row.table_name_;
  result_.encryption_ = row.encryption_;
  result_.storage_cache_policy_ = row.storage_cache_policy_;
  result_.ttl_definition_ = row.ttl_definition_;
  result_.ttl_flag_ = row.ttl_flag_;
  result_.kv_attributes_ = row.kv_attributes_;
  result_.parts_.reuse();
  if (FAILEDx(parse_lite_table_storage_cache_policy_type_(
          result_.storage_cache_policy_, result_.storage_cache_policy_type_))) {
    LOG_WARN("fail to parse storage cache policy type from lite row", KR(ret),
             K_(result));
  }
  return ret;
}

int ObSchemaLiteTableIterator::assign_table_fields_from_simple_(const ObSimpleTableSchemaV2 &schema)
{
  int ret = OB_SUCCESS;
  result_.table_id_ = schema.get_table_id();
  result_.table_type_ = schema.get_table_type();
  result_.index_type_ = schema.get_index_type();
  result_.tablegroup_id_ = schema.get_tablegroup_id();
  result_.database_id_ = schema.get_database_id();
  result_.data_table_id_ = schema.get_data_table_id();
  result_.tablespace_id_ = schema.get_tablespace_id();
  result_.duplicate_scope_ = static_cast<int64_t>(schema.get_duplicate_scope());
  result_.has_dynamic_partition_policy_ = schema.with_dynamic_partition_policy();
  result_.part_level_ = schema.get_part_level();
  result_.tablet_id_ = schema.get_tablet_id();
  result_.session_id_ = schema.get_session_id();
  result_.index_status_ = schema.get_index_status();
  result_.schema_version_ = schema.get_schema_version();
  result_.object_status_ = schema.get_object_status();
  result_.table_mode_ = schema.get_table_mode_struct().mode_;
  result_.duplicate_read_consistency_ =
      static_cast<int64_t>(schema.get_duplicate_read_consistency());
  result_.table_name_.reset();
  result_.encryption_.reset();
  result_.storage_cache_policy_.reset();
  result_.storage_cache_policy_type_ = schema.get_storage_cache_policy_type();
  result_.ttl_definition_.reset();
  result_.ttl_flag_.reset();
  result_.kv_attributes_.reset();
  // Only deep-copy projected string fields; callers that need them must set select_flags_.
  // storage_cache_policy / ttl_* / kv_attributes live on Full ObTableSchema; Original
  // Simple path leaves the varchar empty but fills storage_cache_policy_type_ from the enum.
  const uint64_t flags = spec_.table_spec_.select_flags_;
  if ((flags & OB_SCHEMA_SCAN_SELECT_TABLE_NAME)
      && OB_FAIL(ob_write_string(string_allocator_, schema.get_table_name_str(),
                                 result_.table_name_))) {
    LOG_WARN("fail to copy table_name from simple", KR(ret));
  } else if ((flags & OB_SCHEMA_SCAN_SELECT_ENCRYPTION)
             && OB_FAIL(ob_write_string(string_allocator_, schema.get_encryption_str(),
                                        result_.encryption_))) {
    LOG_WARN("fail to copy encryption from simple", KR(ret));
  } else {
    result_.parts_.reuse();
  }
  return ret;
}

int ObSchemaLiteTableIterator::get_next_from_sql_()
{
  int ret = OB_SUCCESS;
  result_.reuse();
  string_allocator_.reuse();
  if (OB_ISNULL(table_result_) || table_res_eof_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(table_result_->next())) {
    if (OB_ITER_END == ret) {
      table_res_eof_ = true;
      // Release the InnerSQL connection reference as soon as the cursor is
      // drained, so another STRONG scan can reuse the same transaction.
      table_res_->reset();
      table_result_ = NULL;
    } else {
      LOG_WARN("fail to next table row", KR(ret));
    }
  } else {
    ObSchemaTableLiteRow row;
    if (OB_FAIL(ObSchemaScanOperator::fill_lite_row(
            *table_result_, spec_.table_spec_.select_flags_, row, &string_allocator_))) {
      LOG_WARN("fail to fill lite row", KR(ret));
    } else if (OB_FAIL(assign_table_fields_from_lite_row_(row))) {
      LOG_WARN("fail to assign table fields", KR(ret));
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::get_next_from_simple_schemas_()
{
  int ret = OB_SUCCESS;
  result_.reuse();
  // Per-row reuse: Original/SIMPLE_SCHEMA and SQL+core paths all copy
  // strings into string_allocator_; get_next() result is only valid until the
  // next call, so drop the previous row's deep copies here.
  string_allocator_.reuse();
  if (simple_idx_ >= simple_schemas_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObSimpleTableSchemaV2 *schema = simple_schemas_.at(simple_idx_);
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", KR(ret), K_(simple_idx));
    } else if (OB_FAIL(assign_table_fields_from_simple_(*schema))) {
      LOG_WARN("fail to assign from simple", KR(ret), KPC(schema));
    } else {
      ++simple_idx_;
    }
  }
  return ret;
}

int ObSchemaLiteTableIterator::get_next(const ObSchemaLiteTableResult *&result)
{
  int ret = OB_SUCCESS;
  result = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lite iterator not init", KR(ret));
  } else if (PathKind::SQL == path_) {
    if (!table_res_eof_) {
      if (OB_FAIL(get_next_from_sql_())) {
        if (OB_ITER_END == ret && simple_idx_ < simple_schemas_.count()) {
          ret = get_next_from_simple_schemas_();
          if (OB_FAIL(ret) && OB_ITER_END != ret) {
            LOG_WARN("fail get_next core simple after sql", KR(ret));
          }
        } else if (OB_ITER_END != ret) {
          LOG_WARN("fail get_next sql", KR(ret));
        }
      }
    } else if (simple_idx_ < simple_schemas_.count()) {
      if (OB_FAIL(get_next_from_simple_schemas_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail get_next core simple after sql eof", KR(ret));
        }
      }
    } else {
      ret = OB_ITER_END;
    }
  } else if (PathKind::SIMPLE_SCHEMA == path_) {
    if (OB_FAIL(get_next_from_simple_schemas_())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail get_next simple", KR(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path", KR(ret), "path", static_cast<int>(path_));
  }
  if (OB_SUCC(ret)) {
    result = &result_;
  }
  return ret;
}

bool ObSchemaLiteJoinRow::has_partition() const
{
  return !(VIRTUAL_TABLE == table_type_
           || is_view_table()
           || (is_index_table() && schema::is_index_local_storage(index_type_))
           || AUX_VERTIAL_PARTITION_TABLE == table_type_
           || schema::is_aux_lob_table(table_type_));
}

bool ObSchemaLiteJoinRow::has_tablet() const
{
  return !(VIRTUAL_TABLE == table_type_
           || is_view_table()
           || AUX_VERTIAL_PARTITION_TABLE == table_type_
           || is_virtual_table(table_id_)
           || EXTERNAL_TABLE == table_type_
           || TMP_TABLE_ORA_SESS_V2 == table_type_
           || TMP_TABLE_ORA_TRX_V2 == table_type_);
}

bool ObSchemaLiteJoinRow::is_user_hidden_table() const
{
  ObTableMode mode;
  mode.mode_ = table_mode_;
  return mode.is_user_hidden_table();
}

ObSchemaLitePartJoinIterator::ObSchemaLitePartJoinIterator()
  : is_inited_(false),
    by_simple_schemas_(false),
    path_(PathKind::INVALID),
    join_side_(ObSchemaLiteJoinSide::PART),
    spec_(),
    sql_client_(NULL),
    row_(),
    string_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "SchemaLiteJn")),
    sql_res_(NULL),
    sql_result_(NULL),
    sql_res_eof_(false),
    simple_schemas_(),
    schema_idx_(0),
    part_pos_(0),
    sub_pos_(0)
{
}

void ObSchemaLitePartJoinIterator::reset()
{
  if (NULL != sql_res_) {
    sql_res_->close();
    sql_res_->~ReadResult();
    ob_free(sql_res_);
    sql_res_ = NULL;
  }
  sql_result_ = NULL;
  sql_res_eof_ = false;
  simple_schemas_.reset();
  row_.reset();
  string_allocator_.reset();
  schema_idx_ = 0;
  part_pos_ = 0;
  sub_pos_ = 0;
  sql_client_ = NULL;
  spec_.reset();
  path_ = PathKind::INVALID;
  join_side_ = ObSchemaLiteJoinSide::PART;
  by_simple_schemas_ = false;
  is_inited_ = false;
}

int ObSchemaLitePartJoinIterator::check_spec_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSchemaScanOperator::check_spec(spec_.table_spec_))) {
    LOG_WARN("invalid table_spec", KR(ret), K_(spec));
  } else if (ObSchemaLiteAggKind::NONE != spec_.agg_kind_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan_lite_part_rows does not accept agg_kind", KR(ret), K_(spec));
  } else if (!(spec_.table_spec_.select_flags_ & OB_SCHEMA_SCAN_SELECT_TABLE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id must be selected", KR(ret), K_(spec));
  } else if (0 != spec_.column_flags_) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("column_flags not implemented yet", KR(ret), K_(spec));
  } else if (ObSchemaLiteJoinSide::PART == join_side_) {
    if (0 == spec_.part_flags_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("PART join requires part_flags", KR(ret), K_(spec));
    }
  } else if (ObSchemaLiteJoinSide::SUBPART == join_side_) {
    if (0 == spec_.subpart_flags_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("SUBPART join requires subpart_flags", KR(ret), K_(spec));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid join_side", KR(ret), "join_side", static_cast<int>(join_side_));
  }
  if (OB_SUCC(ret) && NULL != spec_.caller_tag_) {
    spec_.table_spec_.caller_tag_ = spec_.caller_tag_;
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::build_join_table_where_(ObSqlString &where_sql)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteTableIterator helper;
  helper.spec_ = spec_;
  helper.sql_client_ = sql_client_;
  if (OB_FAIL(helper.build_count_table_where_(where_sql, "t"))) {
    LOG_WARN("fail to build aliased table where", KR(ret), K_(spec));
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::build_join_select_list_(ObSqlString &select_list)
{
  int ret = OB_SUCCESS;
  select_list.reset();
  bool first = true;
  const uint64_t flags = spec_.table_spec_.select_flags_;
#define APPEND_T(col, flag)                                            \
  do {                                                                 \
    if (OB_SUCC(ret) && (flags & (flag))) {                            \
      if (!first && OB_FAIL(select_list.append(", "))) {               \
        LOG_WARN("fail to append comma", KR(ret));                     \
      } else if (OB_FAIL(select_list.append("t." col " AS " col))) {   \
        LOG_WARN("fail to append table col", KR(ret), K(col));         \
      } else {                                                         \
        first = false;                                                 \
      }                                                                \
    }                                                                  \
  } while (0)
  APPEND_T("table_id", OB_SCHEMA_SCAN_SELECT_TABLE_ID);
  APPEND_T("table_type", OB_SCHEMA_SCAN_SELECT_TABLE_TYPE);
  APPEND_T("index_type", OB_SCHEMA_SCAN_SELECT_INDEX_TYPE);
  APPEND_T("tablegroup_id", OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID);
  APPEND_T("database_id", OB_SCHEMA_SCAN_SELECT_DATABASE_ID);
  APPEND_T("data_table_id", OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID);
  if (OB_SUCC(ret) && (flags & OB_SCHEMA_SCAN_SELECT_PART_LEVEL)) {
    if (!first && OB_FAIL(select_list.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    } else if (OB_FAIL(select_list.append_fmt(
                   "CASE WHEN t.part_level = %d AND t.part_num = 1 "
                   "AND t.part_func_type = %d "
                   "AND (t.part_func_expr IS NULL OR t.part_func_expr = '') "
                   "THEN %d ELSE t.part_level END AS part_level",
                   static_cast<int>(PARTITION_LEVEL_ONE),
                   static_cast<int>(PARTITION_FUNC_TYPE_HASH),
                   static_cast<int>(PARTITION_LEVEL_ZERO)))) {
      LOG_WARN("fail to append normalized part_level", KR(ret));
    } else {
      first = false;
    }
  }
  APPEND_T("tablespace_id", OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID);
  APPEND_T("duplicate_scope", OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE);
  APPEND_T("has_dynamic_partition_policy", OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY);
  APPEND_T("table_name", OB_SCHEMA_SCAN_SELECT_TABLE_NAME);
  APPEND_T("session_id", OB_SCHEMA_SCAN_SELECT_SESSION_ID);
  APPEND_T("encryption", OB_SCHEMA_SCAN_SELECT_ENCRYPTION);
  APPEND_T("index_status", OB_SCHEMA_SCAN_SELECT_INDEX_STATUS);
  APPEND_T("schema_version", OB_SCHEMA_SCAN_SELECT_SCHEMA_VERSION);
  APPEND_T("object_status", OB_SCHEMA_SCAN_SELECT_OBJECT_STATUS);
  APPEND_T("table_mode", OB_SCHEMA_SCAN_SELECT_TABLE_MODE);
  APPEND_T("duplicate_read_consistency", OB_SCHEMA_SCAN_SELECT_DUPLICATE_READ_CONSISTENCY);
  APPEND_T("storage_cache_policy", OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY);
  APPEND_T("ttl_definition", OB_SCHEMA_SCAN_SELECT_TTL_DEFINITION);
  APPEND_T("ttl_flag", OB_SCHEMA_SCAN_SELECT_TTL_FLAG);
  APPEND_T("kv_attributes", OB_SCHEMA_SCAN_SELECT_KV_ATTRIBUTES);
#undef APPEND_T
  if (OB_SUCC(ret) && (flags & OB_SCHEMA_SCAN_SELECT_TABLET_ID)) {
    if (!first && OB_FAIL(select_list.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    } else if (OB_FAIL(select_list.append("t.tablet_id AS table_tablet_id"))) {
      LOG_WARN("fail to append table_tablet_id", KR(ret));
    } else {
      first = false;
    }
  }
#define APPEND_P(col, flag)                                            \
  do {                                                                 \
    if (OB_SUCC(ret) && (0 != (spec_.part_flags_ & (flag)))) {         \
      if (!first && OB_FAIL(select_list.append(", "))) {               \
        LOG_WARN("fail to append comma", KR(ret));                     \
      } else if (OB_FAIL(select_list.append("p." col " AS " col))) {   \
        LOG_WARN("fail to append part col", KR(ret), K(col));          \
      } else {                                                         \
        first = false;                                                 \
      }                                                                \
    }                                                                  \
  } while (0)
#define APPEND_S(col, flag)                                            \
  do {                                                                 \
    if (OB_SUCC(ret) && (0 != (spec_.subpart_flags_ & (flag)))) {      \
      if (!first && OB_FAIL(select_list.append(", "))) {               \
        LOG_WARN("fail to append comma", KR(ret));                     \
      } else if (OB_FAIL(select_list.append("s." col " AS " col))) {   \
        LOG_WARN("fail to append subpart col", KR(ret), K(col));       \
      } else {                                                         \
        first = false;                                                 \
      }                                                                \
    }                                                                  \
  } while (0)
#define APPEND_P_AS(col, alias, flag)                                  \
  do {                                                                 \
    if (OB_SUCC(ret) && (0 != (spec_.part_flags_ & (flag)))) {         \
      if (!first && OB_FAIL(select_list.append(", "))) {               \
        LOG_WARN("fail to append comma", KR(ret));                     \
      } else if (OB_FAIL(select_list.append("p." col " AS " alias))) { \
        LOG_WARN("fail to append part col alias", KR(ret), K(col));   \
      } else {                                                         \
        first = false;                                                 \
      }                                                                \
    }                                                                  \
  } while (0)
#define APPEND_S_AS(col, alias, flag)                                  \
  do {                                                                 \
    if (OB_SUCC(ret) && (0 != (spec_.subpart_flags_ & (flag)))) {      \
      if (!first && OB_FAIL(select_list.append(", "))) {               \
        LOG_WARN("fail to append comma", KR(ret));                     \
      } else if (OB_FAIL(select_list.append("s." col " AS " alias))) { \
        LOG_WARN("fail to append subpart col alias", KR(ret), K(col));\
      } else {                                                         \
        first = false;                                                 \
      }                                                                \
    }                                                                  \
  } while (0)
  if (OB_FAIL(ret)) {
  } else if (ObSchemaLiteJoinSide::PART == join_side_) {
    APPEND_P("part_id", OB_SCHEMA_LITE_PART_PART_ID);
    APPEND_P("part_idx", OB_SCHEMA_LITE_PART_PART_IDX);
    APPEND_P("tablet_id", OB_SCHEMA_LITE_PART_TABLET_ID);
    APPEND_P("part_name", OB_SCHEMA_LITE_PART_PART_NAME);
    APPEND_P_AS("schema_version", "part_schema_version",
                OB_SCHEMA_LITE_PART_SCHEMA_VERSION);
    APPEND_P_AS("storage_cache_policy", "part_storage_cache_policy",
                OB_SCHEMA_LITE_PART_STORAGE_CACHE_POLICY);
  } else {
    APPEND_S("part_id", OB_SCHEMA_LITE_SUBPART_PART_ID);
    APPEND_S("sub_part_id", OB_SCHEMA_LITE_SUBPART_SUB_PART_ID);
    APPEND_S("sub_part_idx", OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX);
    APPEND_S("tablet_id", OB_SCHEMA_LITE_SUBPART_TABLET_ID);
    APPEND_S_AS("schema_version", "part_schema_version",
                OB_SCHEMA_LITE_SUBPART_SCHEMA_VERSION);
    APPEND_S_AS("storage_cache_policy", "part_storage_cache_policy",
                OB_SCHEMA_LITE_SUBPART_STORAGE_CACHE_POLICY);
    // Parent part_idx / part_name come from __all_part when requested.
    APPEND_P("part_idx", OB_SCHEMA_LITE_PART_PART_IDX);
    APPEND_P("part_name", OB_SCHEMA_LITE_PART_PART_NAME);
  }
#undef APPEND_P
#undef APPEND_S
#undef APPEND_P_AS
#undef APPEND_S_AS
  if (OB_SUCC(ret) && first) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty join select list", KR(ret), K_(spec));
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::build_join_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString select_list;
  ObSqlString where_sql;
  sql.reset();
  const int normal_part = static_cast<int>(PARTITION_TYPE_NORMAL);
  if (OB_FAIL(build_join_select_list_(select_list))) {
    LOG_WARN("fail to build join select", KR(ret), K_(spec));
  } else if (OB_FAIL(build_join_table_where_(where_sql))) {
    LOG_WARN("fail to build join where", KR(ret), K_(spec));
  } else if (ObSchemaLiteJoinSide::PART == join_side_) {
    if (OB_FAIL(sql.assign_fmt(
            "SELECT %s FROM %s p INNER JOIN %s t "
            "ON p.tenant_id = t.tenant_id AND p.table_id = t.table_id "
            "WHERE %s AND p.partition_type = %d "
            "ORDER BY t.table_id ASC, p.part_idx ASC, p.part_id ASC",
            select_list.ptr(), OB_ALL_PART_TNAME, OB_ALL_TABLE_TNAME,
            where_sql.ptr(), normal_part))) {
      LOG_WARN("fail to assign part join sql", KR(ret));
    }
  } else if (0 != (spec_.part_flags_
                   & (OB_SCHEMA_LITE_PART_PART_IDX | OB_SCHEMA_LITE_PART_PART_NAME))) {
    // SUBPART + parent part_idx / part_name: s ⋈ p ⋈ t (normal partitions only).
    if (OB_FAIL(sql.assign_fmt(
            "SELECT %s FROM %s s INNER JOIN %s p "
            "ON s.tenant_id = p.tenant_id AND s.table_id = p.table_id AND s.part_id = p.part_id "
            "INNER JOIN %s t ON s.tenant_id = t.tenant_id AND s.table_id = t.table_id "
            "WHERE %s AND s.partition_type = %d AND p.partition_type = %d "
            "ORDER BY t.table_id ASC, p.part_idx ASC, p.part_id ASC, "
            "s.sub_part_idx ASC, s.sub_part_id ASC",
            select_list.ptr(), OB_ALL_SUB_PART_TNAME, OB_ALL_PART_TNAME,
            OB_ALL_TABLE_TNAME, where_sql.ptr(), normal_part, normal_part))) {
      LOG_WARN("fail to assign subpart join sql with part_idx", KR(ret));
    }
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT %s FROM %s s INNER JOIN %s t "
          "ON s.tenant_id = t.tenant_id AND s.table_id = t.table_id "
          "WHERE %s AND s.partition_type = %d "
          "ORDER BY t.table_id ASC, s.part_id ASC, s.sub_part_idx ASC, s.sub_part_id ASC",
          select_list.ptr(), OB_ALL_SUB_PART_TNAME, OB_ALL_TABLE_TNAME,
          where_sql.ptr(), normal_part))) {
    LOG_WARN("fail to assign subpart join sql", KR(ret));
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::open_sql_scan_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_client_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql_client is null", KR(ret));
  } else {
    ObASHSetInnerSqlWaitGuard ash_guard(get_lite_part_scan_wait_type_(join_side_));
    ObSqlString sql;
    void *buf = ob_malloc(sizeof(ObISQLClient::ReadResult),
                          ObMemAttr(OB_SERVER_TENANT_ID, "SchemaLiteJnR"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ReadResult", KR(ret));
    } else if (FALSE_IT(sql_res_ = new (buf) ObISQLClient::ReadResult())) {
    } else if (OB_FAIL(build_join_sql_(sql))) {
      LOG_WARN("fail to build join sql", KR(ret));
    } else if (OB_FAIL(sql_client_->read(*sql_res_, spec_.table_spec_.tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to execute join sql", KR(ret), K(sql));
    } else if (OB_ISNULL(sql_result_ = sql_res_->get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join result is null", KR(ret));
    } else {
      sql_res_eof_ = false;
      LOG_INFO("schema lite part join sql scan opened",
               K_(spec), "join_side", static_cast<int>(join_side_), K(sql));
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::append_core_schemas_(ObSchemaGetterGuard &guard)
{
  int ret = OB_SUCCESS;
  ObSchemaLiteTableIterator helper;
  helper.spec_ = spec_;
  helper.sql_client_ = sql_client_;
  if (OB_FAIL(helper.append_core_simple_schemas_from_mgr_(guard))) {
    LOG_WARN("fail to append core schemas for part join", KR(ret), K_(spec));
  } else if (OB_FAIL(simple_schemas_.assign(helper.simple_schemas_))) {
    LOG_WARN("fail to assign core schemas", KR(ret));
  } else {
    schema_idx_ = 0;
    part_pos_ = 0;
    sub_pos_ = 0;
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::init(ObSchemaGetterGuard &guard,
                                       const ObSchemaLiteScanSpec &spec,
                                       const ObSchemaLiteJoinSide join_side)
{
  int ret = OB_SUCCESS;
  reset();
  spec_ = spec;
  join_side_ = join_side;
  if (OB_FAIL(check_spec_())) {
    LOG_WARN("invalid lite part join spec", KR(ret), K(spec),
             "join_side", static_cast<int>(join_side));
  } else if (guard.is_on_demand_schema_mode()) {
    if (OB_FAIL(guard.check_can_full_scan(spec.table_spec_.tenant_id_))) {
      LOG_WARN("full scan not allowed", KR(ret), K(spec));
    } else if (OB_FAIL(guard.prepare_schema_scan_sql_client_(
                   spec.table_spec_.tenant_id_, sql_client_))) {
      LOG_WARN("fail to get lite part scan sql client", KR(ret), K(spec));
    } else if (OB_ISNULL(sql_client_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lite part scan sql client is null", KR(ret), K(spec));
    } else {
      by_simple_schemas_ = false;
      // append_core_schemas_ may execute the encrypted-tablespace auxiliary
      // query; do it before opening the main cursor on a STRONG transaction.
      if (OB_FAIL(append_core_schemas_(guard))) {
        LOG_WARN("fail to append core schemas", KR(ret), K_(spec));
      } else if (OB_FAIL(open_sql_scan_())) {
        LOG_WARN("fail to open part join sql scan", KR(ret), K_(spec));
      } else {
        path_ = PathKind::SQL;
        is_inited_ = true;
      }
    }
  } else {
    ObSchemaLiteTableIterator helper;
    helper.spec_ = spec_;
    by_simple_schemas_ = true;
    path_ = PathKind::SIMPLE_SCHEMA;
    if (OB_FAIL(helper.collect_original_schemas_(guard, simple_schemas_))) {
      LOG_WARN("fail to collect original schemas for part join", KR(ret), K_(spec));
    } else {
      schema_idx_ = 0;
      part_pos_ = 0;
      sub_pos_ = 0;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::assign_table_fields_from_lite_row_(
    const ObSchemaTableLiteRow &row)
{
  int ret = OB_SUCCESS;
  row_.table_id_ = row.table_id_;
  row_.table_type_ = row.table_type_;
  row_.index_type_ = row.index_type_;
  row_.tablegroup_id_ = row.tablegroup_id_;
  row_.database_id_ = row.database_id_;
  row_.data_table_id_ = row.data_table_id_;
  row_.tablespace_id_ = row.tablespace_id_;
  row_.duplicate_scope_ = row.duplicate_scope_;
  row_.has_dynamic_partition_policy_ = row.has_dynamic_partition_policy_;
  row_.part_level_ = row.part_level_;
  row_.table_name_ = row.table_name_;
  row_.session_id_ = row.session_id_;
  row_.encryption_ = row.encryption_;
  row_.index_status_ = row.index_status_;
  row_.schema_version_ = row.schema_version_;
  row_.object_status_ = row.object_status_;
  row_.table_mode_ = row.table_mode_;
  row_.duplicate_read_consistency_ = row.duplicate_read_consistency_;
  row_.storage_cache_policy_ = row.storage_cache_policy_;
  row_.ttl_definition_ = row.ttl_definition_;
  row_.ttl_flag_ = row.ttl_flag_;
  row_.kv_attributes_ = row.kv_attributes_;
  return ret;
}

int ObSchemaLitePartJoinIterator::fill_join_row_from_sql_(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaTableLiteRow table_row;
  const uint64_t table_flags = spec_.table_spec_.select_flags_
      & ~static_cast<uint64_t>(OB_SCHEMA_SCAN_SELECT_TABLET_ID);
  row_.reuse();
  if (OB_FAIL(ObSchemaScanOperator::fill_lite_row(
          result, table_flags, table_row, &string_allocator_))) {
    LOG_WARN("fail to fill table fields from join row", KR(ret));
  } else if (OB_FAIL(assign_table_fields_from_lite_row_(table_row))) {
    LOG_WARN("fail to assign table fields", KR(ret));
  }
  if (OB_SUCC(ret) && (spec_.table_spec_.select_flags_ & OB_SCHEMA_SCAN_SELECT_TABLET_ID)) {
    uint64_t table_tablet_id = ObTabletID::INVALID_TABLET_ID;
    EXTRACT_INT_FIELD_MYSQL(result, "table_tablet_id", table_tablet_id, uint64_t);
    if (OB_SUCC(ret)) {
      row_.table_tablet_id_ = ObTabletID(table_tablet_id);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObSchemaLiteJoinSide::PART == join_side_) {
    if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_ID) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_id", row_.part_id_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_IDX)) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_idx", row_.part_idx_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_TABLET_ID)) {
      uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
      EXTRACT_INT_FIELD_MYSQL(result, "tablet_id", tablet_id, uint64_t);
      if (OB_SUCC(ret)) {
        row_.tablet_id_ = ObTabletID(tablet_id);
      }
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_NAME)) {
      ObString tmp_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_name", tmp_name);
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(string_allocator_, tmp_name, row_.part_name_))) {
        LOG_WARN("fail to copy part_name from join row", KR(ret));
      }
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_SCHEMA_VERSION)) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_schema_version", row_.part_schema_version_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_STORAGE_CACHE_POLICY)
        && OB_FAIL(extract_lite_part_storage_cache_policy_type_(
            result, row_.part_storage_cache_policy_type_))) {
      LOG_WARN("fail to extract part storage cache policy", KR(ret));
    }
  } else {
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_PART_ID) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_id", row_.part_id_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SUB_PART_ID)) {
      EXTRACT_INT_FIELD_MYSQL(result, "sub_part_id", row_.sub_part_id_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX)) {
      EXTRACT_INT_FIELD_MYSQL(result, "sub_part_idx", row_.sub_part_idx_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_TABLET_ID)) {
      uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
      EXTRACT_INT_FIELD_MYSQL(result, "tablet_id", tablet_id, uint64_t);
      if (OB_SUCC(ret)) {
        row_.tablet_id_ = ObTabletID(tablet_id);
      }
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_IDX)) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_idx", row_.part_idx_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_NAME)) {
      ObString tmp_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "part_name", tmp_name);
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(string_allocator_, tmp_name, row_.part_name_))) {
        LOG_WARN("fail to copy parent part_name from join row", KR(ret));
      }
    }
    if (OB_SUCC(ret) && (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SCHEMA_VERSION)) {
      EXTRACT_INT_FIELD_MYSQL(result, "part_schema_version", row_.part_schema_version_, int64_t);
    }
    if (OB_SUCC(ret) && (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_STORAGE_CACHE_POLICY)
        && OB_FAIL(extract_lite_part_storage_cache_policy_type_(
            result, row_.part_storage_cache_policy_type_))) {
      LOG_WARN("fail to extract subpart storage cache policy", KR(ret));
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::fill_join_row_from_simple_(
    const ObSimpleTableSchemaV2 &schema,
    const ObPartition *partition,
    const ObSubPartition *subpart)
{
  int ret = OB_SUCCESS;
  row_.reuse();
  row_.table_id_ = schema.get_table_id();
  row_.table_type_ = schema.get_table_type();
  row_.index_type_ = schema.get_index_type();
  row_.tablegroup_id_ = schema.get_tablegroup_id();
  row_.database_id_ = schema.get_database_id();
  row_.data_table_id_ = schema.get_data_table_id();
  row_.tablespace_id_ = schema.get_tablespace_id();
  row_.duplicate_scope_ = static_cast<int64_t>(schema.get_duplicate_scope());
  row_.has_dynamic_partition_policy_ = schema.with_dynamic_partition_policy();
  row_.part_level_ = schema.get_part_level();
  row_.table_tablet_id_ = schema.get_tablet_id();
  row_.session_id_ = schema.get_session_id();
  row_.index_status_ = schema.get_index_status();
  row_.schema_version_ = schema.get_schema_version();
  row_.object_status_ = schema.get_object_status();
  row_.table_mode_ = schema.get_table_mode_struct().mode_;
  row_.duplicate_read_consistency_ =
      static_cast<int64_t>(schema.get_duplicate_read_consistency());
  const uint64_t flags = spec_.table_spec_.select_flags_;
  if ((flags & OB_SCHEMA_SCAN_SELECT_TABLE_NAME)
      && OB_FAIL(ob_write_string(string_allocator_, schema.get_table_name_str(),
                                 row_.table_name_))) {
    LOG_WARN("fail to copy table_name from simple", KR(ret));
  } else if ((flags & OB_SCHEMA_SCAN_SELECT_ENCRYPTION)
             && OB_FAIL(ob_write_string(string_allocator_, schema.get_encryption_str(),
                                        row_.encryption_))) {
    LOG_WARN("fail to copy encryption from simple", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (ObSchemaLiteJoinSide::PART == join_side_) {
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(schema));
    } else {
      if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_ID) {
        row_.part_id_ = partition->get_part_id();
      }
      if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_IDX) {
        // Column value (__all_part.part_idx), not partition_array cursor.
        row_.part_idx_ = partition->get_part_idx();
      }
      if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_TABLET_ID) {
        row_.tablet_id_ = partition->get_tablet_id();
      }
      if ((spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_NAME)
          && OB_FAIL(ob_write_string(string_allocator_, partition->get_part_name(),
                                     row_.part_name_))) {
        LOG_WARN("fail to copy part_name from simple", KR(ret));
      }
      if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_SCHEMA_VERSION) {
        row_.part_schema_version_ = partition->get_schema_version();
      }
      if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_STORAGE_CACHE_POLICY) {
        row_.part_storage_cache_policy_type_ = partition->get_part_storage_cache_policy_type();
      }
    }
  } else if (OB_ISNULL(partition) || OB_ISNULL(subpart)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition or subpart is null", KR(ret), K(schema), KP(partition), KP(subpart));
  } else {
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_PART_ID) {
      row_.part_id_ = partition->get_part_id();
    }
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SUB_PART_ID) {
      row_.sub_part_id_ = subpart->get_sub_part_id();
    }
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX) {
      row_.sub_part_idx_ = subpart->get_sub_part_idx();
    }
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_TABLET_ID) {
      row_.tablet_id_ = subpart->get_tablet_id();
    }
    if (spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_IDX) {
      row_.part_idx_ = partition->get_part_idx();
    }
    if ((spec_.part_flags_ & OB_SCHEMA_LITE_PART_PART_NAME)
        && OB_FAIL(ob_write_string(string_allocator_, partition->get_part_name(),
                                   row_.part_name_))) {
      LOG_WARN("fail to copy parent part_name from simple", KR(ret));
    }
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_SCHEMA_VERSION) {
      row_.part_schema_version_ = subpart->get_schema_version();
    }
    if (spec_.subpart_flags_ & OB_SCHEMA_LITE_SUBPART_STORAGE_CACHE_POLICY) {
      row_.part_storage_cache_policy_type_ = subpart->get_part_storage_cache_policy_type();
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::next_simple_part_row_()
{
  int ret = OB_SUCCESS;
  bool emitted = false;
  while (OB_SUCC(ret) && !emitted) {
    if (schema_idx_ >= simple_schemas_.count()) {
      ret = OB_ITER_END;
    } else {
      const ObSimpleTableSchemaV2 *schema = simple_schemas_.at(schema_idx_);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", KR(ret), K_(schema_idx));
      } else if (PARTITION_LEVEL_ZERO == schema->get_part_level()) {
        ++schema_idx_;
        part_pos_ = 0;
      } else if (part_pos_ >= schema->get_partition_num()) {
        ++schema_idx_;
        part_pos_ = 0;
      } else {
        const ObPartition *partition = NULL;
        if (OB_FAIL(schema->get_partition_by_partition_index(
                part_pos_, CHECK_PARTITION_MODE_NORMAL, partition))) {
          LOG_WARN("fail to get partition", KR(ret), K_(part_pos), KPC(schema));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KR(ret), K_(part_pos), KPC(schema));
        } else if (OB_FAIL(fill_join_row_from_simple_(
                       *schema, partition, NULL))) {
          LOG_WARN("fail to fill join row from simple part", KR(ret), KPC(schema));
        } else {
          ++part_pos_;
          emitted = true;
        }
      }
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::next_simple_subpart_row_()
{
  int ret = OB_SUCCESS;
  bool emitted = false;
  while (OB_SUCC(ret) && !emitted) {
    if (schema_idx_ >= simple_schemas_.count()) {
      ret = OB_ITER_END;
    } else {
      const ObSimpleTableSchemaV2 *schema = simple_schemas_.at(schema_idx_);
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", KR(ret), K_(schema_idx));
      } else if (PARTITION_LEVEL_TWO != schema->get_part_level()) {
        ++schema_idx_;
        part_pos_ = 0;
        sub_pos_ = 0;
      } else if (part_pos_ >= schema->get_partition_num()) {
        ++schema_idx_;
        part_pos_ = 0;
        sub_pos_ = 0;
      } else {
        const ObPartition *partition = NULL;
        if (OB_FAIL(schema->get_partition_by_partition_index(
                part_pos_, CHECK_PARTITION_MODE_NORMAL, partition))) {
          LOG_WARN("fail to get partition", KR(ret), K_(part_pos), KPC(schema));
        } else if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KR(ret), K_(part_pos), KPC(schema));
        } else {
          ObSubPartition **sub_array = partition->get_subpart_array();
          const int64_t sub_cnt = partition->get_subpartition_num();
          if (sub_pos_ >= sub_cnt) {
            ++part_pos_;
            sub_pos_ = 0;
          } else if (OB_ISNULL(sub_array) || OB_ISNULL(sub_array[sub_pos_])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("subpart is null", KR(ret), K_(sub_pos), KPC(partition));
          } else if (OB_FAIL(fill_join_row_from_simple_(
                         *schema, partition, sub_array[sub_pos_]))) {
            LOG_WARN("fail to fill join row from simple subpart", KR(ret), KPC(schema));
          } else {
            ++sub_pos_;
            emitted = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::get_next_from_simple_schemas_()
{
  int ret = OB_SUCCESS;
  row_.reuse();
  string_allocator_.reuse();
  if (ObSchemaLiteJoinSide::PART == join_side_) {
    if (OB_FAIL(next_simple_part_row_())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next simple part row", KR(ret));
      }
    }
  } else if (OB_FAIL(next_simple_subpart_row_())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to next simple subpart row", KR(ret));
    }
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::get_next_from_sql_()
{
  int ret = OB_SUCCESS;
  row_.reuse();
  string_allocator_.reuse();
  if (OB_ISNULL(sql_result_) || sql_res_eof_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(sql_result_->next())) {
    if (OB_ITER_END == ret) {
      sql_res_eof_ = true;
      // See ObSchemaLiteTableIterator: drop the result handler at EOF so the
      // shared STRONG transaction can execute the next family scan.
      sql_res_->reset();
      sql_result_ = NULL;
    } else {
      LOG_WARN("fail to next join row", KR(ret));
    }
  } else if (OB_FAIL(fill_join_row_from_sql_(*sql_result_))) {
    LOG_WARN("fail to fill join row from sql", KR(ret));
  }
  return ret;
}

int ObSchemaLitePartJoinIterator::get_next(const ObSchemaLiteJoinRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lite part join iterator not init", KR(ret));
  } else if (PathKind::SQL == path_) {
    if (!sql_res_eof_) {
      if (OB_FAIL(get_next_from_sql_())) {
        if (OB_ITER_END == ret && schema_idx_ < simple_schemas_.count()) {
          ret = get_next_from_simple_schemas_();
          if (OB_FAIL(ret) && OB_ITER_END != ret) {
            LOG_WARN("fail get_next core simple after join sql", KR(ret));
          }
        } else if (OB_ITER_END != ret) {
          LOG_WARN("fail get_next join sql", KR(ret));
        }
      }
    } else if (schema_idx_ < simple_schemas_.count()) {
      if (OB_FAIL(get_next_from_simple_schemas_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail get_next core simple after join eof", KR(ret));
        }
      }
    } else {
      ret = OB_ITER_END;
    }
  } else if (PathKind::SIMPLE_SCHEMA == path_) {
    if (OB_FAIL(get_next_from_simple_schemas_())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail get_next simple join", KR(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path", KR(ret), "path", static_cast<int>(path_));
  }
  if (OB_SUCC(ret)) {
    row = &row_;
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
