/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_scan_operator.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ObSchemaScanKind ObSchemaScanOperator::last_scan_kind_ = ObSchemaScanKind::BY_TENANT;
int64_t ObSchemaScanOperator::last_scan_row_cnt_ = 0;
int64_t ObSchemaScanOperator::last_scan_elapsed_us_ = 0;

const char *ObSchemaScanOperator::scan_kind_to_str(const ObSchemaScanKind kind)
{
  const char *str = "UNKNOWN";
  switch (kind) {
    case ObSchemaScanKind::BY_TABLE_TYPE:
      str = "BY_TABLE_TYPE";
      break;
    case ObSchemaScanKind::BY_DUPLICATE_SCOPE:
      str = "BY_DUPLICATE_SCOPE";
      break;
    case ObSchemaScanKind::BY_DYNAMIC_PARTITION:
      str = "BY_DYNAMIC_PARTITION";
      break;
    case ObSchemaScanKind::BY_INDEX_TYPE:
      str = "BY_INDEX_TYPE";
      break;
    case ObSchemaScanKind::BY_TABLEGROUP:
      str = "BY_TABLEGROUP";
      break;
    case ObSchemaScanKind::BY_TABLESPACE:
      str = "BY_TABLESPACE";
      break;
    case ObSchemaScanKind::BY_DATABASE:
      str = "BY_DATABASE";
      break;
    case ObSchemaScanKind::BY_TENANT:
      str = "BY_TENANT";
      break;
    case ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE:
      str = "BY_ENCRYPTED_TABLESPACE";
      break;
    default:
      break;
  }
  return str;
}

int ObSchemaScanOperator::fill_tmp_table_types(ObIArray<int64_t> &table_types)
{
  int ret = OB_SUCCESS;
  table_types.reset();
  if (OB_FAIL(table_types.push_back(TMP_TABLE))
      || OB_FAIL(table_types.push_back(TMP_TABLE_ORA_SESS))
      || OB_FAIL(table_types.push_back(TMP_TABLE_ORA_TRX))
      || OB_FAIL(table_types.push_back(TMP_TABLE_ORA_SESS_V2))
      || OB_FAIL(table_types.push_back(TMP_TABLE_ORA_TRX_V2))) {
    LOG_WARN("fail to push tmp table types", KR(ret));
  }
  return ret;
}

int ObSchemaScanOperator::fill_view_table_types(ObIArray<int64_t> &table_types)
{
  int ret = OB_SUCCESS;
  // Align with is_view_table() / Original only_view: USER + SYSTEM + MATERIALIZED.
  table_types.reset();
  if (OB_FAIL(table_types.push_back(USER_VIEW))) {
    LOG_WARN("fail to push USER_VIEW table type", KR(ret));
  } else if (OB_FAIL(table_types.push_back(SYSTEM_VIEW))) {
    LOG_WARN("fail to push SYSTEM_VIEW table type", KR(ret));
  } else if (OB_FAIL(table_types.push_back(MATERIALIZED_VIEW))) {
    LOG_WARN("fail to push MATERIALIZED_VIEW table type", KR(ret));
  }
  return ret;
}

int ObSchemaScanOperator::fill_external_table_types(ObIArray<int64_t> &table_types)
{
  int ret = OB_SUCCESS;
  table_types.reset();
  if (OB_FAIL(table_types.push_back(EXTERNAL_TABLE))) {
    LOG_WARN("fail to push external table type", KR(ret));
  }
  return ret;
}

int ObSchemaScanOperator::fill_vec_index_types(ObIArray<int64_t> &index_types)
{
  int ret = OB_SUCCESS;
  index_types.reset();
  for (int64_t i = INDEX_TYPE_IS_NOT; OB_SUCC(ret) && i < INDEX_TYPE_MAX; ++i) {
    const ObIndexType index_type = static_cast<ObIndexType>(i);
    if (is_vec_index(index_type)) {
      if (OB_FAIL(index_types.push_back(i))) {
        LOG_WARN("fail to push vec index type", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSchemaScanOperator::fill_vec_ivf_index_types(ObIArray<int64_t> &index_types)
{
  int ret = OB_SUCCESS;
  index_types.reset();
  for (int64_t i = INDEX_TYPE_IS_NOT; OB_SUCC(ret) && i < INDEX_TYPE_MAX; ++i) {
    const ObIndexType index_type = static_cast<ObIndexType>(i);
    if (is_vec_ivf_index(index_type)) {
      if (OB_FAIL(index_types.push_back(i))) {
        LOG_WARN("fail to push vec ivf index type", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSchemaScanOperator::fill_async_index_types(ObIArray<int64_t> &index_types)
{
  int ret = OB_SUCCESS;
  index_types.reset();
  for (int64_t i = INDEX_TYPE_IS_NOT; OB_SUCC(ret) && i < INDEX_TYPE_MAX; ++i) {
    const ObIndexType index_type = static_cast<ObIndexType>(i);
    if (is_async_index(index_type)) {
      if (OB_FAIL(index_types.push_back(i))) {
        LOG_WARN("fail to push async index type", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSchemaScanOperator::check_spec_(const ObSchemaScanSpec &spec)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == spec.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(spec));
  } else if (OB_UNLIKELY(PARTITION_LEVEL_MAX != spec.part_level_
             && (PARTITION_LEVEL_ZERO != spec.part_level_
                 && PARTITION_LEVEL_ONE != spec.part_level_
                 && PARTITION_LEVEL_TWO != spec.part_level_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_level filter", KR(ret), K(spec));
  } else {
    switch (spec.kind_) {
      case ObSchemaScanKind::BY_TABLE_TYPE:
      case ObSchemaScanKind::BY_INDEX_TYPE: {
        if (OB_ISNULL(spec.enum_values_) || spec.enum_values_->empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("enum_values required", KR(ret), K(spec));
        }
        break;
      }
      case ObSchemaScanKind::BY_TABLEGROUP:
      case ObSchemaScanKind::BY_TABLESPACE:
      case ObSchemaScanKind::BY_DATABASE: {
        if (OB_UNLIKELY(OB_INVALID_ID == spec.filter_id_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("filter_id required", KR(ret), K(spec));
        }
        break;
      }
      case ObSchemaScanKind::BY_DUPLICATE_SCOPE:
      case ObSchemaScanKind::BY_DYNAMIC_PARTITION:
      case ObSchemaScanKind::BY_TENANT:
      case ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE:
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown scan kind", KR(ret), K(spec));
        break;
    }
  }
  return ret;
}

int ObSchemaScanOperator::build_select_list_(const uint64_t select_flags, ObSqlString &select_list)
{
  int ret = OB_SUCCESS;
  select_list.reset();
  bool first = true;
  if (!(select_flags & OB_SCHEMA_SCAN_SELECT_TABLE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id must be selected", KR(ret), K(select_flags));
  }
#define APPEND_COL(col, flag)                                      \
  do {                                                             \
    if (OB_SUCC(ret) && (select_flags & (flag))) {                 \
      if (!first && OB_FAIL(select_list.append(", "))) {           \
        LOG_WARN("fail to append comma", KR(ret));                 \
      } else if (OB_FAIL(select_list.append(col))) {               \
        LOG_WARN("fail to append col", KR(ret), K(col));           \
      } else {                                                     \
        first = false;                                             \
      }                                                            \
    }                                                              \
  } while (0)
  APPEND_COL("table_id", OB_SCHEMA_SCAN_SELECT_TABLE_ID);
  APPEND_COL("table_type", OB_SCHEMA_SCAN_SELECT_TABLE_TYPE);
  APPEND_COL("index_type", OB_SCHEMA_SCAN_SELECT_INDEX_TYPE);
  APPEND_COL("tablegroup_id", OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID);
  APPEND_COL("database_id", OB_SCHEMA_SCAN_SELECT_DATABASE_ID);
  APPEND_COL("data_table_id", OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID);
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_PART_LEVEL)) {
    if (!first && OB_FAIL(select_list.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    } else if (OB_FAIL(select_list.append_fmt(
                   "CASE WHEN part_level = %d AND part_num = 1 "
                   "AND part_func_type = %d "
                   "AND (part_func_expr IS NULL OR part_func_expr = '') "
                   "THEN %d ELSE part_level END AS part_level",
                   static_cast<int>(PARTITION_LEVEL_ONE),
                   static_cast<int>(PARTITION_FUNC_TYPE_HASH),
                   static_cast<int>(PARTITION_LEVEL_ZERO)))) {
      LOG_WARN("fail to append normalized part_level", KR(ret));
    } else {
      first = false;
    }
  }
  APPEND_COL("tablet_id", OB_SCHEMA_SCAN_SELECT_TABLET_ID);
  APPEND_COL("tablespace_id", OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID);
  APPEND_COL("duplicate_scope", OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE);
  APPEND_COL("has_dynamic_partition_policy", OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY);
  APPEND_COL("table_name", OB_SCHEMA_SCAN_SELECT_TABLE_NAME);
  APPEND_COL("session_id", OB_SCHEMA_SCAN_SELECT_SESSION_ID);
  APPEND_COL("encryption", OB_SCHEMA_SCAN_SELECT_ENCRYPTION);
  APPEND_COL("index_status", OB_SCHEMA_SCAN_SELECT_INDEX_STATUS);
  APPEND_COL("schema_version", OB_SCHEMA_SCAN_SELECT_SCHEMA_VERSION);
  APPEND_COL("object_status", OB_SCHEMA_SCAN_SELECT_OBJECT_STATUS);
  APPEND_COL("table_mode", OB_SCHEMA_SCAN_SELECT_TABLE_MODE);
  APPEND_COL("duplicate_read_consistency", OB_SCHEMA_SCAN_SELECT_DUPLICATE_READ_CONSISTENCY);
  APPEND_COL("storage_cache_policy", OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY);
  APPEND_COL("ttl_definition", OB_SCHEMA_SCAN_SELECT_TTL_DEFINITION);
  APPEND_COL("ttl_flag", OB_SCHEMA_SCAN_SELECT_TTL_FLAG);
  APPEND_COL("kv_attributes", OB_SCHEMA_SCAN_SELECT_KV_ATTRIBUTES);
#undef APPEND_COL
  return ret;
}

int ObSchemaScanOperator::build_in_list_(const ObIArray<int64_t> &values, ObSqlString &in_list)
{
  int ret = OB_SUCCESS;
  in_list.reset();
  if (OB_UNLIKELY(values.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty in list", KR(ret));
  } else if (OB_FAIL(in_list.append("("))) {
    LOG_WARN("fail to append", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
      if (0 != i && OB_FAIL(in_list.append(","))) {
        LOG_WARN("fail to append comma", KR(ret));
      } else if (OB_FAIL(in_list.append_fmt("%ld", values.at(i)))) {
        LOG_WARN("fail to append value", KR(ret), K(i));
      }
    }
    if (FAILEDx(in_list.append(")"))) {
      LOG_WARN("fail to append end", KR(ret));
    }
  }
  return ret;
}

int ObSchemaScanOperator::build_uint_in_list_(const ObIArray<uint64_t> &values, ObSqlString &in_list)
{
  int ret = OB_SUCCESS;
  in_list.reset();
  if (OB_UNLIKELY(values.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty in list", KR(ret));
  } else if (OB_FAIL(in_list.append("("))) {
    LOG_WARN("fail to append", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
      if (0 != i && OB_FAIL(in_list.append(","))) {
        LOG_WARN("fail to append comma", KR(ret));
      } else if (OB_FAIL(in_list.append_fmt("%lu", values.at(i)))) {
        LOG_WARN("fail to append value", KR(ret), K(i));
      }
    }
    if (FAILEDx(in_list.append(")"))) {
      LOG_WARN("fail to append end", KR(ret));
    }
  }
  return ret;
}

int ObSchemaScanOperator::append_part_level_predicate(const ObSchemaScanSpec &spec,
                                                      const char *table_alias,
                                                      ObSqlString &where_sql)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_MAX == spec.part_level_) {
    // no part_level predicate
  } else {
    const bool with_alias = OB_NOT_NULL(table_alias) && '\0' != table_alias[0];
    const char *a = with_alias ? table_alias : "";
    const char *d = with_alias ? "." : "";
    // compare the normalized part_level, same expression as build_select_list_ and
    // ObSimpleTableSchemaV2::get_part_level(): one partition hash table is level zero
    if (OB_FAIL(where_sql.append_fmt(
            " AND (CASE WHEN %s%spart_level = %d AND %s%spart_num = 1 "
            "AND %s%spart_func_type = %d AND (%s%spart_func_expr IS NULL OR %s%spart_func_expr = '') "
            "THEN %d ELSE %s%spart_level END) = %ld",
            a, d, static_cast<int>(PARTITION_LEVEL_ONE),
            a, d, a, d, static_cast<int>(PARTITION_FUNC_TYPE_HASH),
            a, d, a, d,
            static_cast<int>(PARTITION_LEVEL_ZERO),
            a, d, static_cast<int64_t>(spec.part_level_)))) {
      LOG_WARN("fail to append part_level predicate", KR(ret), K(spec));
    }
  }
  return ret;
}

int ObSchemaScanOperator::build_where_clause_(const ObSchemaScanSpec &spec, ObSqlString &where_sql)
{
  int ret = OB_SUCCESS;
  where_sql.reset();
  // __all_table stores tenant_id = 0 for user tenants; exec tenant is passed to sql_client.read().
  if (OB_FAIL(where_sql.assign_fmt("tenant_id = 0"))) {
    LOG_WARN("fail to assign where", KR(ret));
  } else {
    switch (spec.kind_) {
      case ObSchemaScanKind::BY_TABLE_TYPE: {
        ObSqlString in_list;
        if (OB_FAIL(build_in_list_(*spec.enum_values_, in_list))) {
          LOG_WARN("fail to build in list", KR(ret));
        } else if (OB_FAIL(where_sql.append_fmt(" AND table_type IN %s", in_list.ptr()))) {
          LOG_WARN("fail to append table_type predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_DUPLICATE_SCOPE: {
        if (OB_FAIL(where_sql.append(" AND duplicate_scope != 0"))) {
          LOG_WARN("fail to append duplicate_scope predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_DYNAMIC_PARTITION: {
        // Requires __all_table.has_dynamic_partition_policy (see 全量扫描.md §3.2).
        if (OB_FAIL(where_sql.append(" AND has_dynamic_partition_policy = 1"))) {
          LOG_WARN("fail to append dynamic partition predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_INDEX_TYPE: {
        ObSqlString in_list;
        if (OB_FAIL(build_in_list_(*spec.enum_values_, in_list))) {
          LOG_WARN("fail to build in list", KR(ret));
        } else if (OB_FAIL(where_sql.append_fmt(" AND index_type IN %s", in_list.ptr()))) {
          LOG_WARN("fail to append index_type predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_TABLEGROUP: {
        if (OB_FAIL(where_sql.append_fmt(
                " AND tablegroup_id = %lu AND table_type IN (%d, %d, %d)",
                spec.filter_id_,
                static_cast<int>(ObTableType::SYSTEM_TABLE),
                static_cast<int>(ObTableType::USER_TABLE),
                static_cast<int>(ObTableType::TMP_TABLE)))) {
          LOG_WARN("fail to append tablegroup predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_TABLESPACE: {
        if (OB_FAIL(where_sql.append_fmt(" AND tablespace_id = %lu", spec.filter_id_))) {
          LOG_WARN("fail to append tablespace predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_DATABASE: {
        if (OB_FAIL(where_sql.append_fmt(" AND database_id = %lu", spec.filter_id_))) {
          LOG_WARN("fail to append database predicate", KR(ret));
        }
        break;
      }
      case ObSchemaScanKind::BY_TENANT:
        // tenant_id = 0 prefix only
        break;
      case ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("kind should be handled outside build_where_clause_", KR(ret), K(spec));
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown kind", KR(ret), K(spec));
        break;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append_part_level_predicate(spec, NULL, where_sql))) {
      LOG_WARN("fail to append part_level predicate", KR(ret), K(spec));
    }
  }
  return ret;
}

int ObSchemaScanOperator::scan_encrypted_tablespace_ids(ObISQLClient &sql_client,
                                                        const uint64_t tenant_id,
                                                        ObIArray<uint64_t> &tablespace_ids)
{
  return scan_encrypted_tablespace_ids_(sql_client, tenant_id, tablespace_ids);
}

int ObSchemaScanOperator::scan_encrypted_tablespace_ids_(ObISQLClient &sql_client,
                                                        const uint64_t tenant_id,
                                                        ObIArray<uint64_t> &tablespace_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  tablespace_ids.reset();
  if (OB_FAIL(sql.assign_fmt(
          "SELECT tablespace_id FROM %s WHERE tenant_id = 0 AND encryption_name != ''",
          OB_ALL_TENANT_TABLESPACE_TNAME))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          uint64_t tablespace_id = OB_INVALID_ID;
          EXTRACT_INT_FIELD_MYSQL(*result, "tablespace_id", tablespace_id, uint64_t);
          if (FAILEDx(tablespace_ids.push_back(tablespace_id))) {
            LOG_WARN("fail to push tablespace_id", KR(ret), K(tablespace_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSchemaScanOperator::fill_lite_row_(sqlclient::ObMySQLResult &result,
                                         const uint64_t select_flags,
                                         ObSchemaTableLiteRow &row,
                                         ObIAllocator *str_allocator)
{
  int ret = OB_SUCCESS;
  row = ObSchemaTableLiteRow();
  const bool need_str = (0 != (select_flags & OB_SCHEMA_SCAN_SELECT_NEED_STR_ALLOCATOR));
  if (need_str && OB_ISNULL(str_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("string select flags require str_allocator", KR(ret), K(select_flags));
  }
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", row.table_id_, uint64_t);
  if (OB_FAIL(ret)) {
  } else if ((select_flags & OB_SCHEMA_SCAN_SELECT_TABLE_TYPE)) {
    int64_t table_type = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "table_type", table_type, int64_t);
    if (OB_SUCC(ret)) {
      row.table_type_ = static_cast<ObTableType>(table_type);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_INDEX_TYPE)) {
    int64_t index_type = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "index_type", index_type, int64_t);
    if (OB_SUCC(ret)) {
      row.index_type_ = static_cast<ObIndexType>(index_type);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID)) {
    EXTRACT_INT_FIELD_MYSQL(result, "tablegroup_id", row.tablegroup_id_, uint64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_DATABASE_ID)) {
    EXTRACT_INT_FIELD_MYSQL(result, "database_id", row.database_id_, uint64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID)) {
    EXTRACT_INT_FIELD_MYSQL(result, "data_table_id", row.data_table_id_, uint64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID)) {
    EXTRACT_INT_FIELD_MYSQL(result, "tablespace_id", row.tablespace_id_, uint64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE)) {
    EXTRACT_INT_FIELD_MYSQL(result, "duplicate_scope", row.duplicate_scope_, int64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY)) {
    int64_t has_policy = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "has_dynamic_partition_policy", has_policy, int64_t);
    if (OB_SUCC(ret)) {
      row.has_dynamic_partition_policy_ = (0 != has_policy);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_PART_LEVEL)) {
    int64_t part_level = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "part_level", part_level, int64_t);
    if (OB_SUCC(ret)) {
      row.part_level_ = static_cast<ObPartitionLevel>(part_level);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TABLET_ID)) {
    uint64_t tablet_id = common::ObTabletID::INVALID_TABLET_ID;
    EXTRACT_INT_FIELD_MYSQL(result, "tablet_id", tablet_id, uint64_t);
    if (OB_SUCC(ret)) {
      row.tablet_id_ = common::ObTabletID(tablet_id);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TABLE_NAME)) {
    ObString tmp_name;
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "table_name", tmp_name);
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(*str_allocator, tmp_name, row.table_name_))) {
      LOG_WARN("fail to deep copy table_name", KR(ret), K(tmp_name));
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_SESSION_ID)) {
    EXTRACT_INT_FIELD_MYSQL(result, "session_id", row.session_id_, uint64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_ENCRYPTION)) {
    ObString tmp_enc;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "encryption", tmp_enc);
    if (OB_SUCC(ret) && !tmp_enc.empty()
        && OB_FAIL(ob_write_string(*str_allocator, tmp_enc, row.encryption_))) {
      LOG_WARN("fail to deep copy encryption", KR(ret), K(tmp_enc));
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_INDEX_STATUS)) {
    int64_t index_status = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "index_status", index_status, int64_t);
    if (OB_SUCC(ret)) {
      row.index_status_ = static_cast<ObIndexStatus>(index_status);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_SCHEMA_VERSION)) {
    EXTRACT_INT_FIELD_MYSQL(result, "schema_version", row.schema_version_, int64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_OBJECT_STATUS)) {
    int64_t object_status = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "object_status", object_status, int64_t);
    if (OB_SUCC(ret)) {
      row.object_status_ = static_cast<ObObjectStatus>(object_status);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TABLE_MODE)) {
    int64_t table_mode = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "table_mode", table_mode, int64_t);
    if (OB_SUCC(ret)) {
      row.table_mode_ = static_cast<int32_t>(table_mode);
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_DUPLICATE_READ_CONSISTENCY)) {
    EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "duplicate_read_consistency",
                                     row.duplicate_read_consistency_, int64_t);
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY)) {
    ObString tmp_policy;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "storage_cache_policy", tmp_policy);
    if (OB_SUCC(ret) && !tmp_policy.empty()
        && OB_FAIL(ob_write_string(*str_allocator, tmp_policy, row.storage_cache_policy_))) {
      LOG_WARN("fail to deep copy storage_cache_policy", KR(ret), K(tmp_policy));
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TTL_DEFINITION)) {
    ObString tmp_ttl;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "ttl_definition", tmp_ttl);
    if (OB_SUCC(ret) && !tmp_ttl.empty()
        && OB_FAIL(ob_write_string(*str_allocator, tmp_ttl, row.ttl_definition_))) {
      LOG_WARN("fail to deep copy ttl_definition", KR(ret), K(tmp_ttl));
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_TTL_FLAG)) {
    ObString tmp_flag;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "ttl_flag", tmp_flag);
    if (OB_SUCC(ret) && !tmp_flag.empty()
        && OB_FAIL(ob_write_string(*str_allocator, tmp_flag, row.ttl_flag_))) {
      LOG_WARN("fail to deep copy ttl_flag", KR(ret), K(tmp_flag));
    }
  }
  if (OB_SUCC(ret) && (select_flags & OB_SCHEMA_SCAN_SELECT_KV_ATTRIBUTES)) {
    ObString tmp_kv;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "kv_attributes", tmp_kv);
    if (OB_SUCC(ret) && !tmp_kv.empty()
        && OB_FAIL(ob_write_string(*str_allocator, tmp_kv, row.kv_attributes_))) {
      LOG_WARN("fail to deep copy kv_attributes", KR(ret), K(tmp_kv));
    }
  }
  return ret;
}

void ObSchemaScanOperator::record_scan_stat_(const ObSchemaScanSpec &spec,
                                             const int ret_code,
                                             const int64_t row_cnt,
                                             const int64_t elapsed_us)
{
  last_scan_kind_ = spec.kind_;
  last_scan_row_cnt_ = row_cnt;
  last_scan_elapsed_us_ = elapsed_us;
  EVENT_INC(SCHEMA_FULL_SCAN_COUNT);
  EVENT_ADD(SCHEMA_FULL_SCAN_TIME, elapsed_us);
  EVENT_ADD(SCHEMA_FULL_SCAN_ROW_COUNT, row_cnt);

  const char *kind_str = scan_kind_to_str(spec.kind_);
  const char *caller = (NULL == spec.caller_tag_) ? "" : spec.caller_tag_;
  const int64_t WARN_ELAPSED_US = 1000 * 1000; // 1s
  const int64_t WARN_ROW_CNT = 100000;
  // LOG_* macros require a local `ret` for LOG_ERRCODE_FOR_*.
  const int ret = ret_code;
  if (elapsed_us >= WARN_ELAPSED_US || row_cnt >= WARN_ROW_CNT || OB_SUCCESS != ret) {
    LOG_WARN("schema full scan", KR(ret), K(spec.tenant_id_), K(kind_str), K(caller),
             K(row_cnt), K(elapsed_us), "is_remote", "unknown");
  } else {
    LOG_TRACE("schema full scan", KR(ret), K(spec.tenant_id_), K(kind_str), K(caller),
              K(row_cnt), K(elapsed_us), "is_remote", "unknown");
  }
}

int ObSchemaScanOperator::execute_table_sql_(ObISQLClient &sql_client,
                                             const ObSchemaScanSpec &spec,
                                             const ObSqlString &sql,
                                             const bool only_ids,
                                             ObIArray<uint64_t> *table_ids,
                                             ObIArray<ObSchemaTableLiteRow> *rows)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t row_cnt = 0;
  if (only_ids && OB_ISNULL(table_ids)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ids is null", KR(ret));
  } else if (!only_ids && OB_ISNULL(rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rows is null", KR(ret));
  } else {
    ObASHSetInnerSqlWaitGuard ash_guard(ObInnerSqlWaitTypeId::SCHEMA_FULL_SCAN);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, spec.tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(spec), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (only_ids) {
            uint64_t table_id = OB_INVALID_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
            if (FAILEDx(table_ids->push_back(table_id))) {
              LOG_WARN("fail to push table_id", KR(ret), K(table_id));
            }
          } else {
            ObSchemaTableLiteRow row;
            // scan_table_rows has no string arena; reject string projections here.
            if (OB_FAIL(fill_lite_row_(*result, spec.select_flags_, row, NULL))) {
              LOG_WARN("fail to fill lite row", KR(ret));
            } else if (OB_FAIL(rows->push_back(row))) {
              LOG_WARN("fail to push row", KR(ret), K(row));
            }
          }
          if (OB_SUCC(ret)) {
            ++row_cnt;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    record_scan_stat_(spec, ret, row_cnt, ObTimeUtility::current_time() - start_ts);
  }
  return ret;
}

int ObSchemaScanOperator::scan_table_ids(ObISQLClient &sql_client,
                                         const ObSchemaScanSpec &spec,
                                         ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (OB_FAIL(check_spec_(spec))) {
    LOG_WARN("invalid spec", KR(ret), K(spec));
  } else if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec.kind_) {
    ObArray<uint64_t> tablespace_ids;
    const int64_t start_ts = ObTimeUtility::current_time();
    if (OB_FAIL(scan_encrypted_tablespace_ids_(sql_client, spec.tenant_id_, tablespace_ids))) {
      LOG_WARN("fail to scan encrypted tablespaces", KR(ret), K(spec));
      record_scan_stat_(spec, ret, 0, ObTimeUtility::current_time() - start_ts);
    } else if (tablespace_ids.empty()) {
      record_scan_stat_(spec, OB_SUCCESS, 0, ObTimeUtility::current_time() - start_ts);
    } else {
      ObSqlString in_list;
      ObSqlString where_sql;
      ObSqlString sql;
      if (OB_FAIL(build_uint_in_list_(tablespace_ids, in_list))) {
        LOG_WARN("fail to build tablespace in list", KR(ret));
      } else if (OB_FAIL(where_sql.assign_fmt("tenant_id = 0 AND tablespace_id IN %s",
              in_list.ptr()))) {
        LOG_WARN("fail to assign encrypted where", KR(ret));
      } else if (OB_FAIL(append_part_level_predicate(spec, NULL, where_sql))) {
        LOG_WARN("fail to append part_level predicate", KR(ret), K(spec));
      } else if (OB_FAIL(sql.assign_fmt("SELECT table_id FROM %s WHERE %s",
              OB_ALL_TABLE_TNAME, where_sql.ptr()))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(execute_table_sql_(sql_client, spec, sql, true/*only_ids*/, &table_ids, NULL))) {
        LOG_WARN("fail to execute table sql", KR(ret), K(sql));
      }
    }
  } else {
    ObSqlString select_list;
    ObSqlString where_sql;
    ObSqlString sql;
    if (OB_FAIL(select_list.assign("table_id"))) {
      LOG_WARN("fail to assign select", KR(ret));
    } else if (OB_FAIL(build_where_clause_(spec, where_sql))) {
      LOG_WARN("fail to build where", KR(ret), K(spec));
    } else if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s",
                                     select_list.ptr(), OB_ALL_TABLE_TNAME, where_sql.ptr()))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(execute_table_sql_(sql_client, spec, sql, true/*only_ids*/, &table_ids, NULL))) {
      LOG_WARN("fail to execute table sql", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObSchemaScanOperator::scan_alive_table_ids_from_history(ObISQLClient &sql_client,
                                                           const uint64_t tenant_id,
                                                           const int64_t schema_version,
                                                           ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSchemaScanSpec spec;
  spec.tenant_id_ = tenant_id;
  spec.kind_ = ObSchemaScanKind::BY_TENANT;
  spec.caller_tag_ = "scan_alive_table_ids_from_history";
  table_ids.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+ leading(b a) use_nl(b a) no_rewrite() */ a.table_id "
          "FROM %s AS a JOIN ("
          "  SELECT tenant_id, table_id, MAX(schema_version) AS schema_version "
          "  FROM %s WHERE tenant_id = 0 AND schema_version <= %ld "
          "  GROUP BY tenant_id, table_id"
          ") AS b "
          "ON a.tenant_id = b.tenant_id AND a.table_id = b.table_id "
          "AND a.schema_version = b.schema_version "
          "WHERE a.is_deleted = 0 AND a.table_id != %lu "
          "ORDER BY a.table_id ASC",
          OB_ALL_TABLE_HISTORY_TNAME, OB_ALL_TABLE_HISTORY_TNAME,
          schema_version, static_cast<uint64_t>(OB_ALL_CORE_TABLE_TID)))) {
    LOG_WARN("fail to assign history alive-id sql", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(execute_table_sql_(sql_client, spec, sql, true/*only_ids*/, &table_ids, NULL))) {
    LOG_WARN("fail to execute history alive-id sql", KR(ret), K(tenant_id), K(schema_version), K(sql));
  }
  return ret;
}

int ObSchemaScanOperator::scan_table_rows(ObISQLClient &sql_client,
                                          const ObSchemaScanSpec &spec,
                                          ObIArray<ObSchemaTableLiteRow> &rows)
{
  int ret = OB_SUCCESS;
  rows.reset();
  ObSqlString select_list;
  ObSqlString where_sql;
  ObSqlString sql;
  if (OB_FAIL(check_spec_(spec))) {
    LOG_WARN("invalid spec", KR(ret), K(spec));
  } else if (ObSchemaScanKind::BY_ENCRYPTED_TABLESPACE == spec.kind_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("scan_table_rows not supported for this kind yet", KR(ret), K(spec));
  } else if (OB_FAIL(build_select_list_(spec.select_flags_, select_list))) {
    LOG_WARN("fail to build select list", KR(ret), K(spec));
  } else if (OB_FAIL(build_where_clause_(spec, where_sql))) {
    LOG_WARN("fail to build where", KR(ret), K(spec));
  } else if (OB_FAIL(sql.assign_fmt("SELECT %s FROM %s WHERE %s",
                                   select_list.ptr(), OB_ALL_TABLE_TNAME, where_sql.ptr()))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_table_sql_(sql_client, spec, sql, false/*only_ids*/, NULL, &rows))) {
    LOG_WARN("fail to execute table sql", KR(ret), K(sql));
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
