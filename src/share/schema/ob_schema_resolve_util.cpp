/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_get_compat_mode.h"
#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
namespace schema_resolve_util
{

namespace
{

// Must match ObTableMode::TM_TABLE_STATE_FLAG_OFFSET / is_user_hidden_table().
// System views already use: table_mode >> 12 & 15 / & 2.
static const int64_t TABLE_MODE_STATE_FLAG_SHIFT = 12;

int append_user_hidden_predicate_(const bool expect_hidden, ObSqlString &filter)
{
  int ret = OB_SUCCESS;
  if (expect_hidden) {
    if (OB_FAIL(filter.append_fmt(
        "AND ((table_mode >> %ld) & %d) != 0 ",
        TABLE_MODE_STATE_FLAG_SHIFT,
        static_cast<int>(TABLE_STATE_IS_HIDDEN_MASK)))) {
      LOG_WARN("failed to append hidden table_mode filter", KR(ret));
    }
  } else if (OB_FAIL(filter.append_fmt(
      "AND ((table_mode >> %ld) & %d) = 0 ",
      TABLE_MODE_STATE_FLAG_SHIFT,
      static_cast<int>(TABLE_STATE_IS_HIDDEN_MASK)))) {
    LOG_WARN("failed to append non-hidden table_mode filter", KR(ret));
  }
  return ret;
}

// Single source of truth with is_built_in_index(): iterate the enum and call it.
int append_built_in_index_type_predicate_(const bool expect_built_in, ObSqlString &filter)
{
  int ret = OB_SUCCESS;
  ObSqlString in_list;
  bool first = true;
  if (OB_FAIL(in_list.append("("))) {
    LOG_WARN("failed to append built-in index_type list prefix", KR(ret));
  } else {
    for (int64_t i = INDEX_TYPE_IS_NOT; OB_SUCC(ret) && i < INDEX_TYPE_MAX; ++i) {
      const ObIndexType index_type = static_cast<ObIndexType>(i);
      if (is_built_in_index(index_type)) {
        if (!first && OB_FAIL(in_list.append(","))) {
          LOG_WARN("failed to append built-in index_type comma", KR(ret), K(i));
        } else if (OB_FAIL(in_list.append_fmt("%ld", i))) {
          LOG_WARN("failed to append built-in index_type value", KR(ret), K(i));
        } else {
          first = false;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (first) {
      // No built-in index_type currently defined.
      if (expect_built_in) {
        if (OB_FAIL(filter.append("AND 1 = 0 "))) {
          LOG_WARN("failed to append empty built-in index predicate", KR(ret));
        }
      }
      // Normal index: no index_type restriction needed when built-in set is empty.
    } else if (OB_FAIL(in_list.append(")"))) {
      LOG_WARN("failed to append built-in index_type list suffix", KR(ret));
    } else if (expect_built_in) {
      if (OB_FAIL(filter.append_fmt("AND index_type IN %s ", in_list.ptr()))) {
        LOG_WARN("failed to append built-in index_type IN predicate", KR(ret));
      }
    } else if (OB_FAIL(filter.append_fmt("AND index_type NOT IN %s ", in_list.ptr()))) {
      LOG_WARN("failed to append built-in index_type NOT IN predicate", KR(ret));
    }
  }
  return ret;
}

int build_oracle_origin_index_like_pattern_(const ObString &origin_index_name,
                                            ObSqlString &like_pattern)
{
  int ret = OB_SUCCESS;
  like_pattern.reset();
  if (origin_index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid origin index name", KR(ret));
  } else if (OB_FAIL(like_pattern.assign("__idx\\_%\\_"))) {
    LOG_WARN("failed to assign oracle index like prefix", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_index_name.length(); ++i) {
      const char ch = origin_index_name.ptr()[i];
      if ('\\' == ch || '%' == ch || '_' == ch) {
        if (OB_FAIL(like_pattern.append("\\", 1))) {
          LOG_WARN("failed to escape like wildcard", KR(ret), K(ch));
        }
      }
      if (FAILEDx(like_pattern.append(&ch, 1))) {
        LOG_WARN("failed to append origin index name char", KR(ret), K(i));
      }
    }
  }
  return ret;
}

} // namespace

int check_is_oracle_index(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const ObTableNameNamespace table_namespace,
                                  bool &is_oracle_index)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  is_oracle_index = false;
  if (NS_NORMAL_INDEX != table_namespace && NS_BUILT_IN_INDEX != table_namespace) {
    // tables / hidden / aux vp keep encoded table_name keys
  } else if (is_recyclebin_database_id(database_id)
             || is_mysql_sys_database_id(database_id)) {
    // Align ObSchemaMgr: recyclebin uses the generated recycle name;
    // mysql/oceanbase/information_schema on an Oracle tenant stay MySQL-scoped.
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("failed to get tenant mode for oracle index check",
             KR(ret), K(tenant_id), K(database_id), K(table_namespace));
  } else {
    is_oracle_index = (lib::Worker::CompatMode::ORACLE == compat_mode);
  }
  return ret;
}

int try_extract_origin_index_name(const ObString &index_table_name,
                                  ObString &origin_index_name,
                                  bool &is_valid_encoded)
{
  int ret = OB_SUCCESS;
  origin_index_name.reset();
  is_valid_encoded = false;
  if (index_table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoded index table name", KR(ret));
  } else {
    const uint64_t data_table_id =
        ObSimpleTableSchemaV2::extract_data_table_id_from_index_name(index_table_name);
    if (OB_INVALID_ID == data_table_id) {
      // Align ObSchemaMgr::get_index_schema: malformed encoded name is a miss.
    } else if (OB_FAIL(ObSimpleTableSchemaV2::get_index_name(
                   index_table_name, origin_index_name))) {
      if (OB_SCHEMA_ERROR == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("index table name is not in encoded format", K(index_table_name));
      } else {
        LOG_WARN("failed to extract origin index name", KR(ret), K(index_table_name));
      }
    } else {
      is_valid_encoded = !origin_index_name.empty();
    }
  }
  return ret;
}

int resolve_oracle_index_lookup_name(const ObString &index_name,
                                     ObString &origin_index_name,
                                     bool &should_resolve)
{
  int ret = OB_SUCCESS;
  origin_index_name.reset();
  should_resolve = false;
  if (index_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid oracle index lookup name", KR(ret));
  } else if (index_name.prefix_match(OB_INDEX_PREFIX)) {
    bool is_valid_encoded = false;
    if (OB_FAIL(try_extract_origin_index_name(
            index_name, origin_index_name, is_valid_encoded))) {
      LOG_WARN("failed to extract origin index name", KR(ret), K(index_name));
    } else {
      should_resolve = is_valid_encoded;
    }
  } else {
    origin_index_name = index_name;
    should_resolve = true;
  }
  return ret;
}

int resolve_oracle_index_cache_key_name(const ObSimpleTableSchemaV2 &schema,
                                        ObString &name_for_key)
{
  int ret = OB_SUCCESS;
  name_for_key = schema.get_table_name_str();
  if (!schema.get_origin_index_name_str().empty()) {
    name_for_key = schema.get_origin_index_name_str();
  } else {
    ObString origin_index_name;
    bool is_valid_encoded = false;
    if (OB_FAIL(try_extract_origin_index_name(
            schema.get_table_name_str(), origin_index_name, is_valid_encoded))) {
      LOG_WARN("failed to extract origin index name for cache key", KR(ret), K(schema));
    } else if (!is_valid_encoded) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("oracle index cache key missing origin name", KR(ret), K(schema));
    } else {
      name_for_key = origin_index_name;
    }
  }
  return ret;
}

int append_name_predicate(ObSqlString &sql,
                                  const bool is_oracle_index,
                                  const ObString &normalized_name)
{
  int ret = OB_SUCCESS;
  if (normalized_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid normalized name", KR(ret), K(is_oracle_index));
  } else if (!is_oracle_index) {
    if (OB_FAIL(sql.append("table_name = "))) {
      LOG_WARN("failed to append exact name predicate", KR(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(normalized_name, sql))) {
      LOG_WARN("failed to append exact table name", KR(ret), K(normalized_name));
    }
  } else {
    ObSqlString like_pattern;
    if (OB_FAIL(build_oracle_origin_index_like_pattern_(normalized_name, like_pattern))) {
      LOG_WARN("failed to build oracle origin index like pattern",
               KR(ret), K(normalized_name));
    } else if (OB_FAIL(sql.append("table_name LIKE "))) {
      LOG_WARN("failed to append like predicate", KR(ret));
    } else if (OB_FAIL(sql_append_hex_escape_str(like_pattern.string(), sql))) {
      LOG_WARN("failed to append like pattern", KR(ret), K(normalized_name));
    } else if (OB_FAIL(sql.append(" ESCAPE '\\\\'"))) {
      LOG_WARN("failed to append like escape", KR(ret));
    }
  }
  return ret;
}

int stored_table_name_matches_resolve_name(const ObString &resolve_name,
                                           const ObString &stored_table_name,
                                           const bool is_oracle_index,
                                           ObSchemaService &schema_service,
                                           const bool case_compare,
                                           const bool compare_with_collation,
                                           bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (resolve_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name compare argument", KR(ret), K(resolve_name),
             K(stored_table_name), K(is_oracle_index));
  } else if (stored_table_name.empty()) {
    is_equal = false;
  } else if (!is_oracle_index) {
    is_equal = schema_service.schema_name_is_equal(
        resolve_name, stored_table_name, case_compare, compare_with_collation);
  } else {
    ObString origin_index_name;
    if (OB_FAIL(ObSimpleTableSchemaV2::get_index_name(
            stored_table_name, origin_index_name))) {
      if (OB_SCHEMA_ERROR == ret) {
        ret = OB_SUCCESS;
        is_equal = false;
      } else {
        LOG_WARN("failed to extract origin index name from stored table name",
                 KR(ret), K(stored_table_name));
      }
    } else {
      is_equal = schema_service.schema_name_is_equal(
          resolve_name, origin_index_name, case_compare, compare_with_collation);
    }
  }
  return ret;
}

// Keep predicates aligned with is_table_namespace_matched() below.
int append_table_namespace_filter(const ObTableNameNamespace table_namespace,
                                  ObSqlString &filter)
{
  int ret = OB_SUCCESS;
  if (NS_HIDDEN_TABLE == table_namespace) {
    // Hidden namespace covers both hidden data tables and hidden indexes.
    if (OB_FAIL(append_user_hidden_predicate_(true/*expect_hidden*/, filter))) {
      LOG_WARN("failed to append hidden namespace filter", KR(ret));
    }
  } else if (NS_NORMAL_INDEX == table_namespace) {
    if (OB_FAIL(append_user_hidden_predicate_(false/*expect_hidden*/, filter))) {
      LOG_WARN("failed to append non-hidden predicate", KR(ret), K(table_namespace));
    } else if (OB_FAIL(filter.append_fmt(
        "AND table_type = %ld ", static_cast<int64_t>(USER_INDEX)))) {
      LOG_WARN("failed to append normal index table_type filter", KR(ret));
    } else if (OB_FAIL(append_built_in_index_type_predicate_(
        false/*expect_built_in*/, filter))) {
      LOG_WARN("failed to append normal index index_type filter", KR(ret));
    }
  } else if (NS_BUILT_IN_INDEX == table_namespace) {
    if (OB_FAIL(append_user_hidden_predicate_(false/*expect_hidden*/, filter))) {
      LOG_WARN("failed to append non-hidden predicate", KR(ret), K(table_namespace));
    } else if (OB_FAIL(filter.append_fmt(
        "AND table_type = %ld ", static_cast<int64_t>(USER_INDEX)))) {
      LOG_WARN("failed to append built-in index table_type filter", KR(ret));
    } else if (OB_FAIL(append_built_in_index_type_predicate_(
        true/*expect_built_in*/, filter))) {
      LOG_WARN("failed to append built-in index index_type filter", KR(ret));
    }
  } else if (NS_AUX_VP == table_namespace) {
    if (OB_FAIL(append_user_hidden_predicate_(false/*expect_hidden*/, filter))) {
      LOG_WARN("failed to append non-hidden predicate", KR(ret), K(table_namespace));
    } else if (OB_FAIL(filter.append_fmt(
        "AND table_type = %ld ",
        static_cast<int64_t>(AUX_VERTIAL_PARTITION_TABLE)))) {
      LOG_WARN("failed to append aux vp table_type filter", KR(ret));
    }
  } else if (NS_NORMAL_TABLE == table_namespace) {
    if (OB_FAIL(append_user_hidden_predicate_(false/*expect_hidden*/, filter))) {
      LOG_WARN("failed to append non-hidden predicate", KR(ret), K(table_namespace));
    } else if (OB_FAIL(filter.append_fmt(
        "AND table_type != %ld AND table_type != %ld "
        "AND table_type != %ld AND table_type != %ld ",
        static_cast<int64_t>(USER_INDEX),
        static_cast<int64_t>(AUX_VERTIAL_PARTITION_TABLE),
        static_cast<int64_t>(AUX_LOB_PIECE),
        static_cast<int64_t>(AUX_LOB_META)))) {
      LOG_WARN("failed to append normal table type filter", KR(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table namespace", KR(ret), K(table_namespace));
  }
  return ret;
}

bool is_table_namespace_matched(const ObTableNameNamespace table_namespace,
                                const ObTableType table_type,
                                const ObTableMode &table_mode,
                                const ObIndexType index_type)
{
  bool is_matched = false;
  if (NS_HIDDEN_TABLE == table_namespace) {
    is_matched = table_mode.is_user_hidden_table();
  } else if (NS_NORMAL_INDEX == table_namespace) {
    is_matched = !table_mode.is_user_hidden_table()
                 && is_index_table(table_type)
                 && !is_built_in_index(index_type);
  } else if (NS_BUILT_IN_INDEX == table_namespace) {
    is_matched = !table_mode.is_user_hidden_table()
                 && is_index_table(table_type)
                 && is_built_in_index(index_type);
  } else if (NS_AUX_VP == table_namespace) {
    is_matched = !table_mode.is_user_hidden_table()
                 && AUX_VERTIAL_PARTITION_TABLE == table_type;
  } else if (NS_NORMAL_TABLE == table_namespace) {
    is_matched = !table_mode.is_user_hidden_table()
                 && !is_index_table(table_type)
                 && AUX_VERTIAL_PARTITION_TABLE != table_type
                 && !is_aux_lob_table(table_type);
  }
  return is_matched;
}

int read_table_resolve_row_by_name(sqlclient::ObMySQLResult &result,
                                   ObSchemaService &schema_service,
                                   const ObTableNameNamespace table_namespace,
                                   const uint64_t database_id,
                                   const ObString &input_table_name,
                                   const bool case_compare,
                                   const bool compare_with_collation,
                                   const bool is_oracle_index,
                                   ObSchemaResolveRange &version_range,
                                   bool &is_matched)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  int64_t schema_version = OB_INVALID_VERSION;
  bool is_deleted = false;
  ObTableType table_type = MAX_TABLE_TYPE;
  ObTableMode table_mode;
  ObIndexType index_type = INDEX_TYPE_IS_NOT;
  ObString table_name;
  bool name_equal = false;
  is_matched = false;
  UNUSED(database_id);
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", table_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "table_name", table_name);
  EXTRACT_INT_FIELD_MYSQL(result, "schema_version", schema_version, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "is_deleted", is_deleted, bool);
  EXTRACT_INT_FIELD_MYSQL(result, "table_type", table_type, ObTableType);
  EXTRACT_INT_FIELD_MYSQL(result, "table_mode", table_mode.mode_, uint32_t);
  EXTRACT_INT_FIELD_MYSQL(result, "index_type", index_type, ObIndexType);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to extract resolve row", KR(ret));
  } else if (OB_FAIL(stored_table_name_matches_resolve_name(
                 input_table_name, table_name, is_oracle_index,
                 schema_service, case_compare, compare_with_collation, name_equal))) {
    LOG_WARN("failed to compare resolve name with history table name", KR(ret),
             K(input_table_name), "history_table_name", table_name,
             K(is_oracle_index));
  } else if (!name_equal) {
    // __all_table_history uses a case-insensitive collation. In mode 0 the SQL
    // predicate intentionally returns all case variants and C++ filters them.
    // Oracle origin-index LIKE may also return suffix false-positives.
    LOG_TRACE("[OnDemandNameResolve] skip name-mismatched history row",
              K(input_table_name), "history_table_name", table_name,
              K(table_namespace), K(table_id), K(schema_version), K(is_deleted),
              K(table_type), K(index_type), K(is_oracle_index));
  } else if (is_table_namespace_matched(table_namespace, table_type, table_mode, index_type)) {
    LOG_TRACE("[OnDemandNameResolve] history row matched",
             K(input_table_name), "history_table_name", table_name,
             K(table_namespace), K(table_id), K(schema_version), K(is_deleted),
             K(table_type), K(index_type));
    version_range.table_namespace_ = table_namespace;
    version_range.schema_id_ = table_id;
    version_range.precise_schema_version_ = schema_version;
    version_range.start_version_ = schema_version;
    version_range.is_exist_ = !is_deleted;
    is_matched = true;
  } else {
    // Namespace filter is pushed into SQL; a remaining mismatch means SQL/C++ drifted.
    // Keep treating as miss but surface it for diagnostics (soft miss, not hard fail).
    LOG_WARN("[OnDemandNameResolve] history row namespace mismatch after SQL filter",
             K(input_table_name), "history_table_name", table_name,
             K(table_namespace), K(table_id), K(schema_version), K(is_deleted),
             K(table_type), "table_mode", table_mode.mode_, K(index_type));
  }
  return ret;
}

int weak_read_sql(ObISQLClient &sql_client,
                  const int64_t snapshot_timestamp,
                  const uint64_t exec_tenant_id,
                  const char *sql,
                  ObMySQLProxy::MySQLResult &res)
{
  int ret = OB_SUCCESS;
  const bool check_sys_variable = true;
  ObSQLClientRetryWeak sql_client_retry_weak(
      &sql_client, false, snapshot_timestamp, check_sys_variable);
  if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id));
    LOG_TRACE("failed to execute sql", KR(ret), K(exec_tenant_id), "sql", sql);
  }
  return ret;
}

} // namespace schema_resolve_util
} // namespace schema
} // namespace share
} // namespace oceanbase
