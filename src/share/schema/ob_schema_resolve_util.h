/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_RESOLVE_UTIL_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_RESOLVE_UTIL_H_

#include "share/schema/ob_simple_schema_kv_cache.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObString;
class ObSqlString;
}
namespace share
{
namespace schema
{

class ObSchemaService;

// Shared helpers for on-demand name/id resolve SQL paths (used by name_resolver
// and simple_schema_lookup). Keep contracts aligned with ObSchemaNameResolver
// phase A/B: namespace-safe history row pick, then fill ObSchemaResolveRange.
namespace schema_resolve_util
{

// Append SQL predicates equivalent to is_table_namespace_matched() so
// ORDER BY schema_version DESC LIMIT 1 cannot pick a same-name row from
// another namespace. Built-in index_type values come from iterating
// ObIndexType and calling is_built_in_index().
// @param[in]  table_namespace  target namespace (normal/index/hidden/...)
// @param[out] filter           SQL fragment appended in-place
int append_table_namespace_filter(const ObTableNameNamespace table_namespace,
                                  common::ObSqlString &filter);

// Check whether one full table-history row belongs to the requested name namespace.
// Keep this predicate aligned with append_table_namespace_filter().
bool is_table_namespace_matched(const ObTableNameNamespace table_namespace,
                                const ObTableType table_type,
                                const ObTableMode &table_mode,
                                const ObIndexType index_type);

// Oracle user-index names are unique within a database (SchemaMgr index_name_map
// uses origin_index_name + data_table_id=INVALID). Recyclebin / mysql-sys-db /
// hidden indexes stay encoded-table-name resolve.
//
// Write / read / backsource must share this predicate:
//   write  : incremental name-map key uses origin_index_name
//   read   : peel __idx_<id>_ then resolve by origin_index_name
//   source : LIKE encoded physical name, then peel+compare origin
int check_is_oracle_index(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const ObTableNameNamespace table_namespace,
                                  bool &is_oracle_index);

// Peel __idx_<data_table_id>_<origin> the same way ObSchemaMgr::get_index_schema
// does. Invalid encoding is a miss (is_valid_encoded=false), not an error.
int try_extract_origin_index_name(const common::ObString &index_table_name,
                                  common::ObString &origin_index_name,
                                  bool &is_valid_encoded);

// Accept encoded __idx_<id>_<origin> or bare origin_index_name for Oracle
// user-index name-map lookup. Malformed encoded names are a miss.
int resolve_oracle_index_lookup_name(const common::ObString &index_name,
                                     common::ObString &origin_index_name,
                                     bool &should_resolve);

// Pick origin_index_name for name-map cache write. Caller must gate with
// check_is_oracle_index(); returns OB_ERR_UNEXPECTED when the
// schema has no usable origin name.
int resolve_oracle_index_cache_key_name(const ObSimpleTableSchemaV2 &schema,
                                        common::ObString &name_for_key);

// Exact `table_name =` for normal names; Oracle origin-index uses
// `table_name LIKE '__idx\_%\_<origin>' ESCAPE '\'` because __all_table stores
// the encoded physical name.
int append_name_predicate(common::ObSqlString &sql,
                                  const bool is_oracle_index,
                                  const common::ObString &normalized_name);

// Compare a history/cache stored table_name with the resolve key.
// Oracle origin-index: peel __idx_<id>_ and compare origin names.
int stored_table_name_matches_resolve_name(const common::ObString &resolve_name,
                                           const common::ObString &stored_table_name,
                                           const bool is_oracle_index,
                                           ObSchemaService &schema_service,
                                           const bool case_compare,
                                           const bool compare_with_collation,
                                           bool &is_equal);

// Parse one __all_table_history row for phase-A name resolve.
// On namespace + name match, fills version_range start/precise/schema_id/is_exist
// and sets is_matched=true; otherwise leaves is_matched=false.
// @param[in]  result                 current MySQL result row
// @param[in]  schema_service         for schema_name_is_equal()
// @param[in]  table_namespace        expected namespace
// @param[in]  database_id            unused today; kept for call-site symmetry
// @param[in]  input_table_name       original lookup name
// @param[in]  case_compare / compare_with_collation  name equality mode
// @param[in]  is_oracle_index  peel encoded __idx_<id>_ before compare
// @param[out] version_range          filled only when is_matched
// @param[out] is_matched             whether this row is the authoritative hit
int read_table_resolve_row_by_name(common::sqlclient::ObMySQLResult &result,
                                   ObSchemaService &schema_service,
                                   const ObTableNameNamespace table_namespace,
                                   const uint64_t database_id,
                                   const common::ObString &input_table_name,
                                   const bool case_compare,
                                   const bool compare_with_collation,
                                   const bool is_oracle_index,
                                   ObSchemaResolveRange &version_range,
                                   bool &is_matched);

// Weak-consistent SELECT at snapshot_timestamp for schema history backsource.
// @param[in]  sql_client          SQL client
// @param[in]  snapshot_timestamp  read snapshot (usec)
// @param[in]  exec_tenant_id      tenant that owns the history tables
// @param[in]  sql                 full SELECT text
// @param[out] res                 MySQL result holder
int weak_read_sql(common::ObISQLClient &sql_client,
                  const int64_t snapshot_timestamp,
                  const uint64_t exec_tenant_id,
                  const char *sql,
                  common::ObMySQLProxy::MySQLResult &res);

} // namespace schema_resolve_util
} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SCHEMA_RESOLVE_UTIL_H_
