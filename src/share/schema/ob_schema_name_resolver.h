/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_NAME_RESOLVER_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_NAME_RESOLVER_H_

#include "share/schema/ob_simple_schema_kv_cache.h"
#include "share/schema/ob_schema_struct.h"

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

struct ObRefreshSchemaStatus;
class ObSchemaService;

// Resolve table_name (+ session_id) to table_id at snapshot (phase A), then reuse
// ObSchemaIDResolver for that object's precise_schema_version and start/end version (phase B).
class ObSchemaNameResolver
{
public:
  static int get_schema_version_range_by_name(common::ObISQLClient &sql_client,
                                              const ObRefreshSchemaStatus &schema_status,
                                              const uint64_t tenant_id,
                                              const ObTableNameNamespace table_namespace,
                                              const uint64_t database_id,
                                              const common::ObString &table_name,
                                              const uint64_t session_id,
                                              const int64_t snapshot_version,
                                              const common::ObNameCaseMode name_case_mode,
                                              ObSchemaResolveRange &version_range);

  // Resolve against one exact session_id only (no session_id=0 fallback).
  // Used by the L1 name-cache path so session-private and global entries stay separate.
  // key.cache_generation_ is ignored; it is only part of the L1 cache identity.
  static int get_schema_version_range_by_name_for_session(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const ObSchemaNameResolveKey &key,
      const common::ObString &table_name,
      const int64_t snapshot_version,
      const common::ObNameCaseMode name_case_mode,
      ObSchemaResolveRange &version_range);

private:
  // Prefer current-session temp table, then fall back to session_id=0.
  // On a name hit, always refill object-level versions via table_id.
  static int resolve_table_by_name_for_session_(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const ObSchemaNameResolveKey &key,
      const common::ObString &table_name,
      const int64_t snapshot_version,
      const common::ObNameCaseMode name_case_mode,
      ObSchemaService &schema_service,
      ObSchemaResolveRange &version_range);
  // table_id -> is_exist / precise_schema_version / start_version / end_version.
  static int fill_version_range_by_table_id_(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const ObSchemaNameResolveKey &key,
      const common::ObString &table_name,
      const int64_t snapshot_version,
      const common::ObNameCaseMode name_case_mode,
      ObSchemaService &schema_service,
      ObSchemaResolveRange &version_range);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SCHEMA_NAME_RESOLVER_H_
