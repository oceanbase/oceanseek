/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T> class ObIArray;
}
namespace sql
{
class ObBasicSessionInfo;
}
namespace share
{
class ObUnifiedCatalogConfig;
namespace schema
{
class ObTableSchema;
}

/** Resolves the tenant UC endpoint and the remote synchronization target. */
class ObUnifiedCatalog
{
public:
  static bool is_configured(const uint64_t tenant_id);
  static bool is_child_catalog_id(const uint64_t tenant_id, const uint64_t catalog_id);
  static int get_session_token(const sql::ObBasicSessionInfo &session,
                               common::ObIAllocator &allocator,
                               common::ObString &access_token);
  static int catalog_exists(const uint64_t tenant_id,
                            const common::ObString &access_token,
                            const common::ObString &catalog_name,
                            common::ObIAllocator &allocator,
                            bool &exists);
  static int validate_sync_target(const uint64_t tenant_id,
                                  const common::ObString &access_token);
  static int validate_sync_target(const uint64_t tenant_id,
                                  const common::ObString &access_token,
                                  common::ObIAllocator &allocator,
                                  common::ObString &remote_catalog_name,
                                  common::ObString &endpoint,
                                  common::ObString &realm);
  static int resolve_target(const uint64_t tenant_id,
                            common::ObIAllocator &allocator,
                            common::ObString &remote_catalog_name,
                            ObUnifiedCatalogConfig *config = NULL);
  // Keep this raw snapshot silent so callers can defer diagnostics and preserve local DDL error priority.
  static int snapshot_raw_target(const uint64_t tenant_id,
                                 common::ObIAllocator &allocator,
                                 common::ObString &catalog_sync_target,
                                 common::ObString &unified_catalog_config);
  static int validate_namespace_target(
      const uint64_t tenant_id,
      const common::ObString &expected_remote_catalog_name,
      const common::ObString &expected_endpoint,
      const common::ObString &expected_realm);

  static int create_namespace(const uint64_t tenant_id,
                              const common::ObString &access_token,
                              const common::ObString &database_name,
                              const uint64_t database_id,
                              const common::ObString &expected_remote_catalog_name = common::ObString(),
                              const common::ObString &expected_endpoint = common::ObString(),
                              const common::ObString &expected_realm = common::ObString());
  static int check_namespace_empty(const uint64_t tenant_id,
                                   const common::ObString &access_token,
                                   const common::ObString &database_name,
                                   const uint64_t database_id,
                                   const common::ObString &expected_remote_catalog_name = common::ObString(),
                                   const common::ObString &expected_endpoint = common::ObString(),
                                   const common::ObString &expected_realm = common::ObString());
  static int drop_namespace(const uint64_t tenant_id,
                            const common::ObString &access_token,
                            const common::ObString &database_name,
                            const common::ObString &expected_remote_catalog_name = common::ObString(),
                            const common::ObString &expected_endpoint = common::ObString(),
                            const common::ObString &expected_realm = common::ObString());
  static int sync_database(const uint64_t tenant_id,
                           const common::ObString &access_token,
                           const common::ObString &expected_remote_catalog_name,
                           const common::ObString &database_name,
                           const uint64_t database_id,
                           const common::ObIArray<const schema::ObTableSchema *> &table_schemas);
  static int reserve_create_table(const uint64_t tenant_id,
                                  const common::ObString &access_token,
                                  const common::ObString &database_name,
                                  const uint64_t database_id,
                                  const schema::ObTableSchema &table_schema,
                                  const common::ObString &operation_id,
                                  common::ObIAllocator &allocator,
                                  common::ObString &remote_catalog_name,
                                  common::ObString &endpoint,
                                  common::ObString &realm);
  static int complete_create_table(const uint64_t tenant_id,
                                   const common::ObString &access_token,
                                   const common::ObString &expected_remote_catalog_name,
                                   const common::ObString &expected_endpoint,
                                   const common::ObString &expected_realm,
                                   const common::ObString &database_name,
                                   const schema::ObTableSchema &table_schema,
                                   const common::ObString &operation_id);
  // Cancel this operation's PREPARING reservation using its original target only
  // after local creation has been ruled out.
  static int cancel_create_table(const uint64_t tenant_id,
                                 const common::ObString &access_token,
                                 const common::ObString &expected_remote_catalog_name,
                                 const common::ObString &expected_endpoint,
                                 const common::ObString &expected_realm,
                                 const common::ObString &database_name,
                                 const common::ObString &table_name,
                                 const common::ObString &operation_id);
  static int sync_table(const uint64_t tenant_id,
                        const common::ObString &access_token,
                        const common::ObString &expected_remote_catalog_name,
                        const common::ObString &database_name,
                        const schema::ObTableSchema &table_schema,
                        const bool recover_preparing = false);
  static int sync_table_to_target(
      const uint64_t tenant_id,
      const common::ObString &access_token,
      const common::ObString &expected_remote_catalog_name,
      const common::ObString &expected_endpoint,
      const common::ObString &expected_realm,
      const common::ObString &database_name,
      const schema::ObTableSchema &table_schema,
      const bool recover_preparing = false);
  static int drop_table(const uint64_t tenant_id,
                        const common::ObString &access_token,
                        const common::ObString &database_name,
                        const common::ObString &table_name,
                        const common::ObString &expected_raw_target,
                        const common::ObString &expected_raw_config);

private:
  static int validate_expected_config(
      const common::ObString &expected_remote_catalog_name,
      const common::ObString &configured_remote_catalog_name);
  static int validate_expected_namespace_target(
      const common::ObString &expected_remote_catalog_name,
      const common::ObString &expected_endpoint,
      const common::ObString &expected_realm,
      const common::ObString &configured_remote_catalog_name,
      const ObUnifiedCatalogConfig &configured_config);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_H_
