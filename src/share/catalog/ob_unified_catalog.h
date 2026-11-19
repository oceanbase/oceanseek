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

class ObUnifiedCatalogSyncTarget
{
public:
  ObUnifiedCatalogSyncTarget(const uint64_t tenant_id,
                             const common::ObString &access_token);

  // Captures the current target configuration without contacting Unified Catalog.
  int capture(common::ObIAllocator &allocator);
  uint64_t tenant_id() const { return tenant_id_; }
  const common::ObString &access_token() const { return access_token_; }
  const common::ObString &catalog_name() const { return catalog_name_; }
  const common::ObString &endpoint() const { return endpoint_; }
  const common::ObString &realm() const { return realm_; }
  bool is_captured() const
  {
    return !access_token_.empty() && !catalog_name_.empty() && !endpoint_.empty()
      && !realm_.empty();
  }

private:
  void reset_snapshot()
  {
    catalog_name_.reset();
    endpoint_.reset();
    realm_.reset();
  }

private:
  uint64_t tenant_id_;
  common::ObString access_token_;
  common::ObString catalog_name_;
  common::ObString endpoint_;
  common::ObString realm_;
};

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
  static int validate_sync_target(const ObUnifiedCatalogSyncTarget &target);
  static int resolve_target(const uint64_t tenant_id,
                            common::ObIAllocator &allocator,
                            common::ObString &remote_catalog_name,
                            ObUnifiedCatalogConfig *config = NULL);
  // Keep this raw snapshot silent so callers can defer diagnostics and preserve local DDL error priority.
  static int snapshot_raw_target(const uint64_t tenant_id,
                                 common::ObIAllocator &allocator,
                                 common::ObString &catalog_sync_target,
                                 common::ObString &unified_catalog_config);
  static int create_namespace(const ObUnifiedCatalogSyncTarget &target,
                              const common::ObString &database_name,
                              const uint64_t database_id);
  static int check_namespace_empty(const ObUnifiedCatalogSyncTarget &target,
                                   const common::ObString &database_name,
                                   const uint64_t database_id);
  static int drop_namespace(const ObUnifiedCatalogSyncTarget &target,
                            const common::ObString &database_name);
  static int sync_database(const uint64_t tenant_id,
                           const common::ObString &access_token,
                           const common::ObString &expected_remote_catalog_name,
                           const common::ObString &database_name,
                           const uint64_t database_id,
                           const common::ObIArray<const schema::ObTableSchema *> &table_schemas);
  static int reserve_create_table(const ObUnifiedCatalogSyncTarget &target,
                                  const common::ObString &database_name,
                                  const uint64_t database_id,
                                  const schema::ObTableSchema &table_schema,
                                  const common::ObString &operation_id);
  static int complete_create_table(const ObUnifiedCatalogSyncTarget &target,
                                   const common::ObString &database_name,
                                   const schema::ObTableSchema &table_schema,
                                   const common::ObString &operation_id);
  // Cancel this operation's PREPARING reservation using its original target only
  // after local creation has been ruled out.
  static int cancel_create_table(const ObUnifiedCatalogSyncTarget &target,
                                 const common::ObString &database_name,
                                 const common::ObString &table_name,
                                 const common::ObString &operation_id);
  static int sync_table(const uint64_t tenant_id,
                        const common::ObString &access_token,
                        const common::ObString &expected_remote_catalog_name,
                        const common::ObString &database_name,
                        const schema::ObTableSchema &table_schema,
                        const bool recover_preparing = false);
  static int sync_table_to_target(const ObUnifiedCatalogSyncTarget &target,
                                  const common::ObString &database_name,
                                  const schema::ObTableSchema &table_schema,
                                  const bool recover_preparing = false);
  // The expected target and source table ID are captured before local DDL;
  // table_schema and final_table_schema describe the successful local result.
  static int update_table_to_target(const ObUnifiedCatalogSyncTarget &target,
                                    const common::ObString &database_name,
                                    const schema::ObTableSchema &table_schema,
                                    const uint64_t expected_source_table_id);
  static int rename_table_to_target(const ObUnifiedCatalogSyncTarget &target,
                                    const common::ObString &source_database_name,
                                    const common::ObString &source_table_name,
                                    const common::ObString &destination_database_name,
                                    const schema::ObTableSchema &final_table_schema,
                                    const uint64_t expected_source_table_id);
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
