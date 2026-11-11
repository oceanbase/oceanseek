/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_HELPER_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_HELPER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace obrpc
{
struct ObAlterTableArg;
}
namespace share
{
struct CatalogPartitionValues;
class ObCatalogMetaWriter;
namespace schema
{
class ObTableSchema;
class ObSchemaGetterGuard;
}
}
namespace sql
{

class ObCatalogSyncHelper
{
public:
  // Guard-free switch: read TENANT_CONF catalog_sync_target and unified_catalog_config.
  static bool is_sync_enabled(const uint64_t tenant_id);
  static bool need_reconcile_partitions(const obrpc::ObAlterTableArg &alter_table_arg);
  static int get_refreshed_schema_guard(const uint64_t tenant_id,
                                        share::schema::ObSchemaGetterGuard &schema_guard);
  static int sync_table_to_hms(
      const uint64_t tenant_id,
      const uint64_t sync_catalog_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema);
  static int sync_rename_table(const uint64_t tenant_id,
                               const uint64_t sync_catalog_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const common::ObString &origin_database_name,
                               const common::ObString &origin_table_name,
                               const common::ObString &new_database_name,
                               const common::ObString &new_table_name,
                               const share::schema::ObTableSchema &table_schema);
  static int reconcile_hms_partitions(
      const uint64_t tenant_id,
      const uint64_t sync_catalog_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema);
  static int sync_table(const uint64_t tenant_id,
                        const common::ObString &access_token,
                        const uint64_t catalog_id,
                        const common::ObString &expected_remote_catalog_name,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        const common::ObString &database_name,
                        const share::schema::ObTableSchema &table_schema);
  static int sync_database(const uint64_t tenant_id,
                           const common::ObString &access_token,
                           const uint64_t catalog_id,
                           const common::ObString &expected_remote_catalog_name,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           const uint64_t database_id,
                           const common::ObString &database_name);
  static int sync_catalog(const uint64_t tenant_id,
                          const common::ObString &access_token,
                          const uint64_t catalog_id,
                          const common::ObString &expected_remote_catalog_name,
                          share::schema::ObSchemaGetterGuard &schema_guard);
  static bool is_syncable_table(const share::schema::ObTableSchema &table_schema);

private:
  static bool is_syncable_database(const uint64_t database_id);
  static int check_sync_target(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               share::schema::ObSchemaGetterGuard &schema_guard);
  static int sync_table_to_catalog(const uint64_t tenant_id,
                                   const uint64_t catalog_id,
                                   share::ObCatalogMetaWriter &meta_writer,
                                   const common::ObString &database_name,
                                   const share::schema::ObTableSchema &table_schema);
  static int reconcile_hms_partitions_with_values(
      const uint64_t tenant_id,
      const uint64_t sync_catalog_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<share::CatalogPartitionValues> &expected_partition_values);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_HELPER_H_ */
