/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
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
struct ObTableItem;
struct ObRenameTableArg;
struct ObAlterTableArg;
struct ObFlashBackTableFromRecyclebinArg;
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
  // Guard-free switch: read TENANT_CONF catalog_sync_target only.
  static bool is_sync_enabled(const uint64_t tenant_id);
  static bool need_reconcile_partitions(const obrpc::ObAlterTableArg &alter_table_arg);
  static int sync_create_database(const uint64_t tenant_id,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  const common::ObString &database_name);
  static int sync_drop_database(const uint64_t tenant_id,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                const common::ObString &database_name);
  static int check_drop_database_with_remote_tables(const uint64_t tenant_id,
                                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                                    const common::ObString &database_name);
  static int sync_create_table(const uint64_t tenant_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const common::ObString &database_name,
                               const share::schema::ObTableSchema &table_schema);
  static int prepare_flashback_table_sync(
      const uint64_t tenant_id,
      const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
      uint64_t &recycle_table_id);
  static int sync_flashback_table(
      const uint64_t tenant_id,
      const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
      const uint64_t recycle_table_id);
  static int sync_alter_table(const uint64_t tenant_id,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const common::ObString &database_name,
                              const share::schema::ObTableSchema &table_schema);
  static int sync_rename_table(const uint64_t tenant_id,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const common::ObString &origin_database_name,
                               const common::ObString &origin_table_name,
                               const common::ObString &new_database_name,
                               const common::ObString &new_table_name,
                               const share::schema::ObTableSchema &table_schema);
  static int sync_rename_table_before(const uint64_t tenant_id,
                                      const obrpc::ObRenameTableArg &rename_table_arg,
                                      bool &out_remote_renamed);
  static int sync_rename_table_rollback(const uint64_t tenant_id,
                                        const obrpc::ObRenameTableArg &rename_table_arg);
  static int reconcile_partitions(const uint64_t tenant_id,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  const common::ObString &database_name,
                                  const share::schema::ObTableSchema &table_schema);
  static int sync_drop_table(const uint64_t tenant_id,
                             share::schema::ObSchemaGetterGuard &schema_guard,
                             const common::ObString &database_name,
                             const common::ObString &table_name);
  // Batch drop the HMS placeholders for a list of tables (builds its own schema guard,
  // idempotent per table). Used by DROP TABLE so the executor stays boilerplate-free.
  static int sync_drop_tables(const uint64_t tenant_id,
                              const common::ObIArray<obrpc::ObTableItem> &table_items);
  static int sync_table(const uint64_t tenant_id,
                        const uint64_t catalog_id,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        const common::ObString &database_name,
                        const share::schema::ObTableSchema &table_schema);
  static int sync_database(const uint64_t tenant_id,
                           const uint64_t catalog_id,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           const uint64_t database_id,
                           const common::ObString &database_name);
  static int sync_catalog(const uint64_t tenant_id,
                          const uint64_t catalog_id,
                          share::schema::ObSchemaGetterGuard &schema_guard);

private:
  static bool is_syncable_database(const uint64_t database_id);
  static bool is_syncable_table(const share::schema::ObTableSchema &table_schema);
  static int check_sync_target(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               share::schema::ObSchemaGetterGuard &schema_guard);
  static int sync_table_to_catalog(const uint64_t tenant_id,
                                   const uint64_t catalog_id,
                                   share::ObCatalogMetaWriter &meta_writer,
                                   const common::ObString &database_name,
                                   const share::schema::ObTableSchema &table_schema);
  static int get_refreshed_schema_guard(const uint64_t tenant_id,
                                        share::schema::ObSchemaGetterGuard &schema_guard);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_HELPER_H_ */
