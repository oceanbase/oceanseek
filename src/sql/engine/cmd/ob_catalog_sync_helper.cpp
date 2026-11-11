/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/catalog/ob_catalog_partition_values.h"
#include "share/catalog/ob_catalog_type_mapping.h"
#include "share/catalog/ob_catalog_meta_getter.h"
#include "share/catalog/ob_catalog_meta_writer.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

int ObCatalogSyncHelper::get_refreshed_schema_guard(const uint64_t tenant_id,
                                                    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t local_version = OB_INVALID_VERSION;
  int64_t global_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_refreshed_schema_version(tenant_id,
                                                                               local_version))) {
    LOG_WARN("failed to get local schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(tenant_id,
                                                                                 global_version))) {
    LOG_WARN("failed to get global schema version", KR(ret), K(tenant_id));
  } else if (local_version < global_version
             && OB_FAIL(GCTX.schema_service_->async_refresh_schema(tenant_id, global_version))) {
    LOG_WARN("failed to refresh schema",
             KR(ret),
             K(tenant_id),
             K(local_version),
             K(global_version));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id));
  }
  return ret;
}

bool ObCatalogSyncHelper::is_syncable_database(const uint64_t database_id)
{
  return OB_INVALID_ID != database_id && !is_sys_database_id(database_id);
}

bool ObCatalogSyncHelper::is_syncable_table(const ObTableSchema &table_schema)
{
  return table_schema.is_user_table() && !table_schema.is_index_table()
         && !table_schema.is_aux_table() && is_syncable_database(table_schema.get_database_id());
}

int ObCatalogSyncHelper::check_sync_target(const uint64_t tenant_id,
                                           const uint64_t catalog_id,
                                           ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t configured_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, configured_catalog_id))) {
    LOG_WARN("failed to get configured catalog sync target", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == configured_catalog_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "catalog sync without catalog_sync_target");
  } else if (catalog_id != configured_catalog_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "sync to catalog other than catalog_sync_target");
  }
  return ret;
}

int ObCatalogSyncHelper::sync_table_to_catalog(const uint64_t tenant_id,
                                               const uint64_t catalog_id,
                                               ObCatalogMetaWriter &meta_writer,
                                               const ObString &database_name,
                                               const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<CatalogPartitionValues> expected_partition_values;
  if (OB_FAIL(meta_writer.create_table(
          tenant_id, catalog_id, database_name, table_schema))) {
    if (OB_ERR_ALREADY_EXISTS == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(meta_writer.alter_table(
              tenant_id, catalog_id, database_name, table_schema))) {
        LOG_WARN("failed to update catalog table", K(ret), K(database_name),
                 K(table_schema.get_table_name_str()), K(catalog_id));
      }
    } else {
      LOG_WARN("failed to create catalog table", K(ret), K(database_name),
               K(table_schema.get_table_name_str()), K(catalog_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObCatalogTypeMapping::build_all_partition_values(
                 table_schema, expected_partition_values))) {
    LOG_WARN("failed to build partition values", K(ret), K(database_name),
             K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(meta_writer.reconcile_partitions(
                 tenant_id,
                 catalog_id,
                 database_name,
                 table_schema,
                 expected_partition_values))) {
    LOG_WARN("failed to reconcile catalog partitions", K(ret), K(database_name),
             K(table_schema.get_table_name_str()), K(catalog_id));
  }
  return ret;
}

bool ObCatalogSyncHelper::is_sync_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    const char *sync_target = tenant_config->catalog_sync_target.str();
    const char *unified_target = tenant_config->unified_catalog_config.str();
    enabled = (NULL != sync_target && '\0' != sync_target[0])
              || (NULL != unified_target && '\0' != unified_target[0]);
  }
  return enabled;
}

bool ObCatalogSyncHelper::need_reconcile_partitions(
    const obrpc::ObAlterTableArg &alter_table_arg)
{
  bool need_reconcile = false;
  switch (alter_table_arg.alter_part_type_) {
    case obrpc::ObAlterTableArg::ADD_PARTITION:
    case obrpc::ObAlterTableArg::DROP_PARTITION:
    case obrpc::ObAlterTableArg::PARTITIONED_TABLE:
    case obrpc::ObAlterTableArg::PARTITIONED_PARTITION:
    case obrpc::ObAlterTableArg::REORGANIZE_PARTITION:
    case obrpc::ObAlterTableArg::SPLIT_PARTITION:
    case obrpc::ObAlterTableArg::ADD_SUB_PARTITION:
    case obrpc::ObAlterTableArg::DROP_SUB_PARTITION:
    case obrpc::ObAlterTableArg::REPARTITION_TABLE:
    case obrpc::ObAlterTableArg::SET_INTERVAL:
    case obrpc::ObAlterTableArg::INTERVAL_TO_RANGE:
    case obrpc::ObAlterTableArg::RENAME_PARTITION:
    case obrpc::ObAlterTableArg::RENAME_SUB_PARTITION:
    case obrpc::ObAlterTableArg::AUTO_SPLIT_PARTITION:
    case obrpc::ObAlterTableArg::EXCHANGE_PARTITION:
    case obrpc::ObAlterTableArg::EXCHANGE_SUBPARTITION:
      need_reconcile = true;
      break;
    default:
      // TRUNCATE and storage-cache-policy changes do not change the physical
      // HMS partition set. Ordinary ALTER TABLE operations also skip reconcile.
      break;
  }
  return need_reconcile;
}

int ObCatalogSyncHelper::sync_table_to_hms(
    const uint64_t tenant_id,
    const uint64_t sync_catalog_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &database_name,
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != sync_catalog_id) {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(
            meta_writer.create_table(tenant_id, sync_catalog_id, database_name, table_schema))) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_INVALID_ARGUMENT == ret) {
        const int create_table_ret = ret;
        ObCatalogMetaGetter meta_getter(schema_guard, allocator);
        ObDatabaseSchema *database_schema = NULL;
        const uint64_t probe_database_id = OB_MIN_EXTERNAL_OBJECT_ID + 1;
        int ns_ret = meta_getter.fetch_namespace_schema(tenant_id,
                                                        sync_catalog_id,
                                                        probe_database_id,
                                                        database_name,
                                                        OB_ORIGIN_AND_INSENSITIVE,
                                                        database_schema);
        if (OB_FAIL(ns_ret)) {
          if (OB_ERR_BAD_DATABASE == ns_ret) {
            if (OB_FAIL(meta_writer.create_namespace(tenant_id, sync_catalog_id, database_name))) {
              if (OB_ERR_ALREADY_EXISTS == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to create missing namespace for table sync",
                         KR(ret),
                         K(database_name),
                         K(sync_catalog_id));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(meta_writer.create_table(tenant_id,
                                                        sync_catalog_id,
                                                        database_name,
                                                        table_schema))) {
              LOG_WARN("failed to sync create table to HMS after creating namespace",
                       KR(ret),
                       K(database_name),
                       K(table_schema.get_table_name_str()),
                       K(sync_catalog_id));
            } else {
              LOG_INFO("synced create table to HMS after creating missing namespace",
                       K(database_name),
                       K(table_schema.get_table_name_str()),
                       K(sync_catalog_id));
            }
          } else {
            ret = ns_ret;
            LOG_WARN("failed to check namespace before retry create table",
                     KR(ret),
                     K(database_name),
                     K(sync_catalog_id));
          }
        } else {
          ret = create_table_ret;
          LOG_WARN("namespace exists but create table still reported not exist",
                   KR(ret),
                   K(database_name),
                   K(table_schema.get_table_name_str()),
                   K(sync_catalog_id));
        }
      } else {
        LOG_WARN("failed to sync create table to HMS",
                 KR(ret),
                 K(database_name),
                 K(table_schema.get_table_name_str()),
                 K(sync_catalog_id));
      }
    } else {
      LOG_INFO("synced create table to HMS",
               K(database_name),
               K(table_schema.get_table_name_str()),
               K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_rename_table(const uint64_t tenant_id,
                                           const uint64_t sync_catalog_id,
                                           ObSchemaGetterGuard &schema_guard,
                                           const ObString &origin_database_name,
                                           const ObString &origin_table_name,
                                           const ObString &new_database_name,
                                           const ObString &new_table_name,
                                           const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != sync_catalog_id) {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.rename_table(tenant_id,
                                         sync_catalog_id,
                                         origin_database_name,
                                         origin_table_name,
                                         new_database_name,
                                         new_table_name,
                                         table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        LOG_INFO("origin table does not exist in HMS, skip remote rename",
                 K(origin_database_name),
                 K(origin_table_name),
                 K(new_database_name),
                 K(new_table_name),
                 K(sync_catalog_id));
      } else {
        LOG_WARN("failed to sync rename table to HMS",
                 KR(ret),
                 K(origin_database_name),
                 K(origin_table_name),
                 K(new_database_name),
                 K(new_table_name),
                 K(sync_catalog_id));
      }
    } else {
      LOG_INFO("synced rename table to HMS",
               K(origin_database_name),
               K(origin_table_name),
               K(new_database_name),
               K(new_table_name),
               K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::reconcile_hms_partitions(
    const uint64_t tenant_id,
    const uint64_t sync_catalog_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &database_name,
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<CatalogPartitionValues> expected_partition_values;
  if (OB_FAIL(ObCatalogTypeMapping::build_all_partition_values(
          table_schema, expected_partition_values))) {
    LOG_WARN("failed to build expected HMS partition values",
             KR(ret), K(database_name), K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(reconcile_hms_partitions_with_values(tenant_id,
                                                           sync_catalog_id,
                                                           schema_guard,
                                                           database_name,
                                                           table_schema,
                                                           expected_partition_values))) {
    LOG_WARN("failed to reconcile partitions with fixed HMS target",
             KR(ret), K(tenant_id), K(database_name), K(sync_catalog_id));
  }
  return ret;
}

int ObCatalogSyncHelper::reconcile_hms_partitions_with_values(
    const uint64_t tenant_id,
    const uint64_t sync_catalog_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &database_name,
    const ObTableSchema &table_schema,
    const ObIArray<CatalogPartitionValues> &expected_partition_values)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID != sync_catalog_id) {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.reconcile_partitions(tenant_id,
                                                  sync_catalog_id,
                                                  database_name,
                                                  table_schema,
                                                  expected_partition_values))) {
      LOG_WARN("failed to reconcile synced partitions",
               K(ret), K(database_name), K(table_schema.get_table_name_str()),
               K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_table(const uint64_t tenant_id,
                                    const ObString &access_token,
                                    const uint64_t catalog_id,
                                    const ObString &expected_remote_catalog_name,
                                    ObSchemaGetterGuard &schema_guard,
                                    const ObString &database_name,
                                    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!ObUnifiedCatalog::is_configured(tenant_id) && !is_external_catalog_id(catalog_id))
                  || database_name.empty()
                  || !table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_name),
             K(table_schema.get_table_id()));
  } else if (ObUnifiedCatalog::is_configured(tenant_id)) {
    if (OB_FAIL(ObUnifiedCatalog::sync_table(tenant_id,
                                             access_token,
                                             expected_remote_catalog_name,
                                             database_name,
                                             table_schema,
                                             true))) {
      LOG_WARN("failed to sync table to Unified Catalog",
               K(ret), K(database_name), K(table_schema.get_table_name_str()));
    }
  } else if (OB_FAIL(check_sync_target(tenant_id, catalog_id, schema_guard))) {
    LOG_WARN("invalid configured catalog sync target", K(ret), K(tenant_id), K(catalog_id));
  } else if (!is_syncable_table(table_schema)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync for non-user table");
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.create_namespace(tenant_id, catalog_id, database_name))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to create catalog namespace", K(ret), K(database_name), K(catalog_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sync_table_to_catalog(tenant_id,
                                             catalog_id,
                                             meta_writer,
                                             database_name,
                                             table_schema))) {
      LOG_WARN("failed to sync table to catalog",
               K(ret),
               K(database_name),
               K(table_schema.get_table_name_str()),
               K(catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_database(const uint64_t tenant_id,
                                       const ObString &access_token,
                                       const uint64_t catalog_id,
                                       const ObString &expected_remote_catalog_name,
                                       ObSchemaGetterGuard &schema_guard,
                                       const uint64_t database_id,
                                       const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  const ObDatabaseSchema *database_schema = NULL;
  const bool unified_mode = is_valid_tenant_id(tenant_id)
                            && ObUnifiedCatalog::is_configured(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!ObUnifiedCatalog::is_configured(tenant_id) && !is_external_catalog_id(catalog_id))
                  || OB_INVALID_ID == database_id || database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_id),
             K(database_name));
  } else if (!unified_mode
             && OB_FAIL(check_sync_target(tenant_id, catalog_id, schema_guard))) {
    LOG_WARN("invalid configured catalog sync target", K(ret), K(tenant_id), K(catalog_id));
  } else if (!is_syncable_database(database_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog sync for system database");
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, database_schema))) {
    LOG_WARN("failed to get database schema", K(ret), K(tenant_id), K(database_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database schema is null", K(ret), K(tenant_id), K(database_id));
  } else if (database_schema->is_or_in_recyclebin()) {
    // databases in the recyclebin have been renamed to __recycle_$_... and must
    // NOT be mirrored to HMS, otherwise HMS accumulates leaked __recycle_$_ namespaces.
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "catalog sync for database in recyclebin");
    LOG_WARN("skip catalog sync for database in recyclebin",
             K(ret), K(database_name), K(database_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_database(tenant_id,
                                                                database_id,
                                                                table_schemas))) {
    LOG_WARN("failed to get database tables", K(ret), K(tenant_id), K(database_id));
  } else if (unified_mode) {
    ObArray<const ObTableSchema *> syncable_table_schemas;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(database_id), K(i));
      } else if (!is_syncable_table(*table_schema)) {
        LOG_INFO("skip non-user table for database catalog sync",
                 K(database_name), K(table_schema->get_table_name_str()));
      } else if (OB_FAIL(syncable_table_schemas.push_back(table_schema))) {
        LOG_WARN("failed to collect syncable table schema",
                 K(ret), K(database_name), K(table_schema->get_table_name_str()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObUnifiedCatalog::sync_database(tenant_id,
                                                  access_token,
                                                  expected_remote_catalog_name,
                                                  database_name,
                                                  database_id,
                                                  syncable_table_schemas))) {
        LOG_WARN("failed to sync database to Unified Catalog",
                 K(ret), K(database_name), K(database_id), K(catalog_id));
      }
    }
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.create_namespace(tenant_id, catalog_id, database_name))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to create catalog namespace", K(ret), K(database_name), K(catalog_id));
      }
    }
    // sync each table in order; any table failure aborts the whole database sync
    // immediately (the user fixes the offending table and re-runs the command).
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(database_id), K(i));
      } else if (!is_syncable_table(*table_schema)) {
        // index/aux/non-user tables are not mirrored to HMS, skip explicitly
        LOG_INFO("skip non-user table for database catalog sync",
                 K(database_name),
                 K(table_schema->get_table_name_str()));
      } else if (OB_FAIL(sync_table_to_catalog(tenant_id,
                                               catalog_id,
                                               meta_writer,
                                               database_name,
                                               *table_schema))) {
        LOG_WARN("failed to sync database table",
                 K(ret),
                 K(database_name),
                 K(table_schema->get_table_name_str()),
                 K(catalog_id));
      }
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_catalog(const uint64_t tenant_id,
                                      const ObString &access_token,
                                      const uint64_t catalog_id,
                                      const ObString &expected_remote_catalog_name,
                                      ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<const ObDatabaseSchema *> database_schemas;
  const bool unified_mode = is_valid_tenant_id(tenant_id)
                            && ObUnifiedCatalog::is_configured(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!ObUnifiedCatalog::is_configured(tenant_id) && !is_external_catalog_id(catalog_id)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (!unified_mode
             && OB_FAIL(check_sync_target(tenant_id, catalog_id, schema_guard))) {
    LOG_WARN("invalid configured catalog sync target", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, database_schemas))) {
    LOG_WARN("failed to get tenant databases", K(ret), K(tenant_id));
  } else {
    // sync each database in order; any database failure aborts the whole catalog
    // sync immediately (the user fixes the offending object and re-runs).
    for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas.count(); ++i) {
      const ObDatabaseSchema *database_schema = database_schemas.at(i);
      if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database schema is null", K(ret), K(i));
      } else if (!is_internal_catalog_id(database_schema->get_catalog_id())
                 || database_schema->is_hidden()
                 || database_schema->is_or_in_recyclebin()
                 || !is_syncable_database(database_schema->get_database_id())) {
        // databases in the recyclebin have been renamed to __recycle_$_...
        // and must NOT be mirrored to HMS, otherwise HMS accumulates leaked
        // __recycle_$_ namespaces.
        LOG_TRACE("skip non-user or recyclebin database for full catalog sync",
                 K(database_schema->get_database_name_str()),
                 "in_recyclebin", database_schema->is_or_in_recyclebin());
      } else if (OB_FAIL(sync_database(tenant_id,
                                       access_token,
                                       catalog_id,
                                       expected_remote_catalog_name,
                                       schema_guard,
                                       database_schema->get_database_id(),
                                       database_schema->get_database_name_str()))) {
        LOG_WARN("failed to sync catalog database",
                 K(ret),
                 K(database_schema->get_database_name_str()),
                 K(catalog_id));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
