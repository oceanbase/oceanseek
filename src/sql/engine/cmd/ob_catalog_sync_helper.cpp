/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/catalog/ob_catalog_partition_values.h"
#include "share/catalog/ob_catalog_type_mapping.h"
#include "share/catalog/ob_catalog_meta_getter.h"
#include "share/catalog/ob_catalog_meta_writer.h"
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
    enabled = (NULL != sync_target && '\0' != sync_target[0]);
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

int ObCatalogSyncHelper::sync_create_database(const uint64_t tenant_id,
                                              ObSchemaGetterGuard &schema_guard,
                                              const ObString &database_name)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.create_namespace(tenant_id, sync_catalog_id, database_name))) {
      LOG_WARN("failed to sync create database to HMS", KR(ret), K(database_name), K(sync_catalog_id));
    } else {
      LOG_INFO("synced create database to HMS", K(database_name), K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_drop_database(const uint64_t tenant_id,
                                            ObSchemaGetterGuard &schema_guard,
                                            const ObString &database_name)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.drop_namespace(tenant_id, sync_catalog_id, database_name, false, true))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("HMS namespace already absent, treat sync drop database as success",
                 K(database_name), K(sync_catalog_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync drop database to HMS", KR(ret), K(database_name), K(sync_catalog_id));
      }
    } else {
      LOG_INFO("synced drop database to HMS", K(database_name), K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::check_drop_database_with_remote_tables(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &database_name)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaGetter meta_getter(schema_guard, allocator);
    ObArray<ObString> table_names;
    if (OB_FAIL(meta_getter.list_table_names(tenant_id,
                                             sync_catalog_id,
                                             database_name,
                                             OB_ORIGIN_AND_INSENSITIVE,
                                             table_names))) {
      LOG_WARN("failed to list remote tables for drop database check",
               KR(ret), K(database_name), K(sync_catalog_id));
    } else if (table_names.count() > 0) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "drop database with remote tables in synced catalog");
      LOG_WARN("cannot drop database with remote non-placeholder tables in synced catalog",
               KR(ret), K(database_name), K(sync_catalog_id), K(table_names.count()));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_create_table(const uint64_t tenant_id,
                                           ObSchemaGetterGuard &schema_guard,
                                           const ObString &database_name,
                                           const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
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

int ObCatalogSyncHelper::prepare_flashback_table_sync(
    const uint64_t tenant_id,
    const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
    uint64_t &recycle_table_id)
{
  int ret = OB_SUCCESS;
  recycle_table_id = OB_INVALID_ID;
  const ObString &origin_table_name = flashback_table_arg.origin_table_name_;
  if (!origin_table_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX)
      && !origin_table_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX)) {
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(get_refreshed_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard before flashback table sync",
               KR(ret),
               K(tenant_id),
               K(origin_table_name));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                     OB_RECYCLEBIN_SCHEMA_ID,
                                                     origin_table_name,
                                                     false /*is_index*/,
                                                     table_schema))) {
      LOG_WARN("failed to get recyclebin table schema before flashback",
               KR(ret),
               K(tenant_id),
               K(origin_table_name));
    } else if (OB_ISNULL(table_schema)) {
      // It may be an original table name which happens to use the recyclebin prefix.
      LOG_INFO("recyclebin table does not exist before flashback sync",
               K(tenant_id),
               K(origin_table_name));
    } else {
      recycle_table_id = table_schema->get_table_id();
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_flashback_table(
    const uint64_t tenant_id,
    const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
    const uint64_t recycle_table_id)
{
  int ret = OB_SUCCESS;
  const bool is_rename = !flashback_table_arg.new_table_name_.empty();
  const ObString &final_db_name
      = is_rename ? flashback_table_arg.new_db_name_ : flashback_table_arg.origin_db_name_;
  const ObString &final_table_name
      = is_rename ? flashback_table_arg.new_table_name_ : flashback_table_arg.origin_table_name_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_FAIL(get_refreshed_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard after flashback table", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID != recycle_table_id
             && OB_FAIL(schema_guard.get_table_schema(tenant_id, recycle_table_id, table_schema))) {
    LOG_WARN("failed to get flashed back table schema by table id",
             KR(ret),
             K(tenant_id),
             K(recycle_table_id));
  } else if (OB_INVALID_ID == recycle_table_id
             && OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                      final_db_name,
                                                      final_table_name,
                                                      false /*is_index*/,
                                                      table_schema))) {
    LOG_WARN("failed to get flashed back table schema by name",
             KR(ret),
             K(tenant_id),
             K(final_db_name),
             K(final_table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flashed back table schema is null",
             KR(ret),
             K(tenant_id),
             K(recycle_table_id),
             K(final_db_name),
             K(final_table_name));
  } else if (!table_schema->is_user_table()) {
  } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id,
                                                      table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("failed to get database schema after flashback table",
             KR(ret),
             K(tenant_id),
             K(table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null after flashback table",
             KR(ret),
             K(tenant_id),
             K(table_schema->get_database_id()));
  } else if (OB_FAIL(sync_create_table(tenant_id,
                                       schema_guard,
                                       database_schema->get_database_name_str(),
                                       *table_schema))) {
    if (OB_ERR_ALREADY_EXISTS == ret) {
      LOG_INFO("HMS placeholder already exists after flashback table",
               K(tenant_id),
               K(database_schema->get_database_name_str()),
               K(table_schema->get_table_name_str()));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to sync flashback table to HMS",
               KR(ret),
               K(tenant_id),
               K(database_schema->get_database_name_str()),
               K(table_schema->get_table_name_str()));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_alter_table(const uint64_t tenant_id,
                                          ObSchemaGetterGuard &schema_guard,
                                          const ObString &database_name,
                                          const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.alter_table(tenant_id, sync_catalog_id, database_name, table_schema))) {
      LOG_WARN("failed to sync alter table to HMS", KR(ret),
               K(database_name), K(table_schema.get_table_name_str()), K(sync_catalog_id));
    } else {
      LOG_INFO("synced alter table to HMS",
               K(database_name), K(table_schema.get_table_name_str()), K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_rename_table(const uint64_t tenant_id,
                                           ObSchemaGetterGuard &schema_guard,
                                           const ObString &origin_database_name,
                                           const ObString &origin_table_name,
                                           const ObString &new_database_name,
                                           const ObString &new_table_name,
                                           const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
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

int ObCatalogSyncHelper::sync_rename_table_before(const uint64_t tenant_id,
                                                  const obrpc::ObRenameTableArg &rename_table_arg,
                                                  bool &out_remote_renamed)
{
  int ret = OB_SUCCESS;
  out_remote_renamed = false;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *origin_table_schema = NULL;
  if (1 != rename_table_arg.rename_table_items_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple table rename with catalog sync is");
    LOG_WARN("multiple table rename with catalog sync is not supported",
             KR(ret),
             K(tenant_id),
             "rename_item_count",
             rename_table_arg.rename_table_items_.count());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for rename table sync", KR(ret), K(tenant_id));
  } else {
    const obrpc::ObRenameTableItem &rename_item = rename_table_arg.rename_table_items_.at(0);
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                              rename_item.origin_db_name_,
                                              rename_item.origin_table_name_,
                                              false /*is_index*/,
                                              origin_table_schema))) {
      LOG_WARN("failed to get origin table schema for rename sync",
               KR(ret),
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_ISNULL(origin_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("origin table schema is null for rename sync",
               KR(ret),
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (!origin_table_schema->is_user_table() || origin_table_schema->is_index_table()
               || origin_table_schema->is_aux_table()) {
      // non-user / index / aux tables are not mirrored to HMS, skip
    } else if (OB_FAIL(sync_rename_table(tenant_id,
                                         schema_guard,
                                         rename_item.origin_db_name_,
                                         rename_item.origin_table_name_,
                                         rename_item.new_db_name_,
                                         rename_item.new_table_name_,
                                         *origin_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        // origin table absent in synced catalog: rename local table only
        ret = OB_SUCCESS;
        LOG_INFO("origin table does not exist in synced catalog, rename local table only",
                 K(tenant_id),
                 K(rename_item.origin_db_name_),
                 K(rename_item.origin_table_name_),
                 K(rename_item.new_db_name_),
                 K(rename_item.new_table_name_));
      } else {
        LOG_WARN("failed to sync rename table before local rename",
                 KR(ret),
                 K(tenant_id),
                 K(rename_item.origin_db_name_),
                 K(rename_item.origin_table_name_),
                 K(rename_item.new_db_name_),
                 K(rename_item.new_table_name_));
      }
    } else {
      out_remote_renamed = true;
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_rename_table_rollback(const uint64_t tenant_id,
                                                    const obrpc::ObRenameTableArg &rename_table_arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *new_table_schema = NULL;
  if (1 != rename_table_arg.rename_table_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rename item count on rollback",
             KR(ret),
             K(tenant_id),
             "rename_item_count",
             rename_table_arg.rename_table_items_.count());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for rename rollback", KR(ret), K(tenant_id));
  } else {
    const obrpc::ObRenameTableItem &rename_item = rename_table_arg.rename_table_items_.at(0);
    // the local rename failed, so the origin name is still current locally; use it to fetch schema
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                              rename_item.origin_db_name_,
                                              rename_item.origin_table_name_,
                                              false /*is_index*/,
                                              new_table_schema))) {
      LOG_WARN("failed to get table schema for rename rollback",
               KR(ret),
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_ISNULL(new_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null for rename rollback",
               KR(ret),
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_FAIL(sync_rename_table(tenant_id,
                                         schema_guard,
                                         rename_item.new_db_name_,
                                         rename_item.new_table_name_,
                                         rename_item.origin_db_name_,
                                         rename_item.origin_table_name_,
                                         *new_table_schema))) {
      LOG_WARN("failed to rollback remote rename after local rename failure",
               KR(ret),
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_),
               K(rename_item.new_db_name_),
               K(rename_item.new_table_name_));
    } else {
      LOG_INFO("rolled back remote rename after local rename failure",
               K(tenant_id),
               K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_),
               K(rename_item.new_db_name_),
               K(rename_item.new_table_name_));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::reconcile_partitions(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &database_name,
    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  ObArray<CatalogPartitionValues> expected_partition_values;
  if (OB_FAIL(ObCatalogTypeMapping::build_all_partition_values(
          table_schema, expected_partition_values))) {
    LOG_WARN("failed to build expected partition values",
             K(ret), K(database_name), K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(
                 ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
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

int ObCatalogSyncHelper::sync_drop_table(const uint64_t tenant_id,
                                         ObSchemaGetterGuard &schema_guard,
                                         const ObString &database_name,
                                         const ObString &table_name)
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(schema_guard, tenant_id, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id", KR(ret), K(tenant_id));
  } else if (OB_INVALID_ID == sync_catalog_id) {
  } else {
    ObArenaAllocator allocator("CatalogSync");
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.drop_table(tenant_id, sync_catalog_id, database_name, table_name))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("HMS placeholder table already absent, treat sync drop table as success",
                 K(database_name), K(table_name), K(sync_catalog_id));
        ret = OB_SUCCESS;
      } else if (OB_OP_NOT_ALLOW == ret) {
        LOG_INFO("skip dropping HMS table not owned by current tenant",
                 K(database_name), K(table_name), K(sync_catalog_id), K(tenant_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync drop table to HMS", KR(ret),
                 K(database_name), K(table_name), K(sync_catalog_id));
      }
    } else {
      LOG_INFO("synced drop table to HMS", K(database_name), K(table_name), K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_drop_tables(const uint64_t tenant_id,
                                          const ObIArray<obrpc::ObTableItem> &table_items)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for drop table sync", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      const obrpc::ObTableItem &table_item = table_items.at(i);
      if (OB_FAIL(sync_drop_table(tenant_id, schema_guard,
                                  table_item.database_name_, table_item.table_name_))) {
        LOG_WARN("failed to sync drop table to HMS", KR(ret), K(tenant_id),
                 K(table_item.database_name_), K(table_item.table_name_));
      }
    }
  }
  return ret;
}

int ObCatalogSyncHelper::sync_table(const uint64_t tenant_id,
                                    const uint64_t catalog_id,
                                    ObSchemaGetterGuard &schema_guard,
                                    const ObString &database_name,
                                    const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || database_name.empty()
                  || !table_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_name),
             K(table_schema.get_table_id()));
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
                                       const uint64_t catalog_id,
                                       ObSchemaGetterGuard &schema_guard,
                                       const uint64_t database_id,
                                       const ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema *> table_schemas;
  const ObDatabaseSchema *database_schema = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id)
                  || OB_INVALID_ID == database_id || database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             K(tenant_id),
             K(catalog_id),
             K(database_id),
             K(database_name));
  } else if (OB_FAIL(check_sync_target(tenant_id, catalog_id, schema_guard))) {
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
                                      const uint64_t catalog_id,
                                      ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObArray<const ObDatabaseSchema *> database_schemas;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_external_catalog_id(catalog_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_FAIL(check_sync_target(tenant_id, catalog_id, schema_guard))) {
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
                                       catalog_id,
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
