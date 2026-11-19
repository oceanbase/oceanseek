/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_flashback_table_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

ObFlashbackTableCatalogSyncOperation::ObFlashbackTableCatalogSyncOperation(
    const uint64_t tenant_id,
    const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg)
  : tenant_id_(tenant_id),
    flashback_table_arg_(flashback_table_arg),
    recycle_table_id_(OB_INVALID_ID),
    sync_catalog_id_(OB_INVALID_ID)
{
}

ObFlashbackTableCatalogSyncOperation::~ObFlashbackTableCatalogSyncOperation()
{
}

int ObFlashbackTableCatalogSyncOperation::prepare(
    ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  recycle_table_id_ = OB_INVALID_ID;
  sync_catalog_id_ = OB_INVALID_ID;
  const ObString &origin_table_name = flashback_table_arg_.origin_table_name_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id_);
  const bool is_hms_sync_enabled =
      !is_unified_catalog && ObCatalogSyncHelper::is_sync_enabled(tenant_id_);
  if (is_unified_catalog || !is_hms_sync_enabled) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("flashback table catalog sync backend changed before prepare",
             KR(ret), K(tenant_id_), K(is_unified_catalog),
             K(is_hms_sync_enabled));
  } else if (OB_FAIL(ObCatalogSyncHelper::get_refreshed_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard before flashback table sync",
             KR(ret), K(tenant_id_), K(origin_table_name));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id_))) {
    LOG_WARN("failed to capture HMS flashback table target",
             KR(ret), K(tenant_id_), K(origin_table_name));
  } else if ((origin_table_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX)
              || origin_table_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX))
             && OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                       OB_RECYCLEBIN_SCHEMA_ID,
                                                       origin_table_name,
                                                       false /*is_index*/,
                                                       table_schema))) {
    LOG_WARN("failed to get recyclebin table schema before flashback",
             KR(ret), K(tenant_id_), K(origin_table_name));
  } else if ((origin_table_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX)
              || origin_table_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX))
             && OB_ISNULL(table_schema)) {
    // It may be an original table name which happens to use the recyclebin prefix.
    LOG_INFO("recyclebin table does not exist before flashback sync",
             K(tenant_id_), K(origin_table_name));
  } else if (OB_NOT_NULL(table_schema)) {
    recycle_table_id_ = table_schema->get_table_id();
  }
  result.source_object_id_ = recycle_table_id_;
  return ret;
}

int ObFlashbackTableCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if (OB_FAIL(complete_catalog_sync())) {
      LOG_WARN("failed to complete flashback table catalog sync",
               KR(ret), K(tenant_id_), K(recycle_table_id_), K(sync_catalog_id_));
    }
  }
  return ret;
}

int ObFlashbackTableCatalogSyncOperation::complete_catalog_sync()
{
  int ret = OB_SUCCESS;
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id_);
  const bool is_hms_sync_enabled =
      !is_unified_catalog && ObCatalogSyncHelper::is_sync_enabled(tenant_id_);
  const bool is_rename = !flashback_table_arg_.new_table_name_.empty();
  const ObString &final_db_name = is_rename
      ? flashback_table_arg_.new_db_name_ : flashback_table_arg_.origin_db_name_;
  const ObString &final_table_name = is_rename
      ? flashback_table_arg_.new_table_name_ : flashback_table_arg_.origin_table_name_;
  uint64_t current_sync_catalog_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  if (is_unified_catalog || !is_hms_sync_enabled) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("flashback table catalog sync backend changed",
             KR(ret), K(tenant_id_), K(is_unified_catalog),
             K(is_hms_sync_enabled), K(sync_catalog_id_));
  } else if (OB_FAIL(ObCatalogSyncHelper::get_refreshed_schema_guard(
          tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard after flashback table",
             KR(ret), K(tenant_id_), K(is_unified_catalog));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, current_sync_catalog_id))) {
    LOG_WARN("failed to resolve HMS flashback table target",
             KR(ret), K(tenant_id_));
  } else if (sync_catalog_id_ != current_sync_catalog_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("HMS flashback table target changed",
             KR(ret), K(tenant_id_), K(sync_catalog_id_),
             K(current_sync_catalog_id));
  } else if (OB_INVALID_ID != recycle_table_id_
             && OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, recycle_table_id_, table_schema))) {
    LOG_WARN("failed to get flashed back table schema by table id",
             KR(ret), K(tenant_id_), K(recycle_table_id_), K(is_unified_catalog));
  } else if (OB_INVALID_ID == recycle_table_id_
             && OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                      final_db_name,
                                                      final_table_name,
                                                      false /*is_index*/,
                                                      table_schema))) {
    LOG_WARN("failed to get flashed back table schema by name",
             KR(ret), K(tenant_id_), K(final_db_name), K(final_table_name), K(is_unified_catalog));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flashed back table schema is null",
             KR(ret), K(tenant_id_), K(recycle_table_id_),
             K(final_db_name), K(final_table_name), K(is_unified_catalog));
  } else if (table_schema->is_user_table()) {
    if (OB_FAIL(schema_guard.get_database_schema(
            tenant_id_, table_schema->get_database_id(), database_schema))) {
      LOG_WARN("failed to get database schema after flashback table",
               KR(ret), K(tenant_id_), K(table_schema->get_database_id()), K(is_unified_catalog));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database schema is null after flashback table",
               KR(ret), K(tenant_id_), K(table_schema->get_database_id()), K(is_unified_catalog));
    } else if (OB_INVALID_ID != sync_catalog_id_
               && OB_FAIL(ObCatalogSyncHelper::sync_table_to_hms(
                   tenant_id_, sync_catalog_id_, schema_guard,
                   database_schema->get_database_name_str(), *table_schema))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        LOG_INFO("catalog placeholder already exists after flashback table",
                 K(tenant_id_), K(sync_catalog_id_),
                 K(database_schema->get_database_name_str()),
                 K(table_schema->get_table_name_str()));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync flashback table to catalog",
                 KR(ret), K(tenant_id_), K(sync_catalog_id_),
                 K(database_schema->get_database_name_str()),
                 K(table_schema->get_table_name_str()));
      }
    }
  }
  return ret;
}

int ObCatalogSync::get_flashback_table_operator(
    const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = flashback_table_arg.tenant_id_;
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id);
  const bool is_sync_enabled = ObCatalogSyncHelper::is_sync_enabled(tenant_id);
  handle.reset();
  if (is_sync_enabled && is_unified_catalog) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("FLASHBACK TABLE is not supported while UC Schema sync is enabled",
             K(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "FLASHBACK TABLE with Unified Catalog schema synchronization");
  } else if (is_sync_enabled) {
    ObFlashbackTableCatalogSyncOperation *operation =
        OB_NEWx(ObFlashbackTableCatalogSyncOperation,
                (&allocator_), tenant_id, flashback_table_arg);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate flashback table catalog sync operation",
               KR(ret), K(tenant_id));
      handle.set_deferred_error(ret);
      ret = OB_SUCCESS;
    } else {
      handle.set(allocator_, operation);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
