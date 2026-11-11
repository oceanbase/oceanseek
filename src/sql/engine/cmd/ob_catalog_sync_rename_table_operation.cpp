/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_rename_table_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

ObRenameTableCatalogSyncOperation::ObRenameTableCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const obrpc::ObRenameTableArg &rename_table_arg)
  : tenant_id_(tenant_id),
    is_unified_catalog_(is_unified_catalog),
    rename_table_arg_(rename_table_arg),
    sync_catalog_id_(OB_INVALID_ID),
    remote_renamed_(false)
{
}

int ObRenameTableCatalogSyncOperation::create(
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const obrpc::ObRenameTableArg &rename_table_arg,
    ObRenameTableCatalogSyncOperation *&operation)
{
  int ret = OB_SUCCESS;
  operation = NULL;
  ObRenameTableCatalogSyncOperation *new_operation =
      OB_NEWx(ObRenameTableCatalogSyncOperation,
              (&allocator),
              tenant_id,
              is_unified_catalog,
              rename_table_arg);
  if (OB_ISNULL(new_operation)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate rename table catalog sync operation",
             KR(ret), K(tenant_id), K(is_unified_catalog));
  } else if (OB_FAIL(new_operation->init())) {
    LOG_WARN("failed to initialize rename table catalog sync operation",
             KR(ret), K(tenant_id), K(is_unified_catalog));
    new_operation->~ObRenameTableCatalogSyncOperation();
    allocator.free(new_operation);
    new_operation = NULL;
  } else {
    operation = new_operation;
  }
  return ret;
}

ObRenameTableCatalogSyncOperation::~ObRenameTableCatalogSyncOperation()
{
}

int ObRenameTableCatalogSyncOperation::init()
{
  int ret = OB_SUCCESS;
  // UC eligibility was checked before task/RPC acquisition in the original
  // executor. Keep that ordering while HMS remote mutation stays in prepare().
  if (is_unified_catalog_ && OB_FAIL(validate_unified_catalog_source())) {
    LOG_WARN("failed to validate Unified Catalog rename table source",
             KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::validate_unified_catalog_source()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool has_external_source = false;
  bool has_non_external_source = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null before rename table",
             K(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard before rename table",
             K(ret), K(tenant_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_non_external_source
                        && i < rename_table_arg_.rename_table_items_.count();
         ++i) {
      const obrpc::ObRenameTableItem &rename_item =
          rename_table_arg_.rename_table_items_.at(i);
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                rename_item.origin_db_name_,
                                                rename_item.origin_table_name_,
                                                false /*is_index*/,
                                                table_schema))) {
        LOG_WARN("failed to get source table before rename",
                 K(ret), K(tenant_id_), K(rename_item));
      } else if (OB_NOT_NULL(table_schema)) {
        if (table_schema->is_external_table()) {
          has_external_source = true;
        } else {
          has_non_external_source = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && (!has_external_source || has_non_external_source)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("RENAME TABLE is not supported while UC Schema sync is enabled",
             K(ret), K(tenant_id_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "RENAME TABLE with Unified Catalog schema synchronization");
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::prepare(
    ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (!is_unified_catalog_ && OB_FAIL(prepare_hms())) {
    LOG_WARN("failed to prepare HMS rename table sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::prepare_hms()
{
  int ret = OB_SUCCESS;
  remote_renamed_ = false;
  sync_catalog_id_ = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *origin_table_schema = NULL;
  if (1 != rename_table_arg_.rename_table_items_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "multiple table rename with catalog sync is");
    LOG_WARN("multiple table rename with catalog sync is not supported",
             KR(ret), K(tenant_id_), "rename_item_count",
             rename_table_arg_.rename_table_items_.count());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for rename table sync",
             KR(ret), K(tenant_id_));
  } else {
    const obrpc::ObRenameTableItem &rename_item =
        rename_table_arg_.rename_table_items_.at(0);
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                              rename_item.origin_db_name_,
                                              rename_item.origin_table_name_,
                                              false /*is_index*/,
                                              origin_table_schema))) {
      LOG_WARN("failed to get origin table schema for rename sync",
               KR(ret), K(tenant_id_), K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_ISNULL(origin_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("origin table schema is null for rename sync",
               KR(ret), K(tenant_id_), K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (!origin_table_schema->is_user_table()
               || origin_table_schema->is_index_table()
               || origin_table_schema->is_aux_table()) {
      // non-user / index / aux tables are not mirrored to HMS, skip
    } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                   schema_guard, tenant_id_, sync_catalog_id_))) {
      LOG_WARN("failed to get fixed HMS target for rename table sync",
               KR(ret), K(tenant_id_));
    } else if (OB_INVALID_ID == sync_catalog_id_) {
      // HMS schema synchronization is no longer enabled for this tenant.
    } else if (OB_FAIL(ObCatalogSyncHelper::sync_rename_table(
                   tenant_id_,
                   sync_catalog_id_,
                   schema_guard,
                   rename_item.origin_db_name_,
                   rename_item.origin_table_name_,
                   rename_item.new_db_name_,
                   rename_item.new_table_name_,
                   *origin_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("origin table does not exist in synced catalog, rename local table only",
                 K(tenant_id_), K(sync_catalog_id_),
                 K(rename_item.origin_db_name_), K(rename_item.origin_table_name_),
                 K(rename_item.new_db_name_), K(rename_item.new_table_name_));
      } else {
        LOG_WARN("failed to sync rename table before local rename",
                 KR(ret), K(tenant_id_), K(sync_catalog_id_),
                 K(rename_item.origin_db_name_), K(rename_item.origin_table_name_),
                 K(rename_item.new_db_name_), K(rename_item.new_table_name_));
      }
    } else {
      remote_renamed_ = true;
    }
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::rollback_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *origin_table_schema = NULL;
  if (1 != rename_table_arg_.rename_table_items_.count()
      || OB_INVALID_ID == sync_catalog_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid HMS rename rollback state",
             KR(ret), K(tenant_id_), K(sync_catalog_id_),
             "rename_item_count", rename_table_arg_.rename_table_items_.count());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for rename rollback",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for rename rollback",
             KR(ret), K(tenant_id_));
  } else {
    const obrpc::ObRenameTableItem &rename_item =
        rename_table_arg_.rename_table_items_.at(0);
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                              rename_item.origin_db_name_,
                                              rename_item.origin_table_name_,
                                              false /*is_index*/,
                                              origin_table_schema))) {
      LOG_WARN("failed to get table schema for rename rollback",
               KR(ret), K(tenant_id_), K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_ISNULL(origin_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema is null for rename rollback",
               KR(ret), K(tenant_id_), K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_));
    } else if (OB_FAIL(ObCatalogSyncHelper::sync_rename_table(
                   tenant_id_,
                   sync_catalog_id_,
                   schema_guard,
                   rename_item.new_db_name_,
                   rename_item.new_table_name_,
                   rename_item.origin_db_name_,
                   rename_item.origin_table_name_,
                   *origin_table_schema))) {
      LOG_WARN("failed to rollback remote rename after local rename failure",
               KR(ret), K(tenant_id_), K(sync_catalog_id_),
               K(rename_item.origin_db_name_), K(rename_item.origin_table_name_),
               K(rename_item.new_db_name_), K(rename_item.new_table_name_));
    } else {
      remote_renamed_ = false;
      LOG_INFO("rolled back remote rename after local rename failure",
               K(tenant_id_), K(sync_catalog_id_), K(rename_item.origin_db_name_),
               K(rename_item.origin_table_name_), K(rename_item.new_db_name_),
               K(rename_item.new_table_name_));
    }
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_unified_catalog_ && remote_renamed_) {
    if (CATALOG_SYNC_DDL_UNKNOWN == result.state_) {
      LOG_WARN("native rename result is unknown; HMS rename retained",
               K(result.ddl_ret_),
               K(tenant_id_),
               K(sync_catalog_id_),
               K(result.state_),
               K(is_unified_catalog_));
    } else if (CATALOG_SYNC_DDL_SUCCEEDED != result.state_ || OB_SUCCESS != result.ddl_ret_) {
      if (OB_FAIL(rollback_hms())) {
        LOG_WARN("failed to compensate HMS rename table sync",
                 KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_),
                 K(is_unified_catalog_));
      }
    }
  }
  return ret;
}

int ObCatalogSync::get_rename_table_operator(
    const obrpc::ObRenameTableArg &rename_table_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = rename_table_arg.tenant_id_;
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id);
  const bool is_sync_enabled = is_unified_catalog
                               || ObCatalogSyncHelper::is_sync_enabled(tenant_id);
  handle.reset();
  if (is_sync_enabled) {
    ObRenameTableCatalogSyncOperation *operation = NULL;
    if (OB_FAIL(ObRenameTableCatalogSyncOperation::create(
            allocator_, tenant_id, is_unified_catalog, rename_table_arg, operation))) {
      LOG_WARN("failed to create rename table catalog sync operation",
               KR(ret), K(tenant_id), K(is_unified_catalog));
      if (!is_unified_catalog) {
        handle.set_deferred_error(ret);
        ret = OB_SUCCESS;
      }
    } else {
      handle.set(allocator_, operation);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
