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
#include "share/catalog/ob_unified_catalog_schema_codec.h"
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
    const uint64_t source_table_id,
    const ObString &access_token,
    const obrpc::ObRenameTableArg &rename_table_arg)
    : tenant_id_(tenant_id), is_unified_catalog_(is_unified_catalog),
      rename_table_arg_(rename_table_arg),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
      target_(tenant_id, access_token),
      source_table_id_(source_table_id), sync_catalog_id_(OB_INVALID_ID), remote_renamed_(false)
{
}

int ObRenameTableCatalogSyncOperation::create(
    ObIAllocator &allocator,
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const uint64_t source_table_id,
    const ObString &access_token,
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
              source_table_id,
              access_token,
              rename_table_arg);
  if (OB_ISNULL(new_operation)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate rename table catalog sync operation",
             KR(ret), K(tenant_id), K(is_unified_catalog));
  } else {
    operation = new_operation;
  }
  return ret;
}

ObRenameTableCatalogSyncOperation::~ObRenameTableCatalogSyncOperation()
{
}

int ObCatalogSync::precheck_unified_catalog_rename(const uint64_t tenant_id,
                                                   const obrpc::ObRenameTableArg &rename_table_arg,
                                                   uint64_t &source_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool has_external_source = false;
  bool has_non_external_source = false;
  source_table_id = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null before rename table", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard before rename table", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rename_table_arg.rename_table_items_.count(); ++i) {
      const obrpc::ObRenameTableItem &rename_item = rename_table_arg.rename_table_items_.at(i);
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                rename_item.origin_db_name_,
                                                rename_item.origin_table_name_,
                                                false /*is_index*/,
                                                table_schema))) {
        LOG_WARN("failed to get source table before rename", K(ret), K(tenant_id), K(rename_item));
      } else if (OB_NOT_NULL(table_schema)) {
        if (table_schema->is_external_table()) {
          has_external_source = true;
        } else {
          has_non_external_source = true;
          if (1 == rename_table_arg.rename_table_items_.count()) {
            if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(*table_schema))) {
              LOG_WARN("source table is not supported by Unified Catalog rename",
                       K(ret),
                       K(tenant_id),
                       K(rename_item));
            } else {
              source_table_id = table_schema->get_table_id();
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)
      && ((!has_external_source && !has_non_external_source)
          || (has_non_external_source
              && (1 != rename_table_arg.rename_table_items_.count() || has_external_source)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("RENAME TABLE is not supported while UC Schema sync is enabled", K(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "RENAME TABLE with Unified Catalog schema synchronization");
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::prepare(
    ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (is_unified_catalog_
      && OB_FAIL(prepare_unified_catalog_target(target_, allocator_))) {
    LOG_WARN("failed to prepare Unified Catalog rename table sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  } else if (is_unified_catalog_) {
    result.source_object_id_ = source_table_id_;
  } else if (!is_unified_catalog_ && OB_FAIL(prepare_hms())) {
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

int ObRenameTableCatalogSyncOperation::complete_unified_catalog()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const obrpc::ObRenameTableItem &rename_item = rename_table_arg_.rename_table_items_.at(0);
  if (OB_FAIL(ObCatalogSyncHelper::get_refreshed_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard after rename table",
             KR(ret),
             K(tenant_id_),
             K(rename_item));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                   rename_item.new_db_name_,
                                                   rename_item.new_table_name_,
                                                   false /*is_index*/,
                                                   table_schema))) {
    LOG_WARN("failed to get renamed table schema for Unified Catalog sync",
             KR(ret),
             K(tenant_id_),
             K(rename_item));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("renamed table schema is null after successful rename",
             KR(ret),
             K(tenant_id_),
             K(rename_item));
  } else if (source_table_id_ != table_schema->get_table_id()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("renamed table source identity changed",
             KR(ret),
             K(tenant_id_),
             K(source_table_id_),
             "final_table_id",
             table_schema->get_table_id(),
             K(rename_item));
  } else if (OB_FAIL(ObUnifiedCatalog::rename_table_to_target(target_,
                                                              rename_item.origin_db_name_,
                                                              rename_item.origin_table_name_,
                                                              rename_item.new_db_name_,
                                                              *table_schema,
                                                              source_table_id_))) {
    LOG_WARN("failed to sync renamed table to Unified Catalog",
             KR(ret),
             K(tenant_id_),
             K(rename_item),
             K(source_table_id_));
  }
  return ret;
}

int ObRenameTableCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (is_unified_catalog_ && CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)
      && OB_FAIL(complete_unified_catalog())) {
    LOG_WARN("failed to complete Unified Catalog rename table sync",
             KR(ret),
             K(tenant_id_),
             K(result.state_),
             K(result.ddl_ret_));
  } else if (!is_unified_catalog_ && remote_renamed_) {
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
                 KR(ret),
                 K(tenant_id_),
                 K(result.state_),
                 K(result.ddl_ret_),
                 K(is_unified_catalog_));
      }
    }
  }
  return ret;
}

int ObCatalogSync::precheck_rename_table(const obrpc::ObRenameTableArg &rename_table_arg,
                                         ObCatalogSyncPrecheckResult &precheck_result)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = rename_table_arg.tenant_id_;
  precheck_result.backend_ = snapshot_backend(tenant_id);
  precheck_result.source_object_id_ = OB_INVALID_ID;
  if (precheck_result.is_unified_catalog()) {
    if (OB_FAIL(precheck_unified_catalog_rename(tenant_id,
                                                rename_table_arg,
                                                precheck_result.source_object_id_))) {
      LOG_WARN("failed to precheck Unified Catalog rename table", KR(ret), K(tenant_id));
    } else if (OB_INVALID_ID == precheck_result.source_object_id_) {
      precheck_result.backend_ = ObCatalogSyncBackend::NONE;
    }
  }
  return ret;
}

int ObCatalogSync::get_rename_table_operator(const ObCatalogSyncPrecheckResult &precheck_result,
                                             const ObString &access_token,
                                             const obrpc::ObRenameTableArg &rename_table_arg,
                                             ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = rename_table_arg.tenant_id_;
  const bool is_unified_catalog = precheck_result.is_unified_catalog();
  handle.reset();
  if (precheck_result.need_operation()) {
    ObRenameTableCatalogSyncOperation *operation = NULL;
    if (OB_FAIL(ObRenameTableCatalogSyncOperation::create(allocator_,
                                                          tenant_id,
                                                          is_unified_catalog,
                                                          precheck_result.get_source_object_id(),
                                                          access_token,
                                                          rename_table_arg,
                                                          operation))) {
      LOG_WARN("failed to create rename table catalog sync operation",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog));
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
