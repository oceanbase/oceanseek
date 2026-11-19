/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_create_table_like_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_tracepoint.h"
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

ERRSIM_POINT_DEF(ERRSIM_CATALOG_SYNC_CREATE_LIKE_ALLOC_FAIL);

ObCreateTableLikeCatalogSyncOperation::ObCreateTableLikeCatalogSyncOperation(
    const ObString &access_token,
    const obrpc::ObCreateTableLikeArg &create_table_like_arg,
    const bool is_unified_catalog)
    : create_table_like_arg_(create_table_like_arg), tenant_id_(create_table_like_arg.tenant_id_),
      is_unified_catalog_(is_unified_catalog),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_),
      target_(tenant_id_, access_token)
{
}

ObCreateTableLikeCatalogSyncOperation::~ObCreateTableLikeCatalogSyncOperation()
{
}

int ObCreateTableLikeCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (is_unified_catalog_
      && USER_TABLE == create_table_like_arg_.table_type_
      && OB_FAIL(prepare_unified_catalog_target(target_, allocator_))) {
    LOG_WARN("failed to prepare Unified Catalog create table like sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableLikeCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  // LIKE carries names, so fetch the final target Schema after local DDL
  // succeeds. IF NOT EXISTS still fetches and synchronizes that target.
  // Synchronization failure returns an error and leaves the local table.
  // This post-DDL timing is part of LIKE's contract.

  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if (USER_TABLE == create_table_like_arg_.table_type_) {
      if (is_unified_catalog_) {
        if (OB_FAIL(complete_unified_catalog())) {
          LOG_WARN("failed to complete Unified Catalog create table like sync",
                   KR(ret), K(tenant_id_), K(is_unified_catalog_));
        }
      } else if (ObCatalogSyncHelper::is_sync_enabled(tenant_id_)
                 && OB_FAIL(complete_hms())) {
        LOG_WARN("failed to complete HMS create table like sync",
                 KR(ret), K(tenant_id_), K(is_unified_catalog_));
      }
    }
  }
  return ret;
}

int ObCreateTableLikeCatalogSyncOperation::get_final_table_schema(
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for create table like sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for create table like sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, create_table_like_arg_.new_db_name_,
                 create_table_like_arg_.new_table_name_, false, table_schema))) {
    LOG_WARN("failed to get created table like schema for sync",
             KR(ret), K(tenant_id_), K(create_table_like_arg_.new_db_name_),
             K(create_table_like_arg_.new_table_name_), K(is_unified_catalog_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created table like schema is null",
             KR(ret), K(tenant_id_), K(create_table_like_arg_.new_db_name_),
             K(create_table_like_arg_.new_table_name_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableLikeCatalogSyncOperation::complete_unified_catalog()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (OB_FAIL(get_final_table_schema(schema_guard, table_schema))) {
    LOG_WARN("failed to resolve final table for Unified Catalog create table like sync",
             KR(ret), K(tenant_id_));
  } else if (table_schema->is_user_table()
             && OB_FAIL(ObUnifiedCatalog::sync_table_to_target(target_,
                                                               create_table_like_arg_.new_db_name_,
                                                               *table_schema))) {
    if (OB_ERR_ALREADY_EXISTS == ret && create_table_like_arg_.if_not_exist_) {
      LOG_INFO("Unified Catalog table already exists, tolerate create table like if not exists",
               K(tenant_id_), K(create_table_like_arg_.new_db_name_),
               K(create_table_like_arg_.new_table_name_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to sync create table like to Unified Catalog",
               KR(ret), K(tenant_id_), K(create_table_like_arg_.new_db_name_),
               K(create_table_like_arg_.new_table_name_));
    }
  }
  return ret;
}

int ObCreateTableLikeCatalogSyncOperation::complete_hms()
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (OB_FAIL(get_final_table_schema(schema_guard, table_schema))) {
    LOG_WARN("failed to resolve final table for HMS create table like sync",
             KR(ret), K(tenant_id_));
  } else if (table_schema->is_user_table()
             && OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id))) {
    LOG_WARN("failed to resolve HMS create table like target",
             KR(ret), K(tenant_id_));
  } else if (table_schema->is_user_table() && OB_INVALID_ID != sync_catalog_id) {
    if (OB_FAIL(ObCatalogSyncHelper::sync_table_to_hms(
            tenant_id_, sync_catalog_id, schema_guard,
            create_table_like_arg_.new_db_name_, *table_schema))) {
      if (OB_ERR_ALREADY_EXISTS == ret && create_table_like_arg_.if_not_exist_) {
        LOG_INFO("HMS table already exists, tolerate create table like if not exists",
                 K(tenant_id_), K(sync_catalog_id), K(create_table_like_arg_.new_db_name_),
                 K(create_table_like_arg_.new_table_name_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync create table like to HMS",
                 KR(ret), K(tenant_id_), K(sync_catalog_id),
                 K(create_table_like_arg_.new_db_name_),
                 K(create_table_like_arg_.new_table_name_));
      }
    } else if (table_schema->is_partitioned_table()
               && OB_FAIL(ObCatalogSyncHelper::reconcile_hms_partitions(
                   tenant_id_, sync_catalog_id, schema_guard,
                   create_table_like_arg_.new_db_name_, *table_schema))) {
      LOG_WARN("failed to reconcile create table like partitions in HMS",
               KR(ret), K(tenant_id_), K(sync_catalog_id),
               K(create_table_like_arg_.new_db_name_),
               K(create_table_like_arg_.new_table_name_));
    }
  }
  return ret;
}

int ObCatalogSync::get_create_table_like_operator(
    const ObCatalogSyncBackend backend,
    const ObString &access_token,
    const obrpc::ObCreateTableLikeArg &create_table_like_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = create_table_like_arg.tenant_id_;
  handle.reset();
  if (ObCatalogSyncBackend::NONE != backend) {
    const bool is_unified_catalog = ObCatalogSyncBackend::UNIFIED_CATALOG == backend;
    ObCreateTableLikeCatalogSyncOperation *operation = NULL;
    if (OB_FAIL(ERRSIM_CATALOG_SYNC_CREATE_LIKE_ALLOC_FAIL)) {
      LOG_WARN("failed to inject create table like catalog sync allocation error",
               KR(ret), K(tenant_id),
               K(is_unified_catalog));
    } else {
      operation = OB_NEWx(ObCreateTableLikeCatalogSyncOperation,
                          (&allocator_),
                          access_token,
                          create_table_like_arg,
                          is_unified_catalog);
      if (OB_ISNULL(operation)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate create table like catalog sync operation",
                 KR(ret), K(tenant_id),
                 K(is_unified_catalog));
      }
    }
    if (OB_SUCCESS != ret) {
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
