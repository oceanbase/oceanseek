/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_create_table_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/engine/expr/ob_expr_uuid.h"
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

ObCreateTableCatalogSyncOperation::ObCreateTableCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const bool if_not_exist,
    const ObString &access_token,
    const ObString &database_name,
    const ObTableSchema &table_schema)
  : tenant_id_(tenant_id),
    is_unified_catalog_(is_unified_catalog),
    if_not_exist_(if_not_exist),
    is_partitioned_table_(table_schema.is_partitioned_table()),
    input_database_name_(database_name),
    table_schema_(table_schema),
    allocator_("UcCreate", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    target_(tenant_id, access_token),
    database_name_(),
    table_name_(),
    operation_id_(),
    database_id_(OB_INVALID_ID),
    sync_catalog_id_(OB_INVALID_ID),
    reserved_(false),
    operation_id_buffer_()
{
}

ObCreateTableCatalogSyncOperation::~ObCreateTableCatalogSyncOperation()
{
}

int ObCreateTableCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  if (is_unified_catalog_) {
    if (OB_FAIL(prepare_unified_catalog(result))) {
      LOG_WARN("failed to prepare Unified Catalog create table sync",
               KR(ret), K(tenant_id_), K(is_unified_catalog_));
    }
  } else if (OB_FAIL(prepare_hms())) {
    LOG_WARN("failed to prepare HMS create table sync", KR(ret), K(tenant_id_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::prepare_unified_catalog(
    ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = NULL;
  uint64_t existing_table_id = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for Unified Catalog create reservation", KR(ret));
  } else if (OB_FAIL(schema_guard.get_database_id(
                 tenant_id_, input_database_name_, database_id_))) {
    LOG_WARN("failed to get database ID for Unified Catalog create reservation", KR(ret));
  } else if (OB_INVALID_ID == database_id_) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database is missing for Unified Catalog create reservation", KR(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, database_id_, database_schema))) {
    LOG_WARN("failed to get database schema for Unified Catalog create reservation", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null for Unified Catalog create reservation", KR(ret));
  } else if (OB_FAIL(ob_write_string(allocator_,
                                     database_schema->get_database_name_str(),
                                     database_name_))) {
    LOG_WARN("failed to copy Unified Catalog create namespace", KR(ret));
  } else if (OB_FAIL(ob_write_string(
                 allocator_, table_schema_.get_table_name_str(), table_name_))) {
    LOG_WARN("failed to copy Unified Catalog create table name", KR(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(
                 tenant_id_, database_id_, table_name_, false,
                 ObSchemaGetterGuard::CheckTableType::ALL_NON_HIDDEN_TYPES,
                 existing_table_id))) {
    LOG_WARN("failed to check existing table before Unified Catalog reservation", KR(ret));
  } else if (OB_INVALID_ID != existing_table_id) {
    if (if_not_exist_) {
      result.skip_local_ddl_ = true;
    } else {
      ret = OB_ERR_TABLE_EXIST;
      LOG_USER_ERROR(OB_ERR_TABLE_EXIST,
                     table_name_.length(),
                     table_name_.ptr());
      LOG_WARN("local table already exists before Unified Catalog reservation",
               KR(ret), K(tenant_id_), K(database_name_), K(table_name_));
    }
  } else if (OB_FAIL(ObExprUuid::gen_server_uuid(
                 operation_id_buffer_, UuidCommon::LENGTH_UUID))) {
    LOG_WARN("failed to generate Unified Catalog create operation ID", KR(ret));
  } else {
    operation_id_.assign_ptr(operation_id_buffer_, UuidCommon::LENGTH_UUID);
    if (OB_FAIL(target_.capture(allocator_))) {
      LOG_WARN("failed to capture Unified Catalog create target", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObUnifiedCatalog::reserve_create_table(target_,
                                                               database_name_,
                                                               database_id_,
                                                               table_schema_,
                                                               operation_id_))) {
      LOG_WARN("failed to reserve Unified Catalog table name before local create",
               KR(ret), K(tenant_id_), K(database_name_), K(table_name_));
    } else {
      reserved_ = true;
    }
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::prepare_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for create table sync", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id_))) {
    LOG_WARN("failed to get fixed HMS target for create table sync", KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == sync_catalog_id_) {
  } else if (OB_FAIL(ObCatalogSyncHelper::sync_table_to_hms(tenant_id_,
                                                            sync_catalog_id_,
                                                            schema_guard,
                                                            input_database_name_,
                                                            table_schema_))) {
    if (OB_ERR_ALREADY_EXISTS == ret && if_not_exist_) {
      LOG_INFO("HMS table already exists, tolerate sync create for create table if not exists",
               K(tenant_id_), K(input_database_name_),
               K(table_schema_.get_table_name_str()));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to sync create table to HMS before local create",
               KR(ret), K(tenant_id_), K(table_schema_.get_table_name_str()));
    }
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (is_unified_catalog_) {
    if (OB_FAIL(complete_unified_catalog(result))) {
      LOG_WARN("failed to complete Unified Catalog create table sync",
               KR(ret), K(tenant_id_), K(result.state_), K(is_unified_catalog_));
    }
  } else if (OB_FAIL(complete_hms(result))) {
    LOG_WARN("failed to complete HMS create table sync",
             KR(ret), K(tenant_id_), K(result.state_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::complete_unified_catalog(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  switch (result.state_) {
    case CATALOG_SYNC_DDL_NOT_STARTED:
    case CATALOG_SYNC_DDL_NOOP:
    case CATALOG_SYNC_DDL_REJECTED: {
      cancel_unified_catalog_reservation(result.ddl_ret_);
      break;
    }
    case CATALOG_SYNC_DDL_SUCCEEDED: {
      if (reserved_ && OB_FAIL(complete_unified_catalog_create(result))) {
        LOG_WARN("failed to finalize Unified Catalog create table sync",
                 KR(ret), K(tenant_id_), K(database_name_), K(table_name_));
      }
      break;
    }
    case CATALOG_SYNC_DDL_UNKNOWN: {
      if (reserved_) {
        LOG_WARN_RET(result.ddl_ret_,
                     "local create rollback is unconfirmed; Unified Catalog reservation retained",
                     K(tenant_id_), K(database_name_), K(table_name_));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected catalog sync DDL state", K(ret), K(result.state_));
      break;
    }
  }
  return ret;
}

void ObCreateTableCatalogSyncOperation::cancel_unified_catalog_reservation(const int ddl_ret)
{
  if (!operation_id_.empty() && target_.is_captured()) {
    int tmp_ret = OB_SUCCESS;
    ObWarningBufferIgnoreScope warning_buffer_guard;
    if (OB_TMP_FAIL(ObUnifiedCatalog::cancel_create_table(target_,
                                                          database_name_,
                                                          table_name_,
                                                          operation_id_))) {
      LOG_WARN_RET(ddl_ret,
                   "failed to cancel Unified Catalog table reservation",
                   KR(tmp_ret), K(tenant_id_), K(database_name_), K(table_name_));
    }
  }
}

int ObCreateTableCatalogSyncOperation::complete_unified_catalog_create(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *created_table_schema = NULL;
  const ObDatabaseSchema *created_database_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == result.object_id_
             || OB_INVALID_VERSION == result.schema_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created table result has no source identity",
             KR(ret), K(result.object_id_), K(result.schema_version_));
  } else if (OB_FAIL(GCTX.schema_service_->async_refresh_schema(
                 tenant_id_, result.schema_version_))) {
    LOG_WARN("failed to refresh schema for Unified Catalog table sync",
             KR(ret), K(tenant_id_), K(result.schema_version_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for Unified Catalog table sync", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, result.object_id_, created_table_schema))) {
    LOG_WARN("failed to get created table schema for Unified Catalog sync",
             KR(ret), K(tenant_id_), K(result.object_id_));
  } else if (OB_ISNULL(created_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created table schema is null for Unified Catalog sync",
             KR(ret), K(tenant_id_), K(result.object_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, created_table_schema->get_database_id(), created_database_schema))) {
    LOG_WARN("failed to get database schema for Unified Catalog sync",
             KR(ret), K(tenant_id_), K(created_table_schema->get_database_id()));
  } else if (OB_ISNULL(created_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null for Unified Catalog sync", KR(ret), K(tenant_id_));
  } else if (created_table_schema->get_database_id() != database_id_
             || created_database_schema->get_database_name_str() != database_name_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("created table database changed after Unified Catalog reservation",
             KR(ret), K(database_id_), K(created_table_schema->get_database_id()));
  } else if (OB_FAIL(ObUnifiedCatalog::complete_create_table(target_,
                                                              database_name_,
                                                              *created_table_schema,
                                                              operation_id_))) {
    LOG_WARN("failed to complete created table in Unified Catalog",
             KR(ret), K(tenant_id_),
             K(created_database_schema->get_database_name_str()),
             K(created_table_schema->get_table_name_str()));
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::complete_hms(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_
      && ObCatalogSyncHelper::is_sync_enabled(tenant_id_)
      && is_partitioned_table_
      && OB_INVALID_ID != sync_catalog_id_
      && OB_FAIL(reconcile_hms_partitions(result))) {
    LOG_WARN("failed to reconcile HMS create table partitions",
             KR(ret), K(tenant_id_), K(result.object_id_));
  }
  return ret;
}

int ObCreateTableCatalogSyncOperation::reconcile_hms_partitions(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *created_table_schema = NULL;
  const ObDatabaseSchema *created_database_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->async_refresh_schema(
                 tenant_id_, result.schema_version_))) {
    LOG_WARN("failed to async refresh schema for initial partition sync",
             KR(ret), K(tenant_id_), K(result.schema_version_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for initial partition sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, result.object_id_, created_table_schema))) {
    LOG_WARN("failed to get created table schema for initial partition sync",
             KR(ret), K(tenant_id_), K(result.object_id_));
  } else if (OB_ISNULL(created_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created table schema is null", KR(ret), K(tenant_id_), K(result.object_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, created_table_schema->get_database_id(), created_database_schema))) {
    LOG_WARN("failed to get database schema for initial partition sync", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(created_database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created database schema is null", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObCatalogSyncHelper::reconcile_hms_partitions(
                 tenant_id_,
                 sync_catalog_id_,
                 schema_guard,
                 created_database_schema->get_database_name_str(),
                 *created_table_schema))) {
    LOG_WARN("failed to reconcile created table partitions in HMS",
             KR(ret), K(tenant_id_),
             K(created_database_schema->get_database_name_str()),
             K(created_table_schema->get_table_name_str()));
  }
  return ret;
}

int ObCatalogSync::get_create_table_operator(const ObCatalogSyncBackend backend,
                                             const ObString &access_token,
                                             const ObString &database_name,
                                             const obrpc::ObCreateTableArg &create_table_arg,
                                             ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTableSchema &table_schema = create_table_arg.schema_;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  handle.reset();
  if (table_schema.is_user_table() && ObCatalogSyncBackend::NONE != backend) {
    const bool is_unified_catalog = ObCatalogSyncBackend::UNIFIED_CATALOG == backend;
    ObCreateTableCatalogSyncOperation *operation = OB_NEWx(ObCreateTableCatalogSyncOperation,
                                                           (&allocator_),
                                                           tenant_id,
                                                           is_unified_catalog,
                                                           create_table_arg.if_not_exist_,
                                                           access_token,
                                                           database_name,
                                                           table_schema);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate create table catalog sync operation",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog));
    } else {
      handle.set(allocator_, operation);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
