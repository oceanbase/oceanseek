/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_database_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/catalog/ob_catalog_meta_getter.h"
#include "share/catalog/ob_catalog_meta_writer.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/catalog/ob_unified_catalog_config.h"
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

ObCreateDatabaseCatalogSyncOperation::ObCreateDatabaseCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const ObString &access_token,
    const ObString &database_name,
    const ObString &remote_catalog_name,
    const ObString &endpoint,
    const ObString &realm)
  : tenant_id_(tenant_id),
    is_unified_catalog_(is_unified_catalog),
    access_token_(access_token),
    database_name_(database_name),
    remote_catalog_name_(remote_catalog_name),
    endpoint_(endpoint),
    realm_(realm)
{
}

ObCreateDatabaseCatalogSyncOperation::~ObCreateDatabaseCatalogSyncOperation()
{
}

ObDropDatabaseCatalogSyncOperation::ObDropDatabaseCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const bool is_hms_sync_enabled,
    const ObString &access_token,
    const ObString &database_name)
  : tenant_id_(tenant_id),
    is_unified_catalog_(is_unified_catalog),
    is_hms_sync_enabled_(is_hms_sync_enabled),
    access_token_(access_token),
    database_name_(database_name),
    database_id_(OB_INVALID_ID),
    sync_catalog_id_(OB_INVALID_ID),
    allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id)
{
}

ObDropDatabaseCatalogSyncOperation::~ObDropDatabaseCatalogSyncOperation()
{
}

int ObCreateDatabaseCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (is_unified_catalog_) {
    if (OB_FAIL(prepare_unified_catalog())) {
      LOG_WARN("failed to prepare Unified Catalog create database sync",
               KR(ret), K(tenant_id_), K(database_name_), K(is_unified_catalog_));
    }
  } else if (!ObUnifiedCatalog::is_configured(tenant_id_)
             && ObCatalogSyncHelper::is_sync_enabled(tenant_id_)
             && OB_FAIL(prepare_hms())) {
    LOG_WARN("failed to prepare HMS create database sync",
             KR(ret), K(tenant_id_), K(database_name_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateDatabaseCatalogSyncOperation::prepare_hms()
{
  int ret = OB_SUCCESS;
  uint64_t sync_catalog_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS create database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard for create database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id))) {
    LOG_WARN("failed to get synced catalog id for create database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID != sync_catalog_id) {
    ObArenaAllocator allocator("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.create_namespace(tenant_id_, sync_catalog_id, database_name_))) {
      if (OB_ERR_ALREADY_EXISTS == ret) {
        LOG_INFO("HMS namespace already exists, treat create database sync as success",
                 K(database_name_), K(sync_catalog_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync create database to HMS",
                 KR(ret), K(database_name_), K(sync_catalog_id));
      }
    } else {
      LOG_INFO("synced create database to HMS", K(database_name_), K(sync_catalog_id));
    }
  }
  return ret;
}

int ObCreateDatabaseCatalogSyncOperation::prepare_unified_catalog()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObUnifiedCatalog::validate_namespace_target(
          tenant_id_, remote_catalog_name_, endpoint_, realm_))) {
    LOG_WARN("Unified Catalog create database target changed",
             KR(ret), K(tenant_id_), K(database_name_));
  }
  return ret;
}

int ObCreateDatabaseCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (is_unified_catalog_ && OB_FAIL(complete_unified_catalog(result))) {
    LOG_WARN("failed to complete Unified Catalog create database sync",
             KR(ret), K(tenant_id_), K(database_name_), K(result.state_),
             K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateDatabaseCatalogSyncOperation::complete_unified_catalog(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null for Unified Catalog create database sync",
               KR(ret), K(tenant_id_));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   tenant_id_, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard for Unified Catalog namespace sync",
               KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObUnifiedCatalog::create_namespace(
                   tenant_id_,
                   access_token_,
                   database_name_,
                   result.object_id_,
                   remote_catalog_name_,
                   endpoint_,
                   realm_))) {
      LOG_WARN("failed to sync create database to Unified Catalog",
               KR(ret), K(database_name_), K(result.object_id_));
    } else {
      LOG_INFO("synced create database to Unified Catalog",
               K(database_name_), K(result.object_id_));
    }
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  sync_catalog_id_ = OB_INVALID_ID;
  const bool current_is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id_);
  const bool current_is_hms_sync_enabled =
      !current_is_unified_catalog && ObCatalogSyncHelper::is_sync_enabled(tenant_id_);
  if (is_unified_catalog_ != current_is_unified_catalog
      || is_hms_sync_enabled_ != current_is_hms_sync_enabled) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("drop database catalog sync backend changed before prepare",
             KR(ret), K(tenant_id_), K(is_unified_catalog_),
             K(is_hms_sync_enabled_), K(current_is_unified_catalog),
             K(current_is_hms_sync_enabled));
  } else if (is_unified_catalog_) {
    if (OB_FAIL(prepare_unified_catalog())) {
      LOG_WARN("failed to prepare Unified Catalog drop database sync",
               KR(ret), K(tenant_id_), K(database_name_));
    }
    result.source_object_id_ = database_id_;
  } else if (is_hms_sync_enabled_ && OB_FAIL(prepare_hms())) {
    LOG_WARN("failed to prepare HMS drop database sync",
             KR(ret), K(tenant_id_), K(database_name_));
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::prepare_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS drop database check",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard for drop database check",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id_))) {
    LOG_WARN("failed to get synced catalog id for drop database check",
             KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID != sync_catalog_id_) {
    ObArenaAllocator allocator("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObCatalogMetaGetter meta_getter(schema_guard, allocator);
    ObArray<ObString> table_names;
    if (OB_FAIL(meta_getter.list_table_names(tenant_id_,
                                             sync_catalog_id_,
                                             database_name_,
                                             OB_ORIGIN_AND_INSENSITIVE,
                                             table_names))) {
      LOG_WARN("failed to list remote tables for drop database check",
               KR(ret), K(database_name_), K(sync_catalog_id_));
    } else if (table_names.count() > 0) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "drop database with remote tables in synced catalog");
      LOG_WARN("cannot drop database with remote non-placeholder tables in synced catalog",
               KR(ret), K(database_name_), K(sync_catalog_id_), K(table_names.count()));
    }
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::prepare_unified_catalog()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObUnifiedCatalogConfig config;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for Unified Catalog drop database check",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard for drop database check",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_database_id(
                 tenant_id_, database_name_, database_id_))) {
    LOG_WARN("failed to resolve database source identity before drop",
             KR(ret), K(tenant_id_), K(database_name_));
  } else if (OB_INVALID_ID == database_id_) {
    LOG_INFO("skip Unified Catalog namespace inspection without a local source identity",
             K(tenant_id_), K(database_name_));
  } else if (OB_FAIL(ObUnifiedCatalog::resolve_target(
                 tenant_id_, allocator_, remote_catalog_name_, &config))) {
    LOG_WARN("failed to resolve Unified Catalog drop database target",
             KR(ret), K(tenant_id_));
  } else {
    endpoint_ = config.endpoint_;
    realm_ = config.realm_;
    if (OB_FAIL(ObUnifiedCatalog::check_namespace_empty(
            tenant_id_,
            access_token_,
            database_name_,
            database_id_,
            remote_catalog_name_,
            endpoint_,
            realm_))) {
      LOG_WARN("failed to check Unified Catalog namespace before drop database",
               KR(ret), K(tenant_id_), K(database_name_), K(database_id_));
    }
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    const bool current_is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id_);
    const bool current_is_hms_sync_enabled =
        !current_is_unified_catalog && ObCatalogSyncHelper::is_sync_enabled(tenant_id_);
    if (is_unified_catalog_ != current_is_unified_catalog
        || is_hms_sync_enabled_ != current_is_hms_sync_enabled) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("drop database catalog sync backend changed",
               KR(ret), K(tenant_id_), K(is_unified_catalog_),
               K(is_hms_sync_enabled_), K(current_is_unified_catalog),
               K(current_is_hms_sync_enabled));
    } else if (is_unified_catalog_ && OB_FAIL(complete_unified_catalog())) {
      LOG_WARN("failed to complete Unified Catalog drop database sync",
               KR(ret), K(tenant_id_), K(database_name_));
    } else if (is_hms_sync_enabled_ && OB_FAIL(complete_hms())) {
      LOG_WARN("failed to complete HMS drop database sync",
               KR(ret), K(tenant_id_), K(database_name_));
    }
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::complete_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t current_sync_catalog_id = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS drop database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard for drop database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, current_sync_catalog_id))) {
    LOG_WARN("failed to resolve HMS drop database target",
             KR(ret), K(tenant_id_));
  } else if (sync_catalog_id_ != current_sync_catalog_id) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("HMS drop database target changed",
             KR(ret), K(tenant_id_), K(sync_catalog_id_),
             K(current_sync_catalog_id));
  } else if (OB_INVALID_ID != sync_catalog_id_) {
    ObArenaAllocator allocator("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    if (OB_FAIL(meta_writer.drop_namespace(
            tenant_id_, sync_catalog_id_, database_name_, false, true))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("HMS namespace already absent, treat sync drop database as success",
                 K(database_name_), K(sync_catalog_id_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to sync drop database to HMS",
                 KR(ret), K(database_name_), K(sync_catalog_id_));
      }
    } else {
      LOG_INFO("synced drop database to HMS", K(database_name_), K(sync_catalog_id_));
    }
  }
  return ret;
}

int ObDropDatabaseCatalogSyncOperation::complete_unified_catalog()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for Unified Catalog drop database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard for drop database sync",
             KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == database_id_) {
    LOG_INFO("skip Unified Catalog namespace drop without a local source identity",
             K(tenant_id_), K(database_name_));
  } else if (OB_FAIL(ObUnifiedCatalog::drop_namespace(
                 tenant_id_,
                 access_token_,
                 database_name_,
                 remote_catalog_name_,
                 endpoint_,
                 realm_))) {
    LOG_WARN("failed to sync drop database to Unified Catalog",
             KR(ret), K(database_name_), K(database_id_));
  } else {
    LOG_INFO("synced drop database to Unified Catalog", K(database_name_), K(database_id_));
  }
  return ret;
}

int ObCatalogSync::get_create_database_operator(
    ObExecContext &ctx,
    const obrpc::ObCreateDatabaseArg &create_database_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema &database_schema = create_database_arg.database_schema_;
  const uint64_t tenant_id = database_schema.get_tenant_id();
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id);
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObString access_token;
  ObString remote_catalog_name;
  ObString endpoint;
  ObString realm;
  handle.reset();
  if (OB_NOT_NULL(session)
      && ObUnifiedCatalog::is_configured(session->get_effective_tenant_id())
      && OB_FAIL(ObUnifiedCatalog::get_session_token(
          *session, allocator_, access_token))) {
    LOG_WARN("failed to get Unified Catalog execution token",
             KR(ret), K(tenant_id));
  } else if (is_unified_catalog
      && OB_FAIL(ObUnifiedCatalog::validate_sync_target(
          tenant_id,
          access_token,
          allocator_,
          remote_catalog_name,
          endpoint,
          realm))) {
    LOG_WARN("failed to validate Unified Catalog create database sync target",
             KR(ret), K(tenant_id));
  } else {
    ObCreateDatabaseCatalogSyncOperation *operation =
        OB_NEWx(ObCreateDatabaseCatalogSyncOperation,
                (&allocator_),
                tenant_id,
                is_unified_catalog,
                access_token,
                database_schema.get_database_name_str(),
                remote_catalog_name,
                endpoint,
                realm);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate create database catalog sync operation",
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

int ObCatalogSync::prepare_drop_database_context(
    ObExecContext &ctx,
    const obrpc::ObDropDatabaseArg &drop_database_arg,
    ObCatalogSyncAuthContext &context)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = drop_database_arg.tenant_id_;
  ObSQLSessionInfo *session = ctx.get_my_session();
  context.access_token_.reset();
  context.is_unified_catalog_ = ObUnifiedCatalog::is_configured(tenant_id);
  context.is_hms_sync_enabled_ =
      !context.is_unified_catalog_ && ObCatalogSyncHelper::is_sync_enabled(tenant_id);
  if (OB_NOT_NULL(session)
      && context.is_unified_catalog_
      && OB_FAIL(ObUnifiedCatalog::get_session_token(
          *session, allocator_, context.access_token_))) {
    LOG_WARN("failed to capture Unified Catalog access token for drop database operation",
             KR(ret), K(tenant_id), K(context.is_unified_catalog_));
  }
  return ret;
}

int ObCatalogSync::get_drop_database_operator(
    const ObCatalogSyncAuthContext &context,
    const obrpc::ObDropDatabaseArg &drop_database_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = drop_database_arg.tenant_id_;
  const bool need_operation =
      context.is_unified_catalog_ || context.is_hms_sync_enabled_;
  handle.reset();
  if (need_operation) {
    ObDropDatabaseCatalogSyncOperation *operation =
        OB_NEWx(ObDropDatabaseCatalogSyncOperation,
                (&allocator_),
                tenant_id,
                context.is_unified_catalog_,
                context.is_hms_sync_enabled_,
                context.access_token_,
                drop_database_arg.database_name_);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate drop database catalog sync operation",
               KR(ret), K(tenant_id), K(context.is_unified_catalog_),
               K(context.is_hms_sync_enabled_));
    } else {
      handle.set(allocator_, operation);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
