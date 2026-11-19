/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_create_table_as_select_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "share/catalog/ob_catalog_meta_writer.h"
#include "share/catalog/ob_unified_catalog_config.h"
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

ObCreateTableAsSelectCatalogSyncOperation::ObCreateTableAsSelectCatalogSyncOperation(
    const ObString &access_token,
    ObCreateTableStmt &stmt,
    const bool is_unified_catalog)
    : stmt_(stmt), is_unified_catalog_(is_unified_catalog),
      tenant_id_(stmt.get_create_table_arg().schema_.get_tenant_id()),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_),
      target_(tenant_id_, access_token),
      unified_catalog_database_name_(), unified_catalog_table_name_(), raw_config_(),
      hms_database_name_(), hms_table_name_(), sync_placeholder_created_(false),
      sync_catalog_id_(OB_INVALID_ID), unified_catalog_database_id_(OB_INVALID_ID),
      unified_catalog_table_id_(OB_INVALID_ID)
{
}

ObCreateTableAsSelectCatalogSyncOperation::~ObCreateTableAsSelectCatalogSyncOperation()
{
}

int ObCreateTableAsSelectCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (is_unified_catalog_ && OB_FAIL(prepare_unified_catalog())) {
    LOG_WARN("failed to prepare Unified Catalog create table as select sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::prepare_unified_catalog()
{
  int ret = OB_SUCCESS;
  ObString raw_catalog_name;
  ObUnifiedCatalogConfig raw_target_config;
  if (stmt_.get_create_table_arg().schema_.is_user_table()) {
    if (OB_FAIL(prepare_unified_catalog_target(target_, allocator_))) {
      LOG_WARN("failed to prepare Unified Catalog create table as select target",
               KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObUnifiedCatalog::snapshot_raw_target(tenant_id_,
                                                             allocator_,
                                                             raw_catalog_name,
                                                             raw_config_))) {
      LOG_WARN("failed to preserve Unified Catalog create table as select target",
               KR(ret),
               K(tenant_id_));
    } else if (OB_FAIL(raw_target_config.parse(raw_config_, allocator_))) {
      LOG_WARN("failed to parse Unified Catalog create table as select target",
               KR(ret),
               K(tenant_id_));
    } else if (raw_catalog_name != target_.catalog_name()
               || raw_target_config.endpoint_ != target_.endpoint()
               || raw_target_config.realm_ != target_.realm()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog create table as select target changed during prepare",
               KR(ret),
               K(tenant_id_),
               "expected_catalog_name", target_.catalog_name(),
               K(raw_catalog_name));
    }
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if (is_unified_catalog_ && OB_FAIL(complete_unified_catalog(result))) {
      LOG_WARN("failed to complete Unified Catalog create table as select sync",
               KR(ret), K(tenant_id_), K(is_unified_catalog_));
    } else if (!is_unified_catalog_ && OB_FAIL(complete_hms(result))) {
      LOG_WARN("failed to complete HMS create table as select sync",
               KR(ret), K(tenant_id_), K(is_unified_catalog_));
    }
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::get_final_table_schema(
    const uint64_t table_id,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for create table as select sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("final create table as select table id is invalid",
             KR(ret), K(tenant_id_), K(table_id), K(is_unified_catalog_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard for create table as select sync",
             KR(ret), K(tenant_id_), K(is_unified_catalog_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get final create table as select schema",
             KR(ret), K(tenant_id_), K(table_id), K(is_unified_catalog_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("final create table as select schema is null",
             KR(ret), K(tenant_id_), K(table_id), K(is_unified_catalog_));
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::complete_unified_catalog(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  const obrpc::ObCreateTableArg &create_table_arg = stmt_.get_create_table_arg();
  if (OB_FAIL(get_final_table_schema(result.object_id_,
                                     schema_guard,
                                     table_schema))) {
    LOG_WARN("failed to resolve final table for Unified Catalog sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, table_schema->get_database_id(), database_schema))) {
    LOG_WARN("failed to resolve final database for Unified Catalog sync",
             KR(ret), K(tenant_id_), K(table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("final database schema is null for Unified Catalog sync",
             KR(ret), K(tenant_id_), K(table_schema->get_database_id()));
  } else if (OB_FAIL(ob_write_string(allocator_,
                                     database_schema->get_database_name_str(),
                                     unified_catalog_database_name_))) {
    LOG_WARN("failed to preserve Unified Catalog create table as select database name",
             KR(ret),
             K(tenant_id_));
  } else if (OB_FAIL(ob_write_string(allocator_,
                                     table_schema->get_table_name_str(),
                                     unified_catalog_table_name_))) {
    LOG_WARN("failed to preserve Unified Catalog create table as select table name",
             KR(ret),
             K(tenant_id_));
  } else {
    unified_catalog_database_id_ = table_schema->get_database_id();
    unified_catalog_table_id_ = table_schema->get_table_id();
    if (OB_FAIL(ObUnifiedCatalog::sync_table_to_target(target_,
                                                       unified_catalog_database_name_,
                                                       *table_schema))) {
      if (OB_ERR_ALREADY_EXISTS == ret && create_table_arg.if_not_exist_) {
        LOG_INFO(
            "Unified Catalog table already exists, tolerate create table as select if not exists",
            K(tenant_id_),
            K(unified_catalog_database_name_),
            K(unified_catalog_table_name_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to publish create table as select to Unified Catalog",
                 KR(ret),
                 K(tenant_id_),
                 K(unified_catalog_database_name_),
                 K(unified_catalog_table_name_));
      }
    }
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::complete_hms(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  const obrpc::ObCreateTableArg &create_table_arg = stmt_.get_create_table_arg();
  if (OB_FAIL(get_final_table_schema(
          result.object_id_, schema_guard, table_schema))) {
    LOG_WARN("failed to resolve final table for HMS sync",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, table_schema->get_database_id(), database_schema))) {
    LOG_WARN("failed to resolve final database for HMS sync",
             KR(ret), K(tenant_id_), K(table_schema->get_database_id()));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("final database schema is null for HMS sync",
             KR(ret), K(tenant_id_), K(table_schema->get_database_id()));
  } else if (OB_FAIL(ob_write_string(
                 allocator_, database_schema->get_database_name_str(), hms_database_name_))) {
    LOG_WARN("failed to preserve HMS create table as select database name",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ob_write_string(
                 allocator_, table_schema->get_table_name_str(), hms_table_name_))) {
    LOG_WARN("failed to preserve HMS create table as select table name",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id_))) {
    LOG_WARN("failed to resolve HMS create table as select target",
             KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == sync_catalog_id_) {
  } else if (OB_FAIL(ObCatalogSyncHelper::sync_table_to_hms(tenant_id_,
                                                            sync_catalog_id_,
                                                            schema_guard,
                                                            hms_database_name_,
                                                            *table_schema))) {
    if (OB_ERR_ALREADY_EXISTS == ret && create_table_arg.if_not_exist_) {
      LOG_INFO("HMS table already exists, tolerate create table as select if not exists",
               K(tenant_id_),
               K(sync_catalog_id_),
               K(hms_database_name_),
               K(hms_table_name_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to publish create table as select to HMS",
               KR(ret), K(tenant_id_), K(sync_catalog_id_),
               K(hms_database_name_), K(hms_table_name_));
    }
  } else {
    sync_placeholder_created_ = true;
    if (table_schema->is_partitioned_table()
        && OB_FAIL(ObCatalogSyncHelper::reconcile_hms_partitions(
               tenant_id_,
               sync_catalog_id_,
               schema_guard,
               hms_database_name_,
               *table_schema))) {
      LOG_WARN("failed to reconcile create table as select partitions in HMS",
               KR(ret), K(tenant_id_), K(sync_catalog_id_),
               K(hms_database_name_), K(hms_table_name_));
    }
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::compensate(const int ddl_ret)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != ddl_ret) {
    if (OB_INVALID_ID != unified_catalog_table_id_ && OB_FAIL(compensate_unified_catalog())) {
      LOG_WARN("failed to compensate Unified Catalog create table as select sync",
               KR(ret),
               K(tenant_id_),
               K(ddl_ret),
               K(unified_catalog_database_id_),
               K(unified_catalog_table_id_));
    } else if (OB_INVALID_ID == unified_catalog_table_id_ && sync_placeholder_created_
               && OB_FAIL(compensate_hms())) {
      LOG_WARN("failed to compensate HMS create table as select sync",
               KR(ret),
               K(tenant_id_),
               K(ddl_ret),
               K(is_unified_catalog_));
    }
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::compensate_unified_catalog()
{
  int ret = OB_SUCCESS;
  if (!ObUnifiedCatalog::is_configured(tenant_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog backend changed before create table as select compensation",
             KR(ret),
             K(tenant_id_),
             K(unified_catalog_database_name_),
             K(unified_catalog_table_name_));
  } else if (OB_FAIL(ObUnifiedCatalog::drop_table(target_.tenant_id(),
                                                  target_.access_token(),
                                                  unified_catalog_database_name_,
                                                  unified_catalog_table_name_,
                                                  target_.catalog_name(),
                                                  raw_config_))) {
    LOG_WARN("failed to drop Unified Catalog table after local create table as select cleanup",
             KR(ret),
             K(tenant_id_),
             K(unified_catalog_database_name_),
             K(unified_catalog_database_id_),
             K(unified_catalog_table_name_),
             K(unified_catalog_table_id_));
  }
  return ret;
}

int ObCreateTableAsSelectCatalogSyncOperation::compensate_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS create table as select compensation",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for HMS create table as select compensation",
             KR(ret), K(tenant_id_));
  } else {
    ObCatalogMetaWriter meta_writer(schema_guard, allocator_);
    if (OB_FAIL(meta_writer.drop_table(tenant_id_,
                                       sync_catalog_id_,
                                       hms_database_name_,
                                       hms_table_name_))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("HMS create table as select placeholder is already absent",
                 K(tenant_id_), K(sync_catalog_id_), K(hms_database_name_),
                 K(hms_table_name_));
        ret = OB_SUCCESS;
      } else if (OB_OP_NOT_ALLOW == ret) {
        LOG_INFO("skip dropping HMS create table as select placeholder not owned by current tenant",
                 K(tenant_id_), K(sync_catalog_id_), K(hms_database_name_),
                 K(hms_table_name_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to drop HMS create table as select placeholder",
                 KR(ret), K(tenant_id_), K(sync_catalog_id_),
                 K(hms_database_name_), K(hms_table_name_));
      }
    }
  }
  return ret;
}

int ObCatalogSync::get_create_table_as_select_operator(
    const ObCatalogSyncBackend backend,
    const ObString &access_token,
    ObCreateTableStmt &stmt,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObTableSchema &table_schema = stmt.get_create_table_arg().schema_;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  handle.reset();
  if (ObCatalogSyncBackend::NONE != backend) {
    const bool is_unified_catalog = ObCatalogSyncBackend::UNIFIED_CATALOG == backend;
    ObCreateTableAsSelectCatalogSyncOperation *operation =
        OB_NEWx(ObCreateTableAsSelectCatalogSyncOperation,
                (&allocator_),
                access_token,
                stmt,
                is_unified_catalog);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate create table as select catalog sync operation",
               KR(ret), K(tenant_id),
               K(is_unified_catalog));
      if (!is_unified_catalog) {
        // Report HMS allocation failure only at the original post-local sync point.
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
