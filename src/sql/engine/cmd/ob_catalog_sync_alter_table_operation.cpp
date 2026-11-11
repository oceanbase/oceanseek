/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_alter_table_operation.h"
#include "common/ob_timeout_ctx.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/string/ob_string.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/catalog/ob_catalog_meta_writer.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

ObAlterTableCatalogSyncOperation::ObAlterTableCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    ObExecContext &ctx,
    const obrpc::ObAlterTableArg &alter_table_arg)
    : tenant_id_(tenant_id), is_unified_catalog_(is_unified_catalog), ctx_(ctx),
      alter_table_arg_(alter_table_arg),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id), access_token_(),
      remote_catalog_name_(),
      endpoint_(),
      realm_()
{
}

ObAlterTableCatalogSyncOperation::~ObAlterTableCatalogSyncOperation()
{
}

int ObAlterTableCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (is_unified_catalog_ && OB_FAIL(prepare_unified_catalog())) {
    LOG_WARN("failed to prepare Unified Catalog alter table sync",
             KR(ret),
             K(tenant_id_),
             K(is_unified_catalog_));
  }
  return ret;
}

int ObAlterTableCatalogSyncOperation::prepare_unified_catalog()
{
  int ret = OB_SUCCESS;
  if (!is_supported_uc_partition_alter(alter_table_arg_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ALTER TABLE is not supported while UC Schema sync is enabled", K(ret), K(tenant_id_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ALTER TABLE with Unified Catalog schema synchronization");
  } else if (OB_FAIL(validate_uc_partition_alter_source(tenant_id_, alter_table_arg_))) {
    LOG_WARN("failed to validate source table before UC partition ALTER", K(ret), K(tenant_id_));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null before Unified Catalog alter table sync", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObUnifiedCatalog::get_session_token(*ctx_.get_my_session(),
                                                         allocator_,
                                                         access_token_))) {
    LOG_WARN("failed to get Unified Catalog execution token", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObUnifiedCatalog::validate_sync_target(tenant_id_,
                                                            access_token_,
                                                            allocator_,
                                                            remote_catalog_name_,
                                                            endpoint_,
                                                            realm_))) {
    LOG_WARN("failed to validate Unified Catalog sync target", K(ret), K(tenant_id_));
  }
  return ret;
}

bool ObAlterTableCatalogSyncOperation::is_supported_uc_partition_alter(
    const obrpc::ObAlterTableArg &arg)
{
  return arg.is_alter_partitions_ && !arg.is_alter_columns_ && !arg.is_alter_indexs_
         && !arg.is_alter_options_ && !arg.is_convert_to_character_
         && (obrpc::ObAlterTableArg::ADD_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::DROP_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::ADD_SUB_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::DROP_SUB_PARTITION == arg.alter_part_type_);
}

int ObAlterTableCatalogSyncOperation::validate_uc_partition_alter_source(
    const uint64_t tenant_id,
    const obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const uint64_t table_id
      = OB_INVALID_ID == arg.table_id_ ? arg.alter_table_schema_.get_table_id() : arg.table_id_;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null before UC partition ALTER", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source table id is absent before UC partition ALTER", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard before UC partition ALTER", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get source table before UC partition ALTER", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source table is absent before UC partition ALTER", K(ret), K(table_id));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(*table_schema))) {
    LOG_WARN("source table is not supported by UC partition sync", K(ret), K(table_id));
  }
  return ret;
}

int ObAlterTableCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if ((is_unified_catalog_ || ObCatalogSyncHelper::is_sync_enabled(tenant_id_))
        && OB_FAIL(sync_final_schema(result, is_unified_catalog_))) {
      LOG_WARN("failed to complete alter table catalog sync",
               KR(ret),
               K(tenant_id_),
               K(is_unified_catalog_));
    }
  }
  return ret;
}

int ObAlterTableCatalogSyncOperation::sync_final_schema(const ObCatalogSyncDDLResult &result,
                                                        const bool is_unified_catalog)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  const obrpc::ObAlterTableArg &alter_table_arg = alter_table_arg_;
  ObSQLSessionInfo *my_session = ctx_.get_my_session();
  ObSchemaGetterGuard sync_schema_guard;
  const ObTableSchema *sync_table_schema = NULL;
  const ObDatabaseSchema *sync_db_schema = NULL;
  const uint64_t sync_table_id = OB_INVALID_ID == alter_table_arg.table_id_
                                     ? alter_table_arg.alter_table_schema_.get_table_id()
                                     : alter_table_arg.table_id_;
  // Make sure the local schema guard catches up with this DDL's schema
  // version, otherwise the lookup below may see a stale schema.
  if (OB_INVALID_VERSION != result.schema_version_ && OB_NOT_NULL(my_session)) {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(timeout_ctx.set_timeout(result.schema_sync_timeout_us_))) {
      LOG_WARN("failed to set timeout for catalog sync",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog));
    } else if (OB_FAIL(
                   ObSchemaUtils::try_check_parallel_ddl_schema_in_sync(timeout_ctx,
                                                                        my_session,
                                                                        tenant_id,
                                                                        result.schema_version_,
                                                                        true /*skip_consensus*/))) {
      LOG_WARN("failed to wait schema in sync for catalog sync",
               KR(ret),
               K(tenant_id),
               K(result.schema_version_),
               K(sync_table_id),
               K(is_unified_catalog));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret), K(is_unified_catalog));
    } else if (OB_FAIL(
                   GCTX.schema_service_->get_tenant_schema_guard(tenant_id, sync_schema_guard))) {
      LOG_WARN("failed to get tenant schema guard for alter table sync",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog));
    } else if (OB_FAIL(sync_schema_guard.get_table_schema(tenant_id,
                                                          sync_table_id,
                                                          sync_table_schema))) {
      LOG_WARN("failed to get table schema for alter table sync",
               KR(ret),
               K(tenant_id),
               K(sync_table_id),
               K(is_unified_catalog));
    } else if (OB_ISNULL(sync_table_schema)
               // Offline DDL rebuilds the table under a new table id and
               // recycles the original one, so lookup by the old id may miss.
               // Fall back to the final database/table name in the arg.
               && OB_FAIL(sync_schema_guard.get_table_schema(
                   tenant_id,
                   alter_table_arg.alter_table_schema_.get_database_name(),
                   alter_table_arg.alter_table_schema_.get_table_name_str(),
                   false /*is_index*/,
                   sync_table_schema))) {
      LOG_WARN("failed to get table schema for alter table sync by name",
               KR(ret),
               K(tenant_id),
               K(sync_table_id),
               "database_name",
               alter_table_arg.alter_table_schema_.get_database_name(),
               "table_name",
               alter_table_arg.alter_table_schema_.get_table_name_str(),
               K(is_unified_catalog));
    } else if (OB_ISNULL(sync_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync table schema is null",
               KR(ret),
               K(tenant_id),
               K(sync_table_id),
               K(is_unified_catalog));
    } else if (sync_table_schema->is_index_table() || sync_table_schema->is_aux_table()) {
      // do nothing
    } else if (OB_FAIL(sync_schema_guard.get_database_schema(tenant_id,
                                                             sync_table_schema->get_database_id(),
                                                             sync_db_schema))) {
      LOG_WARN("failed to get database schema for alter table sync",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog));
    } else if (OB_NOT_NULL(sync_db_schema)) {
      const ObString &new_table_name = sync_table_schema->get_table_name_str();
      const ObString &origin_table_name
          = alter_table_arg.alter_table_schema_.get_origin_table_name();
      const ObString &new_database_name = sync_db_schema->get_database_name_str();
      const ObString &origin_database_name
          = alter_table_arg.alter_table_schema_.get_origin_database_name().empty()
                ? new_database_name
                : alter_table_arg.alter_table_schema_.get_origin_database_name();
      const bool is_rename
          = !origin_table_name.empty() && 0 != origin_table_name.case_compare(new_table_name);
      if (is_unified_catalog) {
        if (OB_FAIL(ObUnifiedCatalog::sync_table_to_target(tenant_id_,
                                                           access_token_,
                                                           remote_catalog_name_,
                                                           endpoint_,
                                                           realm_,
                                                           new_database_name,
                                                           *sync_table_schema))) {
          LOG_WARN("failed to sync altered table to Unified Catalog",
                   KR(ret),
                   K(tenant_id),
                   K(new_database_name),
                   K(new_table_name));
        }
      } else {
        uint64_t sync_catalog_id = OB_INVALID_ID;
        if (OB_FAIL(
                ObSQLUtils::get_synced_catalog_id(sync_schema_guard, tenant_id, sync_catalog_id))) {
          LOG_WARN("failed to resolve HMS alter table target", KR(ret), K(tenant_id));
        } else if (OB_INVALID_ID != sync_catalog_id) {
          ObArenaAllocator allocator("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
          ObCatalogMetaWriter meta_writer(sync_schema_guard, allocator);
          if (is_rename) {
            if (OB_FAIL(meta_writer.drop_table(tenant_id,
                                               sync_catalog_id,
                                               origin_database_name,
                                               origin_table_name))) {
              if (OB_TABLE_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
                LOG_INFO("HMS placeholder table already absent, treat rename drop as success",
                         K(tenant_id),
                         K(sync_catalog_id),
                         K(origin_database_name),
                         K(origin_table_name));
                ret = OB_SUCCESS;
              } else if (OB_OP_NOT_ALLOW == ret) {
                LOG_INFO("skip dropping HMS table not owned by current tenant during rename",
                         K(tenant_id),
                         K(sync_catalog_id),
                         K(origin_database_name),
                         K(origin_table_name));
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to sync drop old table for rename",
                         KR(ret),
                         K(tenant_id),
                         K(sync_catalog_id),
                         K(origin_database_name),
                         K(origin_table_name),
                         K(new_table_name));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObCatalogSyncHelper::sync_table_to_hms(tenant_id,
                                                                 sync_catalog_id,
                                                                 sync_schema_guard,
                                                                 new_database_name,
                                                                 *sync_table_schema))) {
                LOG_WARN("failed to sync create new table for rename",
                         KR(ret),
                         K(tenant_id),
                         K(sync_catalog_id),
                         K(new_database_name),
                         K(new_table_name),
                         K(origin_table_name));
              } else if (sync_table_schema->is_partitioned_table()
                         && OB_FAIL(
                             ObCatalogSyncHelper::reconcile_hms_partitions(tenant_id,
                                                                           sync_catalog_id,
                                                                           sync_schema_guard,
                                                                           new_database_name,
                                                                           *sync_table_schema))) {
                LOG_WARN("failed to sync initial partitions after table rename",
                         KR(ret),
                         K(tenant_id),
                         K(sync_catalog_id),
                         K(new_database_name),
                         K(new_table_name),
                         K(origin_table_name));
              }
            }
          } else if (OB_FAIL(meta_writer.alter_table(tenant_id,
                                                     sync_catalog_id,
                                                     new_database_name,
                                                     *sync_table_schema))) {
            LOG_WARN("failed to sync alter table to HMS",
                     KR(ret),
                     K(tenant_id),
                     K(sync_catalog_id),
                     K(new_database_name),
                     K(new_table_name));
          } else if (ObCatalogSyncHelper::need_reconcile_partitions(alter_table_arg)
                     && OB_FAIL(
                         ObCatalogSyncHelper::reconcile_hms_partitions(tenant_id,
                                                                       sync_catalog_id,
                                                                       sync_schema_guard,
                                                                       new_database_name,
                                                                       *sync_table_schema))) {
            LOG_WARN("failed to reconcile altered table partitions in HMS",
                     KR(ret),
                     K(tenant_id),
                     K(sync_catalog_id),
                     K(new_database_name),
                     K(new_table_name),
                     K(alter_table_arg.alter_part_type_));
          }
        }
      }
    }
  }
  return ret;
}

int ObCatalogSync::get_alter_table_operator(ObExecContext &ctx,
                                            const obrpc::ObAlterTableArg &alter_table_arg,
                                            ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = alter_table_arg.alter_table_schema_.get_tenant_id();
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id);
  handle.reset();
  if (!is_unified_catalog || !alter_table_arg.alter_table_schema_.is_external_table()) {
    // HMS enablement is checked at the existing post-DDL completion point.
    ObAlterTableCatalogSyncOperation *operation = OB_NEWx(ObAlterTableCatalogSyncOperation,
                                                          (&allocator_),
                                                          tenant_id,
                                                          is_unified_catalog,
                                                          ctx,
                                                          alter_table_arg);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate alter table catalog sync operation",
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
