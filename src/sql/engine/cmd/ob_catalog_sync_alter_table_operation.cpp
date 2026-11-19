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
    const uint64_t source_table_id,
    const ObString &access_token,
    ObSQLSessionInfo *session,
    const obrpc::ObAlterTableArg &alter_table_arg)
    : tenant_id_(tenant_id), is_unified_catalog_(is_unified_catalog),
      alter_table_arg_(alter_table_arg), session_(session), source_table_id_(source_table_id),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
      target_(tenant_id, access_token)
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
  if (is_unified_catalog_
      && OB_FAIL(prepare_unified_catalog_target(target_, allocator_))) {
    LOG_WARN("failed to prepare Unified Catalog alter table sync",
             KR(ret),
             K(tenant_id_),
             K(is_unified_catalog_));
  } else if (is_unified_catalog_) {
    result.source_object_id_ = source_table_id_;
  }
  return ret;
}

bool ObCatalogSync::is_supported_uc_alter(const obrpc::ObAlterTableArg &arg)
{
  return is_supported_uc_partition_alter(arg) || is_supported_uc_column_alter(arg)
         || is_supported_uc_table_rename(arg);
}

bool ObCatalogSync::is_supported_uc_partition_alter(const obrpc::ObAlterTableArg &arg)
{
  return arg.is_alter_partitions_ && !arg.is_alter_columns_ && !arg.is_alter_indexs_
         && !arg.is_alter_options_ && !arg.is_convert_to_character_
         && (obrpc::ObAlterTableArg::ADD_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::DROP_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::ADD_SUB_PARTITION == arg.alter_part_type_
             || obrpc::ObAlterTableArg::DROP_SUB_PARTITION == arg.alter_part_type_);
}

bool ObCatalogSync::is_supported_uc_column_alter(const obrpc::ObAlterTableArg &arg)
{
  bool supported = arg.is_alter_columns_ && !arg.is_alter_indexs_ && !arg.is_alter_options_
                   && !arg.is_alter_partitions_ && !arg.is_convert_to_character_
                   && obrpc::ObAlterTableArg::CONSTRAINT_NO_OPERATION == arg.alter_constraint_type_
                   && arg.index_arg_list_.empty() && arg.foreign_key_arg_list_.empty()
                   && arg.alter_table_schema_.alter_option_bitset_.is_empty()
                   && 0 == arg.alter_table_schema_.get_constraint_count()
                   && arg.alter_table_schema_.get_foreign_key_infos().empty()
                   && arg.alter_table_schema_.get_column_count() > 0;
  ObTableSchema::const_column_iterator column = arg.alter_table_schema_.column_begin();
  for (; supported && column != arg.alter_table_schema_.column_end(); ++column) {
    const AlterColumnSchema *alter_column = static_cast<const AlterColumnSchema *>(*column);
    supported = OB_NOT_NULL(alter_column)
                && (OB_DDL_ADD_COLUMN == alter_column->alter_type_
                    || OB_DDL_DROP_COLUMN == alter_column->alter_type_);
  }
  return supported;
}

bool ObCatalogSync::is_supported_uc_table_rename(const obrpc::ObAlterTableArg &arg)
{
  return arg.has_rename_action() && arg.is_alter_options_ && !arg.is_alter_columns_
         && !arg.is_alter_indexs_ && !arg.is_alter_partitions_ && !arg.is_convert_to_character_
         && obrpc::ObAlterTableArg::CONSTRAINT_NO_OPERATION == arg.alter_constraint_type_
         && arg.index_arg_list_.empty() && arg.foreign_key_arg_list_.empty()
         && 1 == arg.alter_table_schema_.alter_option_bitset_.num_members();
}

int ObCatalogSync::validate_uc_alter_source(const uint64_t tenant_id,
                                            const obrpc::ObAlterTableArg &arg,
                                            uint64_t &source_table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const uint64_t table_id
      = OB_INVALID_ID == arg.table_id_ ? arg.alter_table_schema_.get_table_id() : arg.table_id_;
  source_table_id = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null before UC ALTER", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source table id is absent before UC ALTER", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard before UC ALTER", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get source table before UC ALTER", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source table is absent before UC ALTER", K(ret), K(table_id));
  } else if (OB_FAIL(ObUnifiedCatalogSchemaCodec::validate_table_schema(*table_schema))) {
    LOG_WARN("source table is not supported by UC ALTER sync", K(ret), K(table_id));
  } else {
    ObTableSchema::const_column_iterator column = arg.alter_table_schema_.column_begin();
    for (; OB_SUCC(ret) && column != arg.alter_table_schema_.column_end(); ++column) {
      const AlterColumnSchema *alter_column = static_cast<const AlterColumnSchema *>(*column);
      if (OB_ISNULL(alter_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alter column schema is null before UC ALTER", K(ret), K(table_id));
      } else if (OB_DDL_ADD_COLUMN == alter_column->alter_type_
                 && OB_FAIL(
                     ObUnifiedCatalogSchemaCodec::validate_alter_add_column(*alter_column))) {
        LOG_WARN("added column is not supported by UC ALTER sync",
                 K(ret),
                 K(table_id),
                 K(alter_column->get_column_name_str()));
      }
    }
    if (OB_SUCC(ret)) {
      source_table_id = table_schema->get_table_id();
    }
  }
  return ret;
}

int ObAlterTableCatalogSyncOperation::complete(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if ((is_unified_catalog_ || ObCatalogSyncHelper::is_sync_enabled(tenant_id_))
        && OB_FAIL(sync_final_schema(result))) {
      LOG_WARN("failed to complete alter table catalog sync",
               KR(ret),
               K(tenant_id_),
               K(is_unified_catalog_));
    }
  }
  return ret;
}

int ObAlterTableCatalogSyncOperation::sync_final_schema(const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  const obrpc::ObAlterTableArg &alter_table_arg = alter_table_arg_;
  ObSQLSessionInfo *my_session = session_;
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
               K(is_unified_catalog_));
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
               K(is_unified_catalog_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret), K(is_unified_catalog_));
    } else if (OB_FAIL(
                   GCTX.schema_service_->get_tenant_schema_guard(tenant_id, sync_schema_guard))) {
      LOG_WARN("failed to get tenant schema guard for alter table sync",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog_));
    } else if (OB_FAIL(sync_schema_guard.get_table_schema(tenant_id,
                                                          sync_table_id,
                                                          sync_table_schema))) {
      LOG_WARN("failed to get table schema for alter table sync",
               KR(ret),
               K(tenant_id),
               K(sync_table_id),
               K(is_unified_catalog_));
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
               K(is_unified_catalog_));
    } else if (OB_ISNULL(sync_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync table schema is null",
               KR(ret),
               K(tenant_id),
               K(sync_table_id),
               K(is_unified_catalog_));
    } else if (sync_table_schema->is_index_table() || sync_table_schema->is_aux_table()) {
      // do nothing
    } else if (OB_FAIL(sync_schema_guard.get_database_schema(tenant_id,
                                                             sync_table_schema->get_database_id(),
                                                             sync_db_schema))) {
      LOG_WARN("failed to get database schema for alter table sync",
               KR(ret),
               K(tenant_id),
               K(is_unified_catalog_));
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
      if (is_unified_catalog_) {
        const bool is_uc_rename = alter_table_arg.has_rename_action();
        if (is_uc_rename
            && OB_FAIL(ObUnifiedCatalog::rename_table_to_target(target_,
                                                                origin_database_name,
                                                                origin_table_name,
                                                                new_database_name,
                                                                *sync_table_schema,
                                                                source_table_id_))) {
          LOG_WARN("failed to rename altered table in Unified Catalog",
                   KR(ret),
                   K(tenant_id),
                   K(origin_database_name),
                   K(origin_table_name),
                   K(new_database_name),
                   K(new_table_name));
        } else if (!is_uc_rename
                   && OB_FAIL(ObUnifiedCatalog::update_table_to_target(target_,
                                                                       new_database_name,
                                                                       *sync_table_schema,
                                                                       source_table_id_))) {
          LOG_WARN("failed to update altered table in Unified Catalog",
                   KR(ret),
                   K(tenant_id),
                   K(new_database_name),
                   K(new_table_name),
                   K(source_table_id_));
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

int ObCatalogSync::precheck_alter_table(const obrpc::ObAlterTableArg &alter_table_arg,
                                        ObCatalogSyncPrecheckResult &precheck_result)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = alter_table_arg.alter_table_schema_.get_tenant_id();
  precheck_result.backend_ = snapshot_backend(tenant_id);
  precheck_result.source_object_id_ = OB_INVALID_ID;
  if (precheck_result.is_unified_catalog()
      && alter_table_arg.alter_table_schema_.is_external_table()) {
    precheck_result.backend_ = ObCatalogSyncBackend::NONE;
  } else if (precheck_result.is_unified_catalog()) {
    if (!is_supported_uc_alter(alter_table_arg)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ALTER TABLE is not supported while UC Schema sync is enabled",
               K(ret),
               K(tenant_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ALTER TABLE with Unified Catalog schema synchronization");
    } else if (OB_FAIL(validate_uc_alter_source(tenant_id,
                                                alter_table_arg,
                                                precheck_result.source_object_id_))) {
      LOG_WARN("failed to validate source table before UC ALTER", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObCatalogSync::get_alter_table_operator(const ObCatalogSyncPrecheckResult &precheck_result,
                                            const ObString &access_token,
                                            ObSQLSessionInfo *session,
                                            const obrpc::ObAlterTableArg &alter_table_arg,
                                            ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = alter_table_arg.alter_table_schema_.get_tenant_id();
  const bool is_unified_catalog = precheck_result.is_unified_catalog();
  handle.reset();
  if (precheck_result.need_operation()) {
    // HMS enablement is checked at the existing post-DDL completion point.
    ObAlterTableCatalogSyncOperation *operation = OB_NEWx(ObAlterTableCatalogSyncOperation,
                                                          (&allocator_),
                                                          tenant_id,
                                                          is_unified_catalog,
                                                          precheck_result.get_source_object_id(),
                                                          access_token,
                                                          session,
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
