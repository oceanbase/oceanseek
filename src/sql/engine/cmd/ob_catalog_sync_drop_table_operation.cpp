/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_drop_table_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/catalog/ob_catalog_meta_writer.h"
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

ERRSIM_POINT_DEF(ERRSIM_CATALOG_SYNC_DROP_TABLE_ALLOC_FAIL);

ObDropTableCatalogSyncOperation::ObDropTableCatalogSyncOperation(
    const uint64_t tenant_id,
    const bool is_unified_catalog,
    const bool is_hms_sync_enabled,
    const ObSQLSessionInfo &session,
    const ObIArray<obrpc::ObTableItem> &table_items)
  : tenant_id_(tenant_id),
    is_unified_catalog_(is_unified_catalog),
    is_hms_sync_enabled_(is_hms_sync_enabled),
    session_(session),
    table_items_(table_items),
    allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    local_table_states_(NULL),
    access_token_(),
    raw_target_(),
    raw_config_(),
    target_snapshot_ret_(OB_SUCCESS),
    sync_catalog_id_(OB_INVALID_ID),
    has_missing_local_table_(false),
    has_syncable_local_table_(false)
{
}

ObDropTableCatalogSyncOperation::~ObDropTableCatalogSyncOperation()
{
  if (OB_NOT_NULL(local_table_states_)) {
    local_table_states_->~ObIArray<LocalTableState>();
    local_table_states_ = NULL;
  }
}

int ObDropTableCatalogSyncOperation::alloc_local_table_states()
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<LocalTableState, 4> LocalTableStateStorage;
  void *buf = NULL;
  if (OB_NOT_NULL(local_table_states_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("drop table local states are already initialized",
             KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(LocalTableStateStorage)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate drop table local states",
             KR(ret), K(tenant_id_));
  } else {
    LocalTableStateStorage *local_table_states = new (buf) LocalTableStateStorage();
    local_table_states->set_attr(ObMemAttr(tenant_id_, "CatalogSync"));
    local_table_states_ = local_table_states;
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::capture_local_table_states()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  has_missing_local_table_ = false;
  has_syncable_local_table_ = false;
  if (OB_FAIL(alloc_local_table_states())) {
    LOG_WARN("failed to initialize drop table local state storage",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObCatalogSyncHelper::get_refreshed_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard before drop table sync",
             KR(ret), K(tenant_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); ++i) {
      const obrpc::ObTableItem &table_item = table_items_.at(i);
      const ObTableSchema *table_schema = NULL;
      LocalTableState local_table_state = LOCAL_TABLE_MISSING;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                table_item.database_name_,
                                                table_item.table_name_,
                                                false /*is_index*/,
                                                table_schema))) {
        LOG_WARN("failed to resolve local table before drop",
                 KR(ret), K(tenant_id_), K(i), K(table_item.database_name_),
                 K(table_item.table_name_));
      } else if (OB_ISNULL(table_schema)) {
        LOG_INFO("local table is absent before drop sync",
                 K(tenant_id_), K(i), K(table_item.database_name_),
                 K(table_item.table_name_));
        has_missing_local_table_ = true;
      } else if (!table_schema->is_user_table()
                 || table_schema->is_index_table()
                 || table_schema->is_aux_table()
                 || OB_INVALID_ID == table_schema->get_database_id()
                 || is_sys_database_id(table_schema->get_database_id())) {
        local_table_state = LOCAL_TABLE_NON_SYNCABLE;
      } else {
        local_table_state = LOCAL_TABLE_SYNCABLE;
        has_syncable_local_table_ = true;
      }
      if (OB_SUCC(ret) && OB_FAIL(local_table_states_->push_back(local_table_state))) {
        LOG_WARN("failed to preserve local table state before drop",
                 KR(ret), K(tenant_id_), K(i), K(local_table_state));
      }
    }
  }
  if (OB_SUCC(ret) && local_table_states_->count() != table_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("drop table local state count mismatch",
             KR(ret), K(tenant_id_), K(table_items_.count()),
             "local_state_count", local_table_states_->count());
  }
  if (OB_SUCCESS != ret && OB_NOT_NULL(local_table_states_)) {
    local_table_states_->reset();
    has_missing_local_table_ = false;
    has_syncable_local_table_ = false;
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::prepare(ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (OB_FAIL(validate_backend("prepare"))) {
    LOG_WARN("drop table catalog sync backend changed before prepare",
             KR(ret), K(tenant_id_), K(is_unified_catalog_), K(is_hms_sync_enabled_));
  } else if (OB_FAIL(capture_local_table_states())) {
    LOG_WARN("failed to capture drop table local states",
             KR(ret), K(tenant_id_), K(is_unified_catalog_), K(is_hms_sync_enabled_));
  } else if (is_unified_catalog_ && (has_missing_local_table_ || has_syncable_local_table_)) {
    access_token_.reset();
    raw_target_.reset();
    raw_config_.reset();
    target_snapshot_ret_ = OB_SUCCESS;
    if (OB_FAIL(ObUnifiedCatalog::snapshot_raw_target(
            tenant_id_, allocator_, raw_target_, raw_config_))) {
      LOG_WARN("failed to capture Unified Catalog drop table target snapshot",
               KR(ret), K(tenant_id_));
      target_snapshot_ret_ = ret;
      ret = OB_SUCCESS;
    } else if (!raw_target_.empty()
               && OB_FAIL(ObUnifiedCatalog::get_session_token(
                   session_, allocator_, access_token_))) {
      access_token_.reset();
      LOG_WARN("failed to get Unified Catalog token for drop table sync",
               KR(ret), K(tenant_id_));
      if (has_missing_local_table_ && !has_syncable_local_table_) {
        target_snapshot_ret_ = ret;
        ret = OB_SUCCESS;
      }
    }
  } else if (is_hms_sync_enabled_ && OB_FAIL(prepare_hms())) {
    LOG_WARN("failed to capture HMS drop table target",
             KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::validate_backend(const char *stage)
{
  int ret = OB_SUCCESS;
  const bool current_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id_);
  const bool current_hms_sync_enabled = !current_unified_catalog
                                        && ObCatalogSyncHelper::is_sync_enabled(tenant_id_);
  if (current_unified_catalog != is_unified_catalog_
      || current_hms_sync_enabled != is_hms_sync_enabled_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("drop table catalog sync backend changed",
             KR(ret), K(tenant_id_), K(stage), K(is_unified_catalog_),
             K(is_hms_sync_enabled_), K(current_unified_catalog),
             K(current_hms_sync_enabled));
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::prepare_hms()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  sync_catalog_id_ = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS drop table prepare",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for HMS drop table prepare",
             KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, sync_catalog_id_))) {
    LOG_WARN("failed to capture HMS drop table target",
             KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_ && OB_SUCC(result.ddl_ret_)) {
    if (OB_FAIL(validate_backend("complete"))) {
      LOG_WARN("drop table catalog sync backend changed before complete",
               KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_));
    } else if (is_unified_catalog_) {
      if (OB_FAIL(complete_unified_catalog(false /*orphan_cleanup*/))) {
        LOG_WARN("failed to complete Unified Catalog drop table sync",
                 KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_));
      }
    } else if (is_hms_sync_enabled_
               && OB_FAIL(complete_hms("complete", false /*orphan_cleanup*/))) {
      LOG_WARN("failed to complete HMS drop table sync",
               KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_));
    }
  } else if (CATALOG_SYNC_DDL_REJECTED == result.state_
             && OB_ERR_BAD_TABLE == result.ddl_ret_) {
    if (OB_FAIL(validate_backend("orphan_cleanup"))) {
      LOG_WARN("drop table catalog sync backend changed before orphan cleanup",
               KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_));
    } else if (is_unified_catalog_) {
      if (OB_FAIL(complete_unified_catalog(true /*orphan_cleanup*/))) {
        LOG_WARN("failed to clean Unified Catalog orphan after missing local table",
                 KR(ret), K(tenant_id_), "drop_ret", result.ddl_ret_);
      }
    } else if (is_hms_sync_enabled_
               && OB_FAIL(complete_hms("orphan_cleanup", true /*orphan_cleanup*/))) {
      LOG_WARN("failed to clean HMS orphan after missing local table",
               KR(ret), K(tenant_id_), "drop_ret", result.ddl_ret_);
    }
  } else if (CATALOG_SYNC_DDL_NOT_STARTED != result.state_
             && CATALOG_SYNC_DDL_NOOP != result.state_
             && CATALOG_SYNC_DDL_REJECTED != result.state_
             && CATALOG_SYNC_DDL_UNKNOWN != result.state_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected drop table catalog sync DDL state",
             KR(ret), K(tenant_id_), K(result.state_), K(result.ddl_ret_),
             K(is_unified_catalog_), K(is_hms_sync_enabled_));
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::complete_unified_catalog(const bool orphan_cleanup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(local_table_states_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unified Catalog drop table local state snapshot is null",
             KR(ret), K(tenant_id_), K(orphan_cleanup));
  } else if (local_table_states_->count() != table_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unified Catalog drop table local state snapshot count mismatch",
             KR(ret), K(tenant_id_), K(table_items_.count()),
             "local_state_count", local_table_states_->count());
  } else if ((has_missing_local_table_ || has_syncable_local_table_)
             && (!orphan_cleanup || has_missing_local_table_)) {
    if (OB_SUCCESS != target_snapshot_ret_) {
      ret = target_snapshot_ret_;
      LOG_WARN("failed to capture Unified Catalog drop table target snapshot",
               KR(ret), K(tenant_id_));
    } else if (!raw_target_.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); ++i) {
        const obrpc::ObTableItem &table_item = table_items_.at(i);
        const LocalTableState local_table_state = local_table_states_->at(i);
        const bool should_drop = orphan_cleanup
                                 ? LOCAL_TABLE_MISSING == local_table_state
                                 : LOCAL_TABLE_NON_SYNCABLE != local_table_state;
        if (should_drop
            && OB_FAIL(ObUnifiedCatalog::drop_table(tenant_id_,
                                                    access_token_,
                                                    table_item.database_name_,
                                                    table_item.table_name_,
                                                    raw_target_,
                                                    raw_config_))) {
          LOG_WARN("failed to delete dropped table from Unified Catalog",
                   KR(ret), K(tenant_id_), K(table_item.database_name_),
                   K(table_item.table_name_), K(local_table_state), K(orphan_cleanup));
        }
      }
    }
  }
  return ret;
}

int ObDropTableCatalogSyncOperation::complete_hms(const char *stage,
                                                  const bool orphan_cleanup)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t current_sync_catalog_id = OB_INVALID_ID;
  if (OB_ISNULL(local_table_states_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("HMS drop table local state snapshot is null",
             KR(ret), K(tenant_id_), K(stage), K(orphan_cleanup));
  } else if (local_table_states_->count() != table_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("HMS drop table local state snapshot count mismatch",
             KR(ret), K(tenant_id_), K(stage), K(orphan_cleanup), K(table_items_.count()),
             "local_state_count", local_table_states_->count());
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for HMS drop table sync",
             KR(ret), K(tenant_id_), K(stage));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard for HMS drop table sync",
             KR(ret), K(tenant_id_), K(stage));
  } else if (OB_FAIL(ObSQLUtils::get_synced_catalog_id(
                 schema_guard, tenant_id_, current_sync_catalog_id))) {
    LOG_WARN("failed to resolve current HMS drop table target",
             KR(ret), K(tenant_id_), K(stage));
  } else if (current_sync_catalog_id != sync_catalog_id_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("HMS drop table target changed",
             KR(ret), K(tenant_id_), K(stage), K(sync_catalog_id_), K(current_sync_catalog_id));
  } else if (OB_INVALID_ID != sync_catalog_id_) {
    ObArenaAllocator allocator("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObCatalogMetaWriter meta_writer(schema_guard, allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items_.count(); ++i) {
      const obrpc::ObTableItem &table_item = table_items_.at(i);
      const LocalTableState local_table_state = local_table_states_->at(i);
      const bool should_drop = orphan_cleanup
                               ? LOCAL_TABLE_MISSING == local_table_state
                               : LOCAL_TABLE_NON_SYNCABLE != local_table_state;
      if (should_drop
          && OB_FAIL(meta_writer.drop_table(tenant_id_,
                                            sync_catalog_id_,
                                            table_item.database_name_,
                                            table_item.table_name_))) {
        if (OB_TABLE_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("HMS table is already absent, treat drop table sync as success",
                   K(tenant_id_), K(sync_catalog_id_),
                   K(table_item.database_name_), K(table_item.table_name_), K(stage));
          ret = OB_SUCCESS;
        } else if (OB_OP_NOT_ALLOW == ret) {
          LOG_INFO("skip dropping HMS table not owned by current tenant",
                   K(tenant_id_), K(sync_catalog_id_),
                   K(table_item.database_name_), K(table_item.table_name_), K(stage));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to delete dropped table from HMS",
                   KR(ret), K(tenant_id_), K(sync_catalog_id_),
                   K(table_item.database_name_), K(table_item.table_name_), K(stage));
        }
      }
    }
  }
  return ret;
}

int ObCatalogSync::get_drop_table_operator(
    const ObSQLSessionInfo &session,
    const obrpc::ObDropTableArg &drop_table_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = drop_table_arg.tenant_id_;
  const bool is_unified_catalog = ObUnifiedCatalog::is_configured(tenant_id);
  const bool is_sync_enabled = ObCatalogSyncHelper::is_sync_enabled(tenant_id);
  const bool is_hms_sync_enabled = !is_unified_catalog && is_sync_enabled;
  const bool need_operation = (is_unified_catalog && is_sync_enabled
                               && USER_TABLE == drop_table_arg.table_type_)
                              || is_hms_sync_enabled;
  handle.reset();
  if (need_operation) {
    ObDropTableCatalogSyncOperation *operation = NULL;
    if (OB_FAIL(ERRSIM_CATALOG_SYNC_DROP_TABLE_ALLOC_FAIL)) {
      LOG_WARN("injected drop table catalog sync operation allocation failure",
               KR(ret), K(tenant_id), K(is_unified_catalog));
      if (!is_unified_catalog) {
        handle.set_deferred_error(ret);
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(operation = OB_NEWx(ObDropTableCatalogSyncOperation,
                                             (&allocator_),
                                             tenant_id,
                                             is_unified_catalog,
                                             is_hms_sync_enabled,
                                             session,
                                             drop_table_arg.tables_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate drop table catalog sync operation",
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
