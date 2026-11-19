/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync_fork_table_operation.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "share/catalog/ob_unified_catalog.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

ObForkTableCatalogSyncOperation::ObForkTableCatalogSyncOperation(
    const ObString &access_token,
    const obrpc::ObForkTableArg &fork_table_arg)
    : fork_table_arg_(fork_table_arg), tenant_id_(fork_table_arg.tenant_id_),
      allocator_("CatalogSync", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_),
      target_(tenant_id_, access_token)
{
}

ObForkTableCatalogSyncOperation::~ObForkTableCatalogSyncOperation()
{
}

int ObForkTableCatalogSyncOperation::prepare(
    ObCatalogSyncPrepareResult &result)
{
  int ret = OB_SUCCESS;
  result.skip_local_ddl_ = false;
  result.source_object_id_ = OB_INVALID_ID;
  if (OB_FAIL(prepare_unified_catalog_target(target_, allocator_))) {
    LOG_WARN("failed to prepare Unified Catalog target before fork table sync",
             KR(ret), K(tenant_id_), K(fork_table_arg_));
  }
  return ret;
}

int ObForkTableCatalogSyncOperation::complete(
    const ObCatalogSyncDDLResult &result)
{
  int ret = OB_SUCCESS;
  if (CATALOG_SYNC_DDL_SUCCEEDED == result.state_
      && OB_SUCC(result.ddl_ret_)) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(ObCatalogSyncHelper::get_refreshed_schema_guard(
            tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard after fork table",
               KR(ret), K(tenant_id_), K(fork_table_arg_.dst_database_name_),
               K(fork_table_arg_.dst_table_name_), K(fork_table_arg_));
    } else if (OB_FAIL(schema_guard.get_table_schema(
                   tenant_id_, fork_table_arg_.dst_database_name_,
                   fork_table_arg_.dst_table_name_, false /*is_index*/, table_schema))) {
      LOG_WARN("failed to get forked table schema for Unified Catalog sync",
               KR(ret), K(tenant_id_), K(fork_table_arg_.dst_database_name_),
               K(fork_table_arg_.dst_table_name_), K(fork_table_arg_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("forked table schema is null after successful fork table",
               KR(ret), K(tenant_id_), K(fork_table_arg_.dst_database_name_),
               K(fork_table_arg_.dst_table_name_), K(fork_table_arg_));
    } else if (OB_FAIL(ObUnifiedCatalog::sync_table_to_target(
                   target_, fork_table_arg_.dst_database_name_, *table_schema))) {
      LOG_WARN("failed to sync forked table to Unified Catalog",
               KR(ret), K(tenant_id_), K(fork_table_arg_.dst_database_name_),
               K(fork_table_arg_.dst_table_name_), K(fork_table_arg_));
    }
  }
  return ret;
}

int ObCatalogSync::get_fork_table_operator(
    const ObCatalogSyncBackend backend,
    const ObString &access_token,
    const obrpc::ObForkTableArg &fork_table_arg,
    ObCatalogSyncOperationHandle &handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = fork_table_arg.tenant_id_;
  handle.reset();
  if (ObCatalogSyncBackend::UNIFIED_CATALOG == backend) {
    ObForkTableCatalogSyncOperation *operation
        = OB_NEWx(ObForkTableCatalogSyncOperation, (&allocator_), access_token, fork_table_arg);
    if (OB_ISNULL(operation)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate Unified Catalog fork table sync operation",
               KR(ret),
               K(tenant_id),
               K(fork_table_arg));
    } else {
      handle.set(allocator_, operation);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
