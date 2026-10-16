/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_sync_catalog_executor.h"

#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ddl/ob_sync_catalog_stmt.h"

namespace oceanbase
{
using namespace share::schema;
namespace sql
{

int ObSyncCatalogExecutor::execute(ObExecContext &ctx, ObSyncCatalogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  UNUSED(ctx);
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(stmt.get_tenant_id(),
                                                                   schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(stmt.get_tenant_id()));
  } else if (ObSyncCatalogStmt::TABLE_SCOPE == stmt.get_sync_scope()) {
    if (OB_FAIL(schema_guard.get_table_schema(stmt.get_tenant_id(),
                                              stmt.get_table_id(),
                                              table_schema))) {
      LOG_WARN("failed to get table schema",
               K(ret),
               K(stmt.get_tenant_id()),
               K(stmt.get_table_id()));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table does not exist", K(ret), K(stmt.get_table_id()));
    } else if (OB_FAIL(ObCatalogSyncHelper::sync_table(stmt.get_tenant_id(),
                                                       stmt.get_catalog_id(),
                                                       schema_guard,
                                                       stmt.get_database_name(),
                                                       *table_schema))) {
      LOG_WARN("failed to sync table catalog",
               K(ret),
               K(stmt.get_database_name()),
               K(stmt.get_table_name()));
    }
  } else if (ObSyncCatalogStmt::DATABASE_SCOPE == stmt.get_sync_scope()) {
    if (OB_FAIL(ObCatalogSyncHelper::sync_database(stmt.get_tenant_id(),
                                                   stmt.get_catalog_id(),
                                                   schema_guard,
                                                   stmt.get_database_id(),
                                                   stmt.get_database_name()))) {
      LOG_WARN("failed to sync database catalog", K(ret), K(stmt.get_database_name()));
    }
  } else if (ObSyncCatalogStmt::CATALOG_SCOPE == stmt.get_sync_scope()) {
    if (OB_FAIL(ObCatalogSyncHelper::sync_catalog(stmt.get_tenant_id(),
                                                  stmt.get_catalog_id(),
                                                  schema_guard))) {
      LOG_WARN("failed to sync full catalog", K(ret), K(stmt.get_catalog_name()));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog sync scope", K(ret), K(stmt.get_sync_scope()));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
