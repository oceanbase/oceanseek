/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_sync_catalog_stmt.h"

namespace oceanbase
{
namespace sql
{

ObSyncCatalogStmt::ObSyncCatalogStmt(common::ObIAllocator *name_pool)
    : ObSystemCmdStmt(name_pool, stmt::T_SYNC_CATALOG),
      sync_scope_(INVALID_SCOPE),
      tenant_id_(OB_INVALID_ID),
      catalog_id_(OB_INVALID_ID),
      catalog_name_(),
      database_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      database_name_(),
      table_name_()
{
}

ObSyncCatalogStmt::ObSyncCatalogStmt()
    : ObSystemCmdStmt(stmt::T_SYNC_CATALOG),
      sync_scope_(INVALID_SCOPE),
      tenant_id_(OB_INVALID_ID),
      catalog_id_(OB_INVALID_ID),
      catalog_name_(),
      database_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      database_name_(),
      table_name_()
{
}

} // namespace sql
} // namespace oceanbase
