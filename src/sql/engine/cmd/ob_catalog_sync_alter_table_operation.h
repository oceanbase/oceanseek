/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "share/catalog/ob_unified_catalog.h"

namespace oceanbase
{
namespace sql
{

class ObAlterTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObAlterTableCatalogSyncOperation(const uint64_t tenant_id,
                                   const bool is_unified_catalog,
                                   const uint64_t source_table_id,
                                   const common::ObString &access_token,
                                   ObSQLSessionInfo *session,
                                   const obrpc::ObAlterTableArg &alter_table_arg);
  virtual ~ObAlterTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int sync_final_schema(const ObCatalogSyncDDLResult &result);

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const obrpc::ObAlterTableArg &alter_table_arg_;
  ObSQLSessionInfo *session_;
  uint64_t source_table_id_;
  common::ObArenaAllocator allocator_;
  share::ObUnifiedCatalogSyncTarget target_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_
