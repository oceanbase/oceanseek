/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FORK_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FORK_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "share/catalog/ob_unified_catalog.h"

namespace oceanbase
{
namespace sql
{

class ObForkTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObForkTableCatalogSyncOperation(const common::ObString &access_token,
                                  const obrpc::ObForkTableArg &fork_table_arg);
  virtual ~ObForkTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  const obrpc::ObForkTableArg &fork_table_arg_;
  const uint64_t tenant_id_;
  common::ObArenaAllocator allocator_;
  share::ObUnifiedCatalogSyncTarget target_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FORK_TABLE_OPERATION_H_
