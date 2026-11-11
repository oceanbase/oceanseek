/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FLASHBACK_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FLASHBACK_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"

namespace oceanbase
{
namespace sql
{

class ObFlashbackTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObFlashbackTableCatalogSyncOperation(
      const uint64_t tenant_id,
      const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg);
  virtual ~ObFlashbackTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int complete_catalog_sync();

private:
  const uint64_t tenant_id_;
  const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg_;
  uint64_t recycle_table_id_;
  uint64_t sync_catalog_id_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashbackTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_FLASHBACK_TABLE_OPERATION_H_
