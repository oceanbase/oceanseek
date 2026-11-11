/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

class ObAlterTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObAlterTableCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      ObExecContext &ctx,
      const obrpc::ObAlterTableArg &alter_table_arg);
  virtual ~ObAlterTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  static bool is_supported_uc_partition_alter(
      const obrpc::ObAlterTableArg &arg);
  static int validate_uc_partition_alter_source(
      const uint64_t tenant_id,
      const obrpc::ObAlterTableArg &arg);
  int prepare_unified_catalog();
  int sync_final_schema(const ObCatalogSyncDDLResult &result,
                        const bool is_unified_catalog);

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  ObExecContext &ctx_;
  const obrpc::ObAlterTableArg &alter_table_arg_;
  common::ObArenaAllocator allocator_;
  common::ObString access_token_;
  common::ObString remote_catalog_name_;
  common::ObString endpoint_;
  common::ObString realm_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_ALTER_TABLE_OPERATION_H_
