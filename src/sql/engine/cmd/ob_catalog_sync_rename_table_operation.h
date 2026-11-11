/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_RENAME_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_RENAME_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"

namespace oceanbase
{
namespace sql
{

class ObRenameTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  static int create(
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const obrpc::ObRenameTableArg &rename_table_arg,
      ObRenameTableCatalogSyncOperation *&operation);
  virtual ~ObRenameTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  ObRenameTableCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const obrpc::ObRenameTableArg &rename_table_arg);
  int init();
  int validate_unified_catalog_source();
  int prepare_hms();
  int rollback_hms();

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const obrpc::ObRenameTableArg &rename_table_arg_;
  uint64_t sync_catalog_id_;
  bool remote_renamed_;
  DISALLOW_COPY_AND_ASSIGN(ObRenameTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_RENAME_TABLE_OPERATION_H_
