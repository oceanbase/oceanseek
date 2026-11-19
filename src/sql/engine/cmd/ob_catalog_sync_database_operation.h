/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DATABASE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DATABASE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "share/catalog/ob_unified_catalog.h"

namespace oceanbase
{
namespace sql
{

class ObCreateDatabaseCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObCreateDatabaseCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const common::ObString &access_token,
      const common::ObString &database_name);
  virtual ~ObCreateDatabaseCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int prepare_hms();
  int complete_unified_catalog(const ObCatalogSyncDDLResult &result);

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const common::ObString &database_name_;
  common::ObArenaAllocator allocator_;
  share::ObUnifiedCatalogSyncTarget target_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateDatabaseCatalogSyncOperation);
};

class ObDropDatabaseCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObDropDatabaseCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const bool is_hms_sync_enabled,
      const common::ObString &access_token,
      const common::ObString &database_name);
  virtual ~ObDropDatabaseCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int prepare_hms();
  int prepare_unified_catalog();
  int complete_hms();
  int complete_unified_catalog();

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const bool is_hms_sync_enabled_;
  const common::ObString &database_name_;
  uint64_t database_id_;
  uint64_t sync_catalog_id_;
  common::ObArenaAllocator allocator_;
  share::ObUnifiedCatalogSyncTarget target_;
  DISALLOW_COPY_AND_ASSIGN(ObDropDatabaseCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DATABASE_OPERATION_H_
