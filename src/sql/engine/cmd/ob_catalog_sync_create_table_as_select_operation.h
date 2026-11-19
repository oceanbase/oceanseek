/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_AS_SELECT_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_AS_SELECT_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "share/catalog/ob_unified_catalog.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
}
}
namespace sql
{

class ObCreateTableAsSelectCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObCreateTableAsSelectCatalogSyncOperation(const common::ObString &access_token,
                                            ObCreateTableStmt &stmt,
                                            const bool is_unified_catalog);
  virtual ~ObCreateTableAsSelectCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;
  virtual int compensate(const int ddl_ret) override;

private:
  int prepare_unified_catalog();
  int get_final_table_schema(
      const uint64_t table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&table_schema);
  int complete_unified_catalog(const ObCatalogSyncDDLResult &result);
  int complete_hms(const ObCatalogSyncDDLResult &result);
  int compensate_unified_catalog();
  int compensate_hms();

private:
  ObCreateTableStmt &stmt_;
  const bool is_unified_catalog_;
  const uint64_t tenant_id_;
  common::ObArenaAllocator allocator_;
  share::ObUnifiedCatalogSyncTarget target_;
  common::ObString unified_catalog_database_name_;
  common::ObString unified_catalog_table_name_;
  common::ObString raw_config_;
  common::ObString hms_database_name_;
  common::ObString hms_table_name_;
  bool sync_placeholder_created_;
  uint64_t sync_catalog_id_;
  uint64_t unified_catalog_database_id_;
  uint64_t unified_catalog_table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableAsSelectCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_AS_SELECT_OPERATION_H_
