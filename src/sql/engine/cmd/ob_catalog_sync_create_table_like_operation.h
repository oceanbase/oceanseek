/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_LIKE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_LIKE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"

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

class ObCreateTableLikeCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObCreateTableLikeCatalogSyncOperation(
      ObExecContext &ctx,
      const obrpc::ObCreateTableLikeArg &create_table_like_arg,
      const bool is_unified_catalog);
  virtual ~ObCreateTableLikeCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int prepare_unified_catalog();
  int get_final_table_schema(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&table_schema);
  int complete_unified_catalog();
  int complete_hms();

private:
  // The context and statement argument outlive this execution-scoped Operation.
  ObExecContext &ctx_;
  const obrpc::ObCreateTableLikeArg &create_table_like_arg_;
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  common::ObArenaAllocator allocator_;
  common::ObString access_token_;
  common::ObString remote_catalog_name_;
  common::ObString endpoint_;
  common::ObString realm_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableLikeCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_LIKE_OPERATION_H_
