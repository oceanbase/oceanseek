/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "sql/engine/expr/ob_expr_uuid.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace sql
{

class ObCreateTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObCreateTableCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const bool if_not_exist,
      const common::ObString &access_token,
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema);
  virtual ~ObCreateTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  int prepare_unified_catalog(ObCatalogSyncPrepareResult &result);
  int prepare_hms();
  int complete_unified_catalog(const ObCatalogSyncDDLResult &result);
  int complete_hms(const ObCatalogSyncDDLResult &result);
  void cancel_unified_catalog_reservation(const int ddl_ret);
  int complete_unified_catalog_create(const ObCatalogSyncDDLResult &result);
  int reconcile_hms_partitions(const ObCatalogSyncDDLResult &result);

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const bool if_not_exist_;
  const bool is_partitioned_table_;
  common::ObString access_token_;
  const common::ObString &input_database_name_;
  const share::schema::ObTableSchema &table_schema_;
  common::ObArenaAllocator allocator_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString remote_catalog_name_;
  common::ObString endpoint_;
  common::ObString realm_;
  common::ObString operation_id_;
  uint64_t database_id_;
  uint64_t sync_catalog_id_;
  bool reserved_;
  char operation_id_buffer_[UuidCommon::LENGTH_UUID];
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_CREATE_TABLE_OPERATION_H_
