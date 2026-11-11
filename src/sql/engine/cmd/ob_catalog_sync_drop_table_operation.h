/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DROP_TABLE_OPERATION_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DROP_TABLE_OPERATION_H_

#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obrpc
{
struct ObTableItem;
}

namespace sql
{

class ObDropTableCatalogSyncOperation : public ObCatalogSyncOperation
{
public:
  ObDropTableCatalogSyncOperation(
      const uint64_t tenant_id,
      const bool is_unified_catalog,
      const bool is_hms_sync_enabled,
      const ObSQLSessionInfo &session,
      const common::ObIArray<obrpc::ObTableItem> &table_items);
  virtual ~ObDropTableCatalogSyncOperation();
  virtual int prepare(ObCatalogSyncPrepareResult &result) override;
  virtual int complete(const ObCatalogSyncDDLResult &result) override;

private:
  enum LocalTableState
  {
    LOCAL_TABLE_MISSING = 0,
    LOCAL_TABLE_SYNCABLE,
    LOCAL_TABLE_NON_SYNCABLE
  };
  typedef common::ObIArray<LocalTableState> LocalTableStateArray;

  int alloc_local_table_states();
  int capture_local_table_states();
  int validate_backend(const char *stage);
  int prepare_hms();
  int complete_unified_catalog(const bool orphan_cleanup);
  int complete_hms(const char *stage, const bool orphan_cleanup);

private:
  const uint64_t tenant_id_;
  const bool is_unified_catalog_;
  const bool is_hms_sync_enabled_;
  const ObSQLSessionInfo &session_;
  const common::ObIArray<obrpc::ObTableItem> &table_items_;
  common::ObArenaAllocator allocator_;
  LocalTableStateArray *local_table_states_;
  common::ObString access_token_;
  common::ObString raw_target_;
  common::ObString raw_config_;
  int target_snapshot_ret_;
  uint64_t sync_catalog_id_;
  bool has_missing_local_table_;
  bool has_syncable_local_table_;
  DISALLOW_COPY_AND_ASSIGN(ObDropTableCatalogSyncOperation);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_DROP_TABLE_OPERATION_H_
