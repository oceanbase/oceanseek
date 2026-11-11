/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_SCAN_CATALOG_H_
#define OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_SCAN_CATALOG_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_result_scan.h"
#include "sql/optimizer/file_prune/ob_opt_hive_define_fwd.h"

struct _ParseNode;

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObILakeTableMetadata;
namespace schema
{
class ObTableSchema;
}
}
namespace sql
{
class ObSQLSessionInfo;
class ObSqlSchemaGuard;
class ObILakeTableFilePruner;

struct ObResultScanTableDesc
{
  ObResultScanTableDesc()
    : table_id_(common::OB_INVALID_ID),
      catalog_table_metadata_(nullptr),
      table_schema_(nullptr) {}

  uint64_t table_id_;
  const share::ObILakeTableMetadata *catalog_table_metadata_;
  // The schema is owned by the caller's statement allocator.  It is the
  // logical published result schema, never a persistent Result Catalog table.
  const share::schema::ObTableSchema *table_schema_;
};

// Resolves the logical async-query result identified by its public query_id.
// The catalog is also the result-read authorization boundary: it verifies that
// the current tenant and privilege user are the original async-query submitter
// before it exposes the physical result schema to the resolver.
class ObResultScanCatalog
{
public:
  static int get_result_table(const common::ObString &query_id,
                              const ObSQLSessionInfo *session,
                              ObSqlSchemaGuard *sql_schema_guard,
                              common::ObIAllocator &alloc,
                              ObResultScanTableDesc &table_desc,
                              common::ObIArray<ObResultScanColDesc> &col_descs,
                              const share::ObILakeTableMetadata *cached_metadata = nullptr,
                              int64_t requested_chunk_no = common::OB_INVALID_INDEX_INT64);

  // Result Catalog owns its statement-local manifest policy. The generic lake
  // planner receives only the ordinary Hive-file task descriptors and pruner
  // it already knows how to dispatch. A requested chunk yields exactly one
  // descriptor bounded to exactly one Parquet row group.
  static int prepare_lake_scan(const share::ObILakeTableMetadata *catalog_metadata,
                               common::ObIAllocator &allocator,
                               uint64_t table_id,
                               uint64_t ref_table_id,
                               common::ObIArray<ObHiveFileDesc> &file_descs,
                               ObILakeTableFilePruner *&file_pruner);

  // Return the cardinality visible to the optimizer. An all-result scan uses
  // the manifest total; a chunk scan uses its published row-group row count.
  static int get_result_row_count(const share::ObILakeTableMetadata *catalog_metadata,
                                  int64_t &row_count);

  // Runs as the system-owned daily scheduler job.  It leaves scheduler rows
  // in place so RESULT_SCAN can still enforce owner checks and report expiry.
  static int cleanup_expired_results(uint64_t tenant_id,
                                     common::ObMySQLProxy &sql_proxy);

  // Resolve TABLE(RESULT_SCAN(...)) for DESC/DESCRIBE/SHOW COLUMNS and build
  // the ordinary SHOW-column result SQL.  It deliberately enters through
  // get_result_table(), so this path shares the read-side owner, terminal
  // state, expiry, result-store, and metadata.json checks.
  static int build_show_columns_sql(const ::_ParseNode *table_collection_expr,
                                    const ObSQLSessionInfo *session,
                                    common::ObIAllocator &allocator,
                                    bool is_full,
                                    common::ObString &show_sql);

  // Resolve TABLE(RESULT_SCAN(...)) for SHOW CREATE TABLE. Native results use
  // the ordinary native SHOW CREATE rendering. External results return a
  // logical external definition with immutable result-manifest properties.
  static int build_show_create_table_sql(const ::_ParseNode *table_collection_expr,
                                         const ObSQLSessionInfo *session,
                                         common::ObIAllocator &allocator,
                                         common::ObString &show_sql);

private:
  DISALLOW_COPY_AND_ASSIGN(ObResultScanCatalog);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESULT_CATALOG_OB_RESULT_SCAN_CATALOG_H_
