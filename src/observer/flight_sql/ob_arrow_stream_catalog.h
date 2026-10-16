/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_ARROW_STREAM_CATALOG_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_ARROW_STREAM_CATALOG_H_

#include <string>
#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "sql/engine/expr/ob_expr_arrow_stream.h"

namespace oceanbase
{
namespace sql {
class ObOptimizerContext;
class ObShardingInfo;
class ObSQLSessionInfo;
}
namespace observer
{

// -----------------------------------------------------------------------
// ObArrowStreamCatalog
//
// Single metadata interface for arrow_stream() function tables.
//
// The resolver calls get_columns() to discover column names and types.
// The optimizer calls get_sharding() to decide execution locality.
//
// V1: single local stream per session.
// V2/V3: extend get_sharding() to return distributed sharding with
//         per-stream server locations once multi-stream is supported.
// -----------------------------------------------------------------------
class ObArrowStreamCatalog
{
public:
  // Get column descriptors for stream_id from the session's stream registry.
  // Called by the resolver to build ColumnItem list for the function table.
  // The catalog extracts the registry from the session internally.
  static int get_columns(uint64_t stream_id,
                         const sql::ObSQLSessionInfo *session,
                         common::ObIAllocator &alloc,
                         common::ObIArray<sql::ObArrowStreamColDesc> &col_descs);

  // V2: get column descriptors from the server-level IngestRegistry by ingest_id.
  // Also fills column_mapping for all registered slots so the VecOp can convert.
  static int get_columns_for_ingest(const std::string &ingest_id,
                                    const sql::ObSQLSessionInfo *session,
                                    common::ObIAllocator &alloc,
                                    common::ObIArray<sql::ObArrowStreamColDesc> &col_descs);

  // Get the sharding for an arrow_stream/arrow_streams function table scan path.
  // V1 (arrow_stream): returns opt_ctx.get_local_sharding().
  // arrow_streams returns local sharding only when its sole listed observer is
  // this observer; any remote source location requires distributed sharding.
  static sql::ObShardingInfo *get_sharding(sql::ObOptimizerContext &opt_ctx,
                                           const sql::ObRawExpr *func_expr = nullptr);

  // Get observer addresses for arrow_streams ingest, including one remote observer.
  // Looks up IngestRegistry by ingest_id extracted from the func_expr arg.
  // Returns an empty array (OB_SUCCESS) only when no locations are known.
  static int get_locations_for_arrow_streams(const sql::ObRawExpr *func_expr,
                                             common::ObIArray<common::ObAddr> &servers);

private:
  DISALLOW_COPY_AND_ASSIGN(ObArrowStreamCatalog);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_ARROW_STREAM_CATALOG_H_
