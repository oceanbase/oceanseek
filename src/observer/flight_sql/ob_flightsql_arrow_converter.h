/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_ARROW_CONVERTER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_ARROW_CONVERTER_H_

#include "observer/flight_sql/ob_flightsql_stream_registry.h"

// Forward declarations
namespace arrow { class Field; }
namespace oceanbase {
namespace common { class ObCollectionTypeBase; template <typename T> class ObIArray; }
namespace sql { class ObExecContext; class ObExpr; struct ObArrowStreamColDesc; }
namespace share { namespace schema { class ObColumnSchemaV2; } }
}

namespace oceanbase
{
namespace observer
{

struct ObFlightSqlIngestEntry;

// Resolve the type actually materialized by the Arrow converter for one
// target column.  The catalog must expose this descriptor to the resolver:
// later SQL expressions are responsible for casting it to the table column
// type when the two differ.
int ob_flight_sql_get_output_col_desc(
    const arrow::Field &arrow_field,
    const share::schema::ObColumnSchemaV2 &ob_col,
    uint16_t collection_subschema_id,
    sql::ObArrowStreamColDesc &out_desc);

// Build the pre-compiled ConvertBatchFn for one column.
// Called once at DoPrepare / stream-open time; the result is stored in
// ObFlightSqlColumnDesc::convert_batch and invoked per batch in the hot loop.
//
// Returns OB_SUCCESS on success.
// Returns OB_NOT_SUPPORTED if the Arrow/OB type combination is not handled.
int ob_flight_sql_build_convert_fn(const arrow::Field &arrow_field,
                                    const share::schema::ObColumnSchemaV2 &ob_col,
                                    const common::ObCollectionTypeBase *collection_meta,
                                    ObFlightSqlColumnDesc &out_desc);

// V3 execution builds the local stream mapping from the converter-output
// metadata serialized in the physical plan.  The expression includes the
// collection subschema id, which is resolved through exec_ctx so ARRAY and
// VECTOR retain their element type and dimension when selecting the encoder.
int ob_flight_sql_build_convert_fn(const arrow::Field &arrow_field,
                                    const sql::ObExpr &output_expr,
                                    const sql::ObExecContext &exec_ctx,
                                    ObFlightSqlColumnDesc &out_desc);

// Initialize the V3 local stream mapping once.  The synchronization belongs
// to the Flight SQL registry so SQL operators do not depend on STL locks.
int ob_flight_sql_init_streams_conversion_mapping(
    ObFlightSqlIngestEntry &entry,
    const common::ObIArray<sql::ObExpr *> &column_exprs,
    const sql::ObExecContext &exec_ctx);

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_ARROW_CONVERTER_H_
