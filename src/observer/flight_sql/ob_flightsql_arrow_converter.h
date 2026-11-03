/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_ARROW_CONVERTER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_ARROW_CONVERTER_H_

#include "observer/flight_sql/ob_flightsql_stream_registry.h"

// Forward declarations
namespace arrow { class Array; class DataType; class Field; }
namespace oceanbase {
namespace common {
class ObCollectionTypeBase;
class ObIAllocator;
class ObObjParam;
class ObSqlString;
template <typename T> class ObIArray;
}
namespace sql { class ObExecContext; class ObExpr; struct ObArrowStreamColDesc; }
}

namespace oceanbase
{
namespace observer
{

struct ObFlightSqlIngestEntry;

// Resolve the type actually materialized by the Arrow converter.  This is an
// Arrow-schema plus compatibility-mode mapping; it deliberately does not use
// the final table column type.  The catalog exposes the descriptor to the
// resolver, and later SQL expressions cast it to the destination column.
int ob_flight_sql_get_output_col_desc(
    const arrow::Field &arrow_field,
    bool is_oracle_mode,
    uint16_t collection_subschema_id,
    sql::ObArrowStreamColDesc &out_desc);

// Build the native collection type string for an Arrow LIST, LARGE_LIST,
// FIXED_SIZE_LIST, MAP, or opt-in struct_as_map field.  The caller registers
// it in the execution context and passes the resulting subschema id to the
// descriptor function above.
int ob_flight_sql_get_middle_collection_type_string(
    const arrow::Field &arrow_field,
    bool is_oracle_mode,
    common::ObSqlString &type_string);

// Build the natural native parameter type for a prepared Arrow collection.
// Unlike ingest, prepared UPDATE assignment expressions may consume the bound
// collection bytes directly.  A fixed-size float list is therefore a VECTOR
// parameter, while an ordinary list remains an ARRAY parameter.
int ob_flight_sql_get_prepared_collection_type_string(
    const arrow::Field &arrow_field,
    bool is_oracle_mode,
    common::ObSqlString &type_string);

// Validate the declared Arrow decimal precision and scale against the
// compatibility-mode middle carrier.  This is shared by ingest and prepared
// parameters so neither path silently rescales a client decimal.
int ob_flight_sql_validate_arrow_decimal_schema(
    const arrow::DataType &arrow_type,
    bool is_oracle_mode,
    int32_t &precision,
    int32_t &scale);

// Materialize an Arrow collection parameter in its natural native parameter
// representation.  The supplied execution context owns the subschema
// registration and allocator owns the serialized payload.  This path is
// intentionally independent from ingest's relational middle-type contract.
int ob_flight_sql_convert_collection_param(
    const arrow::Field &arrow_field,
    const arrow::Array &array,
    int64_t row_idx,
    bool is_oracle_mode,
    sql::ObExecContext &exec_ctx,
    common::ObIAllocator &allocator,
    common::ObObjParam &param,
    sql::ObArrowStreamColDesc &out_desc);

// Build the pre-compiled ConvertBatchFn for one column.
// Called once at DoPrepare / stream-open time; the result is stored in
// ObFlightSqlColumnDesc::convert_batch and invoked per batch in the hot loop.
//
// Returns OB_SUCCESS on success.
// Returns OB_NOT_SUPPORTED if the Arrow/OB type combination is not handled.
int ob_flight_sql_build_convert_fn(const arrow::Field &arrow_field,
                                    const sql::ObArrowStreamColDesc &output_desc,
                                    const common::ObCollectionTypeBase *middle_collection_meta,
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
