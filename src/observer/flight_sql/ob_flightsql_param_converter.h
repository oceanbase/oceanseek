/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_PARAM_CONVERTER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_PARAM_CONVERTER_H_

#include <cstdint>

#include "lib/allocator/ob_allocator.h"
#include "sql/ob_sql_define.h"

namespace arrow { class RecordBatch; class Schema; }
namespace oceanbase { namespace sql { class ObExecContext; } }

namespace oceanbase
{
namespace observer
{

struct ObFlightSqlSession;

// Validates and converts positional Arrow parameter rows into native prepared
// parameter values.  Conversion runs inside the native execution context so
// collection subschemas belong to the ParamStore consumed by
// ObSql::stmt_execute().  Every variable-width payload is deep-copied into
// allocator; no ParamStore value refers to RecordBatch memory.
class ObFlightSqlParamConverter
{
public:
  static int validate_schema(const arrow::Schema &schema,
                             const ObFlightSqlSession &session);

  static int convert_row(const arrow::RecordBatch &batch,
                         int64_t row_idx,
                         ObFlightSqlSession &session,
                         sql::ObExecContext &exec_ctx,
                         common::ObIAllocator &allocator,
                         sql::ParamStore &params,
                         int64_t &failed_column);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_PARAM_CONVERTER_H_
