/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_RETRY_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_RETRY_H_

#include "lib/profile/ob_trace_id.h"
#include "observer/mysql/ob_query_retry_ctrl.h"

namespace oceanbase
{
namespace share { namespace schema { class ObSchemaGetterGuard; } }
namespace observer
{

struct ObFlightSqlSession;

// One logical SELECT owns its retry state until the query lock is released.
// The same deadline and trace are used for every local attempt.
class ObFlightSqlQueryRetry
{
public:
  ObFlightSqlQueryRetry() = default;
  int init(ObFlightSqlSession &session, const common::ObString &sql);
  void begin_attempt(ObFlightSqlSession &session);
  int set_schema_versions(ObFlightSqlSession &session,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          sql::ObSqlCtx &sql_ctx);
  void check_retry(sql::ObSqlCtx &sql_ctx, sql::ObResultSet &result, int &client_ret);
  void update_session(ObFlightSqlSession &session);
  void finish(ObFlightSqlSession &session);

  bool need_retry() const { return RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type(); }
  int64_t timeout_ts() const { return timeout_ts_; }
  void set_plan_timeout(int64_t timeout_ts) { if (timeout_ts > 0) { timeout_ts_ = timeout_ts; } }
  const common::ObCurTraceId::TraceId &trace_id() const { return trace_id_; }

private:
  ObQueryRetryCtrl retry_ctrl_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_{0};
  bool initialized_{false};
  DISALLOW_COPY_AND_ASSIGN(ObFlightSqlQueryRetry);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_QUERY_RETRY_H_
