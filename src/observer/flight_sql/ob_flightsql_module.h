/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_MODULE_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_MODULE_H_

#include "observer/flight_sql/ob_flightsql_handler.h"

namespace oceanbase
{
namespace observer
{

// Flight SQL module integration
// This class integrates Flight SQL server into the observer
class ObFlightSQLModule
{
public:
  ObFlightSQLModule();
  ~ObFlightSQLModule();

  int init(ObGlobalContext &gctx);
  int start();
  int stop();
  int wait();

  ObFlightSQLHandler* get_handler() { return &handler_; }

private:
  ObFlightSQLHandler handler_;
  bool inited_;
  bool running_;
  bool disabled_;  // true when flight_sql_port <= 1024; all lifecycle calls are no-ops
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_MODULE_H_