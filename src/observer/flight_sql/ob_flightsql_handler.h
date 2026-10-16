/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_HANDLER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_HANDLER_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/thread/threads.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/frame/ob_req_deliver.h"
#include "observer/ob_server_struct.h"

#include <memory>
#include <string>
#include <mutex>

namespace oceanbase
{
namespace observer
{

class ObOceanBaseFlightSqlServer;

class ObFlightSQLServerThread : public lib::Threads
{
public:
  void set_server(std::shared_ptr<ObOceanBaseFlightSqlServer> srv) { srv_ = srv; }
  void run1() override;
private:
  std::shared_ptr<ObOceanBaseFlightSqlServer> srv_;
};

class ObFlightSQLHandler
{
public:
  ObFlightSQLHandler();
  ~ObFlightSQLHandler();

  int init(ObGlobalContext *gctx);
  int start();
  int stop();
  int wait();

private:
  int init_flight_server_();
  int start_flight_server_();
  int stop_flight_server_();

private:
  ObGlobalContext *gctx_;

  bool inited_;
  bool running_;
  bool stopped_;

  std::shared_ptr<ObOceanBaseFlightSqlServer> flight_server_;
  ObFlightSQLServerThread server_thread_;

  int flight_sql_port_;

  DISALLOW_COPY_AND_ASSIGN(ObFlightSQLHandler);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_HANDLER_H_
