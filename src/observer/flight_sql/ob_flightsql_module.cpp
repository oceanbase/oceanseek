/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_module.h"

#include "lib/utility/ob_macro_utils.h"
#include "observer/ob_server.h"
#include "rpc/frame/ob_req_deliver.h"

namespace oceanbase
{
namespace observer
{

ObFlightSQLModule::ObFlightSQLModule()
    : inited_(false),
      running_(false),
      disabled_(false)
{
  LOG_INFO("[FlightSQL] ObFlightSQLModule created");
}

ObFlightSQLModule::~ObFlightSQLModule()
{
  stop();
  wait();
}

int ObFlightSQLModule::init(ObGlobalContext &gctx)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFlightSQLModule already inited", K(ret));
  } else if (static_cast<int64_t>(GCONF.flight_sql_port) >= 0 && static_cast<int64_t>(GCONF.flight_sql_port) <= 1024) {
    disabled_ = true;
    LOG_INFO("[FlightSQL] flight_sql_port <= 1024, Flight SQL service disabled",
             "flight_sql_port", static_cast<int64_t>(GCONF.flight_sql_port));
  } else {
    if (OB_FAIL(handler_.init(&gctx))) {
      LOG_ERROR("init flight sql handler failed", K(ret));
    } else {
      inited_ = true;
      LOG_INFO("ObFlightSQLModule init success");
    }
  }

  return ret;
}

int ObFlightSQLModule::start()
{
  int ret = OB_SUCCESS;

  if (disabled_) {
    // flight_sql_port <= 1024; service intentionally disabled
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFlightSQLModule not inited", K(ret));
  } else if (running_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ObFlightSQLModule already running", K(ret));
  } else {
    if (OB_FAIL(handler_.start())) {
      LOG_ERROR("start flight sql handler failed", K(ret));
    } else {
      running_ = true;
      LOG_INFO("ObFlightSQLModule started");
    }
  }

  return ret;
}

int ObFlightSQLModule::stop()
{
  int ret = OB_SUCCESS;

  if (running_) {
    handler_.stop();
    running_ = false;
    LOG_INFO("ObFlightSQLModule stopped");
  }

  return ret;
}

int ObFlightSQLModule::wait()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    handler_.wait();
    LOG_INFO("ObFlightSQLModule wait complete");
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase