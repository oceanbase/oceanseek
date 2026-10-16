/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_handler.h"

#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "share/config/ob_server_config.h"

#include "observer/flight_sql/ob_flight_sql_server.h"

namespace oceanbase
{
namespace observer
{

void ObFlightSQLServerThread::run1()
{
  (void)srv_->run_serve();
}

ObFlightSQLHandler::ObFlightSQLHandler()
    : gctx_(nullptr),
      inited_(false),
      running_(false),
      stopped_(false),
      flight_server_(nullptr),
      flight_sql_port_(8810)
{
}

ObFlightSQLHandler::~ObFlightSQLHandler()
{
  stop();
  wait();
}

int ObFlightSQLHandler::init(ObGlobalContext *gctx)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFlightSQLHandler already inited", K(ret));
  } else if (OB_ISNULL(gctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null gctx", K(ret));
  } else {
    gctx_ = gctx;
    flight_sql_port_ = static_cast<int>(GCONF.flight_sql_port);

    if (OB_FAIL(init_flight_server_())) {
      LOG_ERROR("init flight server failed", K(ret));
    } else {
      inited_ = true;
      LOG_TRACE("ObFlightSQLHandler init success", K(flight_sql_port_));
    }
  }

  return ret;
}

int ObFlightSQLHandler::init_flight_server_()
{
  int ret = OB_SUCCESS;

  flight_server_ = std::make_shared<ObOceanBaseFlightSqlServer>(gctx_);
  if (OB_FAIL(flight_server_->init_listener(static_cast<int32_t>(flight_sql_port_)))) {
    LOG_ERROR("Flight server init_listener failed", K(ret));
  } else {
    LOG_TRACE("Flight SQL gRPC server initialized",
             K(flight_sql_port_),
             "listen_port",
             flight_server_->listener_port());
  }

  return ret;
}

int ObFlightSQLHandler::start()
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFlightSQLHandler not inited", K(ret));
  } else if (running_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ObFlightSQLHandler already running", K(ret));
  } else {
    if (OB_FAIL(start_flight_server_())) {
      LOG_ERROR("start flight server failed", K(ret));
    } else {
      running_ = true;
      LOG_TRACE("ObFlightSQLHandler started", K(flight_sql_port_));
    }
  }

  return ret;
}

int ObFlightSQLHandler::start_flight_server_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(flight_server_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flight server is null", K(ret));
  } else {
    server_thread_.set_server(flight_server_);
    if (OB_FAIL(server_thread_.start())) {
      LOG_ERROR("Flight SQL server thread start failed", K(ret));
    } else {
      LOG_TRACE("Flight SQL Serve thread started", K(flight_sql_port_));
    }
  }

  return ret;
}

int ObFlightSQLHandler::stop()
{
  int ret = OB_SUCCESS;

  if (!stopped_) {
    stopped_ = true;
    if (running_) {
      stop_flight_server_();
      running_ = false;
    }
    LOG_TRACE("ObFlightSQLHandler stopped");
  }

  return ret;
}

int ObFlightSQLHandler::stop_flight_server_()
{
  int ret = OB_SUCCESS;

  if (flight_server_ != nullptr) {
    ret = flight_server_->shutdown_server();
  }

  return ret;
}

int ObFlightSQLHandler::wait()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    server_thread_.wait();
    LOG_TRACE("ObFlightSQLHandler wait complete");
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase