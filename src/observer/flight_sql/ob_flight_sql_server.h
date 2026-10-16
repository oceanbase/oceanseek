/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_SQL_SERVER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_SQL_SERVER_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_server_struct.h"

#include <memory>

namespace oceanbase
{
namespace observer
{

struct ObOceanBaseFlightSqlServerImpl;

class ObOceanBaseFlightSqlServer
{
public:
  explicit ObOceanBaseFlightSqlServer(share::ObGlobalContext *gctx);
  ~ObOceanBaseFlightSqlServer();

  int init_listener(const int32_t port);
  int run_serve();
  int shutdown_server();
  int listener_port() const;

private:
  std::shared_ptr<ObOceanBaseFlightSqlServerImpl> impl_;

  DISALLOW_COPY_AND_ASSIGN(ObOceanBaseFlightSqlServer);
};

} // namespace observer
} // namespace oceanbase

#endif
