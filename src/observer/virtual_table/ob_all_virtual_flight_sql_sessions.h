/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_FLIGHT_SQL_SESSIONS_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_FLIGHT_SQL_SESSIONS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/flight_sql/ob_flightsql_session.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualFlightSqlSessions : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualFlightSqlSessions() = default;
  virtual ~ObAllVirtualFlightSqlSessions() = default;

  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  enum COLUMN_ID {
    SVR_IP = OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    SESSION_UUID,
    TENANT_ID,
    USER_NAME,
    SCHEMA_NAME,
    CATALOG_NAME,
    PEER_IP,
    PEER_PORT,
    CONNECTED_AT,
    LAST_ACCESS_US,
    PENDING_SQL_COUNT,
    OB_SESSION_ID,
  };

  std::vector<ObFlightSqlSessionSnapshot> snapshot_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualFlightSqlSessions);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_FLIGHT_SQL_SESSIONS_H_
