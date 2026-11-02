/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_KEY_COLUMN_USAGE_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_KEY_COLUMN_USAGE_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

// Execute the tenant's original view as an internal root session. Keep the
// joins inside that tenant so its metadata indexes and join ranges are usable.
class ObAllVirtualKeyColumnUsage : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualKeyColumnUsage();
  virtual ~ObAllVirtualKeyColumnUsage();
  virtual void reset() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  enum { TENANT_ID = common::OB_APP_MIN_COLUMN_ID, COLUMN_COUNT = 13 };
  int init_scan_();
  int append_string_filter_(const char *column_name);
  int open_tenant_();
  int project_row_(common::ObNewRow *&row);

  bool initialized_;
  bool empty_result_;
  int64_t tenant_idx_;
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  common::ObSqlString filter_sql_;
  common::ObMySQLProxy::MySQLResult result_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKeyColumnUsage);
};

} // namespace observer
} // namespace oceanbase
#endif
