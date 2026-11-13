/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_ALL_VIRTUAL_TENANT_ERRSIM_EVENT_H_
#define OB_ALL_VIRTUAL_TENANT_ERRSIM_EVENT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#ifdef ERRSIM
#include "common/errsim_module/ob_tenant_errsim_event.h"
#endif

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTenantErrsimEvent : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualTenantErrsimEvent();
  ~ObAllVirtualTenantErrsimEvent() override;
  int inner_get_next_row(common::ObNewRow *&row) override;
  void reset() override;
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }
private:
  enum COLUMN_ID
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    EVENT_ID,
    EVENT_TIMESTAMP,
    MODULE,
    ERROR_CODE,
    TRACE_ID,
    BACKTRACE
  };
  bool is_need_process(uint64_t tenant_id) override;
  int process_curr_tenant(common::ObNewRow *&row) override;
  void release_last_tenant() override;
#ifdef ERRSIM
  int fill_cells_(const common::ObTenantErrsimEvent &event);
#endif
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
#ifdef ERRSIM
  common::ObArray<common::ObTenantErrsimEvent> events_;
#endif
  char trace_id_buf_[128];
  int64_t cursor_;
  bool prepared_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantErrsimEvent);
};
} // namespace observer
} // namespace oceanbase
#endif
