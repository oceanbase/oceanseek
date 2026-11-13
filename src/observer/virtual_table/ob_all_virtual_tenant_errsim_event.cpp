/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "observer/virtual_table/ob_all_virtual_tenant_errsim_event.h"
#include "lib/worker.h"
#ifdef ERRSIM
#include "share/errsim_module/ob_tenant_errsim_event_mgr.h"
#include "share/rc/ob_tenant_base.h"
#endif

namespace oceanbase
{
namespace observer
{
using namespace common;

ObAllVirtualTenantErrsimEvent::ObAllVirtualTenantErrsimEvent()
    : addr_(), ip_buf_{}, trace_id_buf_{}, cursor_(0), prepared_(false)
{
}

ObAllVirtualTenantErrsimEvent::~ObAllVirtualTenantErrsimEvent()
{
  reset();
}

void ObAllVirtualTenantErrsimEvent::reset()
{
  omt::ObMultiTenantOperator::reset();
  release_last_tenant();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTenantErrsimEvent::release_last_tenant()
{
#ifdef ERRSIM
  events_.reuse();
#endif
  cursor_ = 0;
  prepared_ = false;
}

bool ObAllVirtualTenantErrsimEvent::is_need_process(uint64_t tenant_id)
{
  return !is_virtual_tenant_id(tenant_id)
      && (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_);
}

int ObAllVirtualTenantErrsimEvent::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
#ifdef ERRSIM
  lib::ErrsimModuleGuard guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  ret = execute(row);
#else
  ret = OB_ITER_END;
#endif
  return ret;
}

int ObAllVirtualTenantErrsimEvent::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
#ifdef ERRSIM
  lib::ErrsimModuleGuard guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  if (OB_ISNULL(allocator_) || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
  } else if (!prepared_) {
    share::ObTenantErrsimEventMgr *mgr = MTL(share::ObTenantErrsimEventMgr *);
    events_.reuse();
    if (OB_ISNULL(mgr)) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(mgr->get_events(events_))) {
      SERVER_LOG(WARN, "failed to snapshot errsim events", K(ret), K(MTL_ID()));
    } else {
      prepared_ = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (cursor_ >= events_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fill_cells_(events_.at(cursor_)))) {
    SERVER_LOG(WARN, "failed to fill errsim event cells", K(ret), K(cursor_));
  } else {
    ++cursor_;
    row = &cur_row_;
  }
#else
  ret = OB_ITER_END;
#endif
  return ret;
}

#ifdef ERRSIM
int ObAllVirtualTenantErrsimEvent::fill_cells_(const ObTenantErrsimEvent &event)
{
  int ret = OB_SUCCESS;
  if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "failed to format server address", K(ret), K(addr_));
  } else {
    event.trace_id_.to_string(trace_id_buf_, sizeof(trace_id_buf_));
    const int64_t column_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      ObObj &cell = cur_row_.cells_[i];
      switch (column_id) {
        case SVR_IP: {
          cell.set_varchar(ip_buf_);
          break;
        }
        case SVR_PORT: {
          cell.set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          cell.set_uint64(MTL_ID());
          break;
        }
        case EVENT_ID: {
          cell.set_uint64(event.event_id_);
          break;
        }
        case EVENT_TIMESTAMP: {
          cell.set_timestamp(event.timestamp_);
          break;
        }
        case MODULE: {
          cell.set_varchar(ObErrsimModuleTypeHelper::get_str(event.type_.type_));
          break;
        }
        case ERROR_CODE: {
          cell.set_int(event.errsim_error_);
          break;
        }
        case TRACE_ID: {
          cell.set_varchar(trace_id_buf_);
          break;
        }
        case BACKTRACE: {
          cell.set_varchar(event.backtrace_.ptr());
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid errsim event column id", K(ret), K(column_id));
          break;
        }
      }
      if (OB_SUCC(ret) && cell.is_varchar()) {
        cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
    }
  }
  return ret;
}
#endif
} // namespace observer
} // namespace oceanbase
