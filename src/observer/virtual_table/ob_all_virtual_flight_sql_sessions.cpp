/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_flight_sql_sessions.h"
#include "share/ob_server_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

void ObAllVirtualFlightSqlSessions::reset()
{
  snapshot_.clear();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualFlightSqlSessions::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_) {
    if (OB_ISNULL(GCTX.flight_sql_session_mgr_)) {
      // Flight SQL server not started; return empty result
      start_to_read_ = true;
      scanner_it_ = scanner_.begin();
      return scanner_it_.get_next_row(cur_row_);
    }

    snapshot_ = GCTX.flight_sql_session_mgr_->snapshot();

    char svr_ip_buf[MAX_IP_ADDR_LENGTH + 1];
    if (!GCTX.self_addr().ip_to_string(svr_ip_buf, sizeof(svr_ip_buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FlightSQL] failed to get self IP", K(ret));
      return ret;
    }
    const int32_t svr_port = GCTX.self_addr().get_port();
    const ObCollationType cs = ObCharset::get_default_collation(ObCharset::get_default_charset());

    for (const auto &snap : snapshot_) {
      const int64_t col_count = output_column_ids_.count();
      int64_t cell_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case SVR_IP:
            cur_row_.cells_[cell_idx].set_varchar(ObString::make_string(svr_ip_buf));
            cur_row_.cells_[cell_idx].set_collation_type(cs);
            break;
          case SVR_PORT:
            cur_row_.cells_[cell_idx].set_int(svr_port);
            break;
          case SESSION_UUID:
            cur_row_.cells_[cell_idx].set_varchar(ObString(snap.session_uuid.size(),
                                                           snap.session_uuid.c_str()));
            cur_row_.cells_[cell_idx].set_collation_type(cs);
            break;
          case TENANT_ID:
            cur_row_.cells_[cell_idx].set_int(static_cast<int64_t>(snap.tenant_id));
            break;
          case USER_NAME:
            cur_row_.cells_[cell_idx].set_varchar(ObString(snap.user_name.size(),
                                                           snap.user_name.c_str()));
            cur_row_.cells_[cell_idx].set_collation_type(cs);
            break;
          case SCHEMA_NAME:
            if (snap.schema_name.empty()) {
              cur_row_.cells_[cell_idx].set_null();
            } else {
              cur_row_.cells_[cell_idx].set_varchar(ObString(snap.schema_name.size(),
                                                             snap.schema_name.c_str()));
              cur_row_.cells_[cell_idx].set_collation_type(cs);
            }
            break;
          case CATALOG_NAME:
            if (snap.catalog_name.empty()) {
              cur_row_.cells_[cell_idx].set_null();
            } else {
              cur_row_.cells_[cell_idx].set_varchar(ObString(snap.catalog_name.size(),
                                                             snap.catalog_name.c_str()));
              cur_row_.cells_[cell_idx].set_collation_type(cs);
            }
            break;
          case PEER_IP:
            if (snap.peer_ip.empty()) {
              cur_row_.cells_[cell_idx].set_null();
            } else {
              cur_row_.cells_[cell_idx].set_varchar(ObString(snap.peer_ip.size(),
                                                             snap.peer_ip.c_str()));
              cur_row_.cells_[cell_idx].set_collation_type(cs);
            }
            break;
          case PEER_PORT:
            if (snap.peer_port == 0) {
              cur_row_.cells_[cell_idx].set_null();
            } else {
              cur_row_.cells_[cell_idx].set_int(snap.peer_port);
            }
            break;
          case CONNECTED_AT:
            cur_row_.cells_[cell_idx].set_int(snap.connected_at_us);
            break;
          case LAST_ACCESS_US:
            cur_row_.cells_[cell_idx].set_int(snap.last_access_us);
            break;
          case PENDING_SQL_COUNT:
            cur_row_.cells_[cell_idx].set_int(snap.pending_sql_count);
            break;
          case OB_SESSION_ID:
            if (snap.ob_session_id == 0) {
              cur_row_.cells_[cell_idx].set_null();
            } else {
              cur_row_.cells_[cell_idx].set_int(static_cast<int64_t>(snap.ob_session_id));
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[FlightSQL] unexpected column id", K(ret), K(col_id));
            break;
        }
        if (OB_SUCC(ret)) { cell_idx++; }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("[FlightSQL] add_row failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("[FlightSQL] get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
