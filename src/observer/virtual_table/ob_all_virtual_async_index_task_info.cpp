/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "observer/virtual_table/ob_all_virtual_async_index_task_info.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

// Default-construct an empty iterator. Address and snapshot are zeroed; the
// real address is stamped later via set_addr() before iteration begins, and
// snapshot_ is filled on first process_curr_tenant() call per tenant.
ObAllVirtualAsyncIndexTaskInfo::ObAllVirtualAsyncIndexTaskInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      snapshot_(),
      cursor_(0),
      prepared_(false)
{
  ip_buf_[0] = '\0';
}

// Destructor: defers to reset() so all per-tenant and per-iterator state is
// torn down through a single code path.
ObAllVirtualAsyncIndexTaskInfo::~ObAllVirtualAsyncIndexTaskInfo()
{
  reset();
}

// Release all iterator state: the multi-tenant operator's per-tenant context,
// the cached server address, the current tenant's task snapshot, and the
// scanner base class state. Safe to call multiple times.
void ObAllVirtualAsyncIndexTaskInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  snapshot_.reset();
  cursor_ = 0;
  prepared_ = false;
  ObVirtualTableScannerIterator::reset();
}

// Multi-tenant filter hook invoked by ObMultiTenantOperator. Sys tenant sees
// every tenant's rows; any other querying tenant only sees its own — matching
// the tenant-isolation policy used by sibling virtual tables.
bool ObAllVirtualAsyncIndexTaskInfo::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

// Drop the previous tenant's snapshot before switching to the next tenant.
// Called by ObMultiTenantOperator between tenants so each tenant re-runs
// prepare_to_read_() against its own MTL(ObAsyncIndexService*).
void ObAllVirtualAsyncIndexTaskInfo::release_last_tenant()
{
  snapshot_.reset();
  cursor_ = 0;
  prepared_ = false;
}


// current MTL tenant context. Sources three segments in order:
//   1. finished_ring_ (recent completed tasks, via get_recent_finished_stats)
//   2. in-flight tasks currently executing in the worker thread pool
//   3. retry-pending tasks parked in each leader LS scheduler's retry list
int ObAllVirtualAsyncIndexTaskInfo::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  snapshot_.reset();
  cursor_ = 0;
  ObAsyncIndexService *svc = MTL(ObAsyncIndexService *);
  if (OB_ISNULL(svc)) {
    // Async index service is unconditionally MTL_BIND'ed for SYS / META / USER tenants
    // (see ob_multi_tenant.cpp), and virtual tenants are filtered out earlier by
    // is_need_process() + ObMultiTenantOperator's tenant iteration. The only case
    // where MTL(...) legitimately returns null here is the brief window in which
    // a tenant is being stopped/destroyed concurrently with this scan. Treat that
    // as "no rows for this tenant" instead of failing the whole cross-tenant
    // query; log at INFO so it is visible if it ever becomes frequent.
    SERVER_LOG(INFO, "async index service not available in this tenant", K(MTL_ID()));
  } else {
    ObTenantAsyncIndexRefresher &refresher = svc->get_refresher();
    ObAsyncIndexRefreshWorker &worker = refresher.get_refresh_worker();
    ObAsyncIndexRefreshViewSnapshot view_snapshot;
    if (OB_FAIL(worker.get_recent_finished_stats(snapshot_))) {
      SERVER_LOG(WARN, "fail to get recent finished stats", K(ret));
    } else if (OB_FAIL(refresher.build_view_snapshot(view_snapshot))) {
      SERVER_LOG(WARN, "fail to build view snapshot for task info", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < view_snapshot.in_flight_stats_.count(); ++i) {
        ObRefreshTaskStat stat = view_snapshot.in_flight_stats_.at(i);
        stat.end_time_ = 0;
        stat.result_ = INT_MIN;  // sentinel: task not yet finished
        if (OB_FAIL(snapshot_.push_back(stat))) {
          SERVER_LOG(WARN, "fail to push back in-flight stat", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < view_snapshot.retry_stats_.count(); ++i) {
        ObRefreshTaskStat stat = view_snapshot.retry_stats_.at(i);
        stat.end_time_ = 0;
        if (OB_FAIL(snapshot_.push_back(stat))) {
          SERVER_LOG(WARN, "fail to push back retry stat", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    prepared_ = true;
  }
  return ret;
}

// Virtual table entry point invoked by the SQL executor for each fetched
// row. Delegates straight to ObMultiTenantOperator::execute(), which
// orchestrates tenant switching and ends up calling process_curr_tenant()
// for the current tenant.
int ObAllVirtualAsyncIndexTaskInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

// Produce one output row for the current MTL tenant. On the first call for
// a tenant, lazily fills snapshot_ via prepare_to_read_(); subsequent calls
// emit snapshot_[cursor_++] until the snapshot is drained, at which point
// OB_ITER_END is returned so ObMultiTenantOperator advances to the next
// tenant. Each output column is filled by mapping the requested column id
// (output_column_ids_) to the corresponding field of ObRefreshTaskStat;
// the column order mirrors the inner table schema generated from
// ob_inner_table_schema_def.py.
int ObAllVirtualAsyncIndexTaskInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(ret));
  } else if (!prepared_ && OB_FAIL(prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare_to_read_ failed", K(ret));
  } else if (cursor_ >= snapshot_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObRefreshTaskStat &s = snapshot_.at(cursor_);
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID + 0: {
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(static_cast<int64_t>(MTL_ID()));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // ls_id
          cur_row_.cells_[i].set_int(s.ls_id_.id());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // tablet_id
          cur_row_.cells_[i].set_int(static_cast<int64_t>(s.tablet_id_.id()));
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // task_id
          cur_row_.cells_[i].set_int(s.refresh_task_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // data_tablet_id
          cur_row_.cells_[i].set_int(static_cast<int64_t>(s.data_tablet_id_.id()));
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // trigger_type (0=PERIOD, 1=CREATE_INDEX, 2=MANUAL)
          cur_row_.cells_[i].set_int(static_cast<int64_t>(s.refresh_type_));
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // queue_time
          cur_row_.cells_[i].set_timestamp(s.queue_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // start_time
          cur_row_.cells_[i].set_timestamp(s.start_time_);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // end_time (nullable: 0 means task not yet finished)
          if (s.end_time_ == 0) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_timestamp(s.end_time_);
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // affected_rows
          cur_row_.cells_[i].set_int(s.affected_rows_);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // start_refresh_snapshot (SCN)
          cur_row_.cells_[i].set_uint64(s.start_refresh_scn_.is_valid()
              ? s.start_refresh_scn_.get_val_for_inner_table_field() : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // target_refresh_snapshot (SCN)
          cur_row_.cells_[i].set_uint64(s.target_refresh_scn_.is_valid()
              ? s.target_refresh_scn_.get_val_for_inner_table_field() : 0);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // ret_code
          cur_row_.cells_[i].set_int(s.result_);
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // refresh_parallelism
          cur_row_.cells_[i].set_int(s.refresh_parallelism_);
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // retry_cnt
          cur_row_.cells_[i].set_int(s.retry_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // skipped_rows
          cur_row_.cells_[i].set_int(s.skipped_rows_);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          // trace_id
          if (s.trace_id_len_ > 0) {
            cur_row_.cells_[i].set_varchar(ObString(s.trace_id_len_, s.trace_id_buf_));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cur_row_.cells_[i].set_null();
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
    if (OB_SUCC(ret)) {
      ++cursor_;
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
