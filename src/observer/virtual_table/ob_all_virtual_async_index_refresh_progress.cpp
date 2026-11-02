/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "observer/virtual_table/ob_all_virtual_async_index_refresh_progress.h"
#include "common/ob_timeout_ctx.h"
#include "lib/thread/thread.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_define.h"
#include "share/scn.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_async_index_params.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_async_index_refresh_info.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx/ob_trans_service.h"


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

static const int64_t INITIAL_PROGRESS_SCAN_RETRY_INTERVAL_US = 10 * 1000; // 10 ms
static const int64_t MAX_PROGRESS_SCAN_RETRY_INTERVAL_US = 1000 * 1000; // 1 s

ObAllVirtualAsyncIndexRefreshProgress::ObAllVirtualAsyncIndexRefreshProgress()
    : ObVirtualTableScannerIterator(),
      addr_(),
      output_rows_(),
      cursor_(0),
      prepared_(false)
{
  ip_buf_[0] = '\0';
}

ObAllVirtualAsyncIndexRefreshProgress::~ObAllVirtualAsyncIndexRefreshProgress()
{
  reset();
}

void ObAllVirtualAsyncIndexRefreshProgress::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  output_rows_.reset();
  cursor_ = 0;
  prepared_ = false;
  ObVirtualTableScannerIterator::reset();
}

// Sys tenant sees rows from every tenant; any other querying tenant only sees
// its own -- matching sibling virtual tables' isolation policy.
bool ObAllVirtualAsyncIndexRefreshProgress::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

void ObAllVirtualAsyncIndexRefreshProgress::release_last_tenant()
{
  output_rows_.reset();
  cursor_ = 0;
  prepared_ = false;
}

int ObAllVirtualAsyncIndexRefreshProgress::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(ret));
  } else if (!prepared_ && OB_FAIL(prepare_to_read_())) {
    SERVER_LOG(WARN, "prepare_to_read_ failed", K(ret));
  } else if (cursor_ >= output_rows_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(fill_row_(output_rows_.at(cursor_)))) {
    SERVER_LOG(WARN, "fill row failed", K(ret), K(cursor_));
  } else {
    ++cursor_;
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", K(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
      timeout_ctx, GCONF.internal_sql_execute_timeout))) {
    SERVER_LOG(WARN, "set refresh progress timeout context failed", K(ret));
  } else {
    bool scan_finished = false;
    int last_ret = OB_SUCCESS;
    int64_t retry_count = 0;
    int64_t retry_interval_us = INITIAL_PROGRESS_SCAN_RETRY_INTERVAL_US;
    while (OB_SUCC(ret) && !scan_finished) {
      output_rows_.reset();
      cursor_ = 0;
      {
        // do not hold schema guard too long, it will block the schema service
        ObSchemaGetterGuard schema_guard;
        if (timeout_ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          SERVER_LOG(WARN, "async index refresh progress scan timed out",
                     KR(ret), KR(last_ret), K(retry_count), "tenant_id", MTL_ID());
        } else if (OB_FAIL(schema_service->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
          SERVER_LOG(WARN, "get tenant schema guard failed", K(ret), "tenant_id", MTL_ID());
        } else if (OB_FAIL(aggregate_local_tenant_(schema_guard))) {
          SERVER_LOG(WARN, "aggregate local tenant failed", KR(ret), "tenant_id", MTL_ID());
        }
      }
      // Release the schema guard before waiting for the next scan.
      if (OB_FAIL(ret)) {
        if (is_progress_scan_retryable_(ret)) {
          last_ret = ret;
          ++retry_count;
          SERVER_LOG(INFO, "retry async index refresh progress scan",
                     KR(ret), K(retry_count), K(retry_interval_us), "tenant_id", MTL_ID());
          ret = OB_SUCCESS;
          if (OB_FAIL(wait_for_progress_scan_retry_(timeout_ctx, retry_interval_us))) {
            SERVER_LOG(WARN, "wait for async index refresh progress scan retry failed",
                       KR(ret), KR(last_ret), K(retry_count), "tenant_id", MTL_ID());
          }
        }
      } else {
        prepared_ = true;
        scan_finished = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::wait_for_progress_scan_retry_(
    const ObTimeoutCtx &timeout_ctx,
    int64_t &retry_interval_us)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_remain_us = timeout_ctx.get_timeout();
  if (timeout_remain_us <= 0 || timeout_ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
  } else {
    const int64_t sleep_us = OB_MIN(retry_interval_us, timeout_remain_us);
    {
      lib::Thread::WaitGuard guard(lib::Thread::WAIT_FOR_LOCAL_RETRY);
      THIS_WORKER.sched_wait();
      ob_usleep(static_cast<uint32_t>(sleep_us));
      THIS_WORKER.sched_run();
    }
    if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
    } else {
      retry_interval_us = OB_MIN(
          retry_interval_us * 2, MAX_PROGRESS_SCAN_RETRY_INTERVAL_US);
    }
  }
  return ret;
}

bool ObAllVirtualAsyncIndexRefreshProgress::is_progress_scan_retryable_(
    const int ret) const
{
  return is_partition_change_error(ret)
      || is_data_not_readable_err(ret)
      || is_partition_splitting(ret)
      || OB_TABLE_NOT_EXIST == ret
      || OB_EAGAIN == ret
      || OB_ITEM_NOT_MATCH == ret
      || OB_ERR_SHARED_LOCK_CONFLICT == ret
      || OB_LS_OFFLINE == ret
      || OB_TABLET_IS_SPLIT_SRC == ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::aggregate_local_tenant_(
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const bool tenant_enable_refresh = ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id);
  ObSEArray<const ObSimpleTableSchemaV2 *, 16> simple_table_schemas;
  if (OB_FAIL(schema_guard.get_table_schemas_with_async_index_in_tenant(tenant_id, simple_table_schemas))) {
    SERVER_LOG(WARN, "get async index tables failed", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *simple_schema = simple_table_schemas.at(i);
      if (ObTimeoutCtx::get_ctx().is_timeouted()) {
        ret = OB_TIMEOUT;
        SERVER_LOG(WARN, "aggregate local tenant timed out", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(simple_schema)) {
        SERVER_LOG(WARN, "simple table schema is null, skip", K(tenant_id), K(i));
        continue;
      } else {
        const uint64_t data_table_id = simple_schema->get_table_id();
        const ObTableSchema *table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, table_schema))) {
          SERVER_LOG(WARN, "get data table schema failed", K(ret), K(tenant_id), K(data_table_id));
        } else if (OB_ISNULL(table_schema)) {
          SERVER_LOG(WARN, "data table schema is null, skip", K(tenant_id), K(data_table_id));
          continue;
        } else {
          // Compute index_target_snapshot once per data_table
          // valid_data_tablets carries the (ls_id, data_tablet_id) pairs whose
          // refresh_snapshots is non-empty; every async index row under this
          // data_table restricts aggregation to the index tablets co-located
          // with these data tablets, excluding partitions with no refresh target.
          int64_t index_target_snapshot = -1;
          bool scn_valid = false;
          ObSEArray<DataTabletLoc, 16> valid_data_tablets;
          if (OB_FAIL(compute_index_target_snapshot_for_table_(
              *table_schema, index_target_snapshot, valid_data_tablets,
              scn_valid))) {
            SERVER_LOG(WARN, "compute data read snapshot failed", K(ret), K(tenant_id), K(data_table_id));
          } else if (valid_data_tablets.empty() || !scn_valid) {
            // No local data tablet on this node has any recorded increment, skip this data table
          } else {
            const ObIArray<ObAuxTableMetaInfo> &aux_infos = table_schema->get_simple_index_infos();
            for (int64_t j = 0; OB_SUCC(ret) && j < aux_infos.count(); ++j) {
              const ObAuxTableMetaInfo &aux_info = aux_infos.at(j);
              if (is_async_index(aux_info.index_type_)) {
                const bool is_async_search_def = is_async_search_def_index(aux_info.index_type_);
                uint64_t index_table_id = aux_info.table_id_;
                const ObTableSchema *index_schema = nullptr;
                if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
                  SERVER_LOG(WARN, "get index table schema failed", K(ret), K(tenant_id), K(index_table_id));
                } else if (OB_ISNULL(index_schema)) {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "index table schema is null", K(ret), K(tenant_id), K(index_table_id));
                } else {
                  const ObTableSchema *refresh_index_schema = index_schema;
                  if (is_async_search_def) {
                    if (OB_FAIL(index_schema->get_search_data_index_tid(index_table_id))) {
                      SERVER_LOG(WARN, "get async search data table id failed", K(ret), K(tenant_id), KPC(index_schema));
                    } else if (OB_FAIL(schema_guard.get_table_schema(
                        tenant_id, index_table_id, refresh_index_schema))) {
                      SERVER_LOG(WARN, "get async search data table schema failed", K(ret), K(tenant_id), K(index_table_id));
                    } else if (OB_ISNULL(refresh_index_schema)) {
                      ret = OB_ERR_UNEXPECTED;
                      SERVER_LOG(WARN, "async search data table schema is null", K(ret), K(tenant_id), K(index_table_id));
                    }
                  }
                  if (OB_FAIL(ret)) {
                  } else if (!refresh_index_schema->can_read_index()) {
                    SERVER_LOG(INFO, "cannot read index", K(ret), K(tenant_id), K(index_table_id));
                  } else if (OB_FAIL(aggregate_one_index_table_(*table_schema, *refresh_index_schema,
                                                                index_target_snapshot, tenant_enable_refresh,
                                                                valid_data_tablets))) {
                    SERVER_LOG(WARN, "aggregate one index table failed", K(ret), K(data_table_id), K(index_table_id));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::aggregate_one_index_table_(
    const ObTableSchema &table_schema,
    const ObTableSchema &index_schema,
    const int64_t index_target_snapshot,
    const bool tenant_enable_refresh,
    const ObIArray<DataTabletLoc> &valid_data_tablets)
{
  int ret = OB_SUCCESS;
  const uint64_t data_table_id = table_schema.get_table_id();
  const uint64_t index_table_id = index_schema.get_table_id();
  const int64_t timeout = GCONF.internal_sql_execute_timeout;
  const ObTimeoutCtx &scan_timeout_ctx = ObTimeoutCtx::get_ctx();
  const bool is_partitioned = table_schema.is_partitioned_table();
  bool enable_refresh = tenant_enable_refresh;
  share::SCN s_now = share::SCN::min_scn();
  ObLSService *ls_svc = MTL(ObLSService *);
  if (enable_refresh) {
    ObAsyncIndexParams async_index_params;
    if (OB_FAIL(ObAsyncIndexParams::parse(table_schema.get_async_index_params(), async_index_params))) {
      SERVER_LOG(WARN, "failed to parse async index params", K(ret), K(data_table_id),
                 "async_index_params", table_schema.get_async_index_params());
    } else if (ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL == async_index_params.refresh_interval_) {
      enable_refresh = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (scan_timeout_ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    SERVER_LOG(WARN, "aggregate index table timed out", KR(ret), K(data_table_id), K(index_table_id));
  } else if (OB_ISNULL(ls_svc)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ls service is null", K(ret), K(MTL_ID()));
  } else if (enable_refresh) {
    transaction::ObTransService *txs = MTL(transaction::ObTransService *);
    ObTimeoutCtx timeout_ctx;
    if (OB_ISNULL(txs)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "txs is null", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, timeout))) {
      SERVER_LOG(WARN, "failed to set default timeout ctx", K(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(timeout_ctx.get_abs_timeout(), s_now))) {
      SERVER_LOG(WARN, "failed to get read snapshot version from trans service", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_data_tablets.count(); ++i) {
      const DataTabletLoc &loc = valid_data_tablets.at(i);
      ObTabletID index_tablet_id;
      if (scan_timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        SERVER_LOG(WARN, "aggregate index tablet timed out", KR(ret), K(index_table_id), K(loc));
      } else if (!is_partitioned) {
        index_tablet_id = index_schema.get_tablet_id();
      } else {
        int64_t part_idx = OB_INVALID_INDEX;
        int64_t subpart_idx = OB_INVALID_INDEX;
        ObObjectID tmp_object_id = OB_INVALID_ID;
        ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
        if (OB_FAIL(table_schema.get_part_idx_by_tablet(loc.tablet_id_, part_idx, subpart_idx))) {
          SERVER_LOG(WARN, "get part idx by data tablet failed", K(ret), K(data_table_id), "data_tablet_id", loc.tablet_id_);
        } else if (OB_FAIL(index_schema.get_part_id_and_tablet_id_by_idx(part_idx,
                                                                         subpart_idx,
                                                                         tmp_object_id,
                                                                         tmp_first_level_part_id,
                                                                         index_tablet_id))) {
          SERVER_LOG(WARN, "get index tablet by part idx failed", K(ret), K(index_table_id), K(part_idx), K(subpart_idx), "data_tablet_id", loc.tablet_id_);
        }
      }

      if (OB_SUCC(ret)) {
        ObLSHandle ls_handle;
        ObLS *ls = nullptr;
        if (OB_FAIL(ls_svc->get_ls(loc.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
          // The data scan already found this LS locally. Its disappearance
          // invalidates the data/index pair and requires a new scan.
          SERVER_LOG(WARN, "failed to get index tablet's LS while collecting refresh progress",
                     KR(ret), K(loc));
        } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ls in handle is null", K(ret), "ls_id", loc.ls_id_);
        } else {
          ObTabletHandle handle;
          if (OB_FAIL(ls->get_tablet(
              index_tablet_id,
              handle,
              0 /*timeout_us*/,
              ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
            SERVER_LOG(WARN, "failed to get readable index tablet on data tablet's LS",
                       KR(ret), K(index_tablet_id), K(loc));
          } else {
            ObTablet *tablet = handle.get_obj();
            int64_t index_snapshot = 0;
            ProgressRow *row = nullptr;
            // Empty refreshes advance logical publication without installing an SSTable.
            if (OB_ISNULL(tablet)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "index tablet is null", K(ret), K(index_tablet_id), "ls_id", loc.ls_id_);
            } else if (OB_FAIL(tablet->get_async_index_query_published_version(
                           SCN::max_scn(), index_snapshot))) {
              SERVER_LOG(WARN, "get async index query published version failed",
                         K(ret), K(index_tablet_id),
                         "ls_id", loc.ls_id_);
            } else if (OB_FAIL(get_or_create_row_(data_table_id, index_table_id, row))) {
              SERVER_LOG(WARN, "get or create row failed", K(ret), K(data_table_id), K(index_table_id));
            } else if (OB_ISNULL(row)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "row is null", K(ret), K(data_table_id), K(index_table_id));
            } else {
              row->set_index_publish_version(index_snapshot);
              row->set_index_target_snapshot(index_target_snapshot);
              row->enable_refresh_ = enable_refresh;
              if (enable_refresh) {
                share::SCN index_scn = async_index_snapshot_to_scn(index_snapshot);
                if (index_scn.is_valid_and_not_min() && !index_scn.is_base_scn()) {
                  // index snapshot is valid, use (data_tablet_snapshot - index_tablet_snapshot) as pending
                  row->set_pending(index_snapshot, loc.data_snapshot_);
                } else {
                  // index snapshot is invalid, means no refresh before, use (s_now - data_tablet_snapshot) as pending
                  int64_t s_now_snapshot = async_index_scn_to_snapshot(s_now);
                  row->set_pending(loc.data_snapshot_, s_now_snapshot);
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::compute_index_target_snapshot_for_table_(
    const ObTableSchema &table_schema,
    int64_t &index_target_snapshot,
    ObIArray<DataTabletLoc> &valid_data_tablets,
    bool &scn_valid)
{
  int ret = OB_SUCCESS;
  index_target_snapshot = -1;
  valid_data_tablets.reset();
  bool any_valid = false;
  int64_t max_of_max = INT64_MIN;

  const uint64_t data_table_id = table_schema.get_table_id();
  const ObTimeoutCtx &timeout_ctx = ObTimeoutCtx::get_ctx();
  ObSEArray<ObTabletID, 16> data_tablet_ids;
  ObSEArray<ObLSID, 16> data_ls_ids;
  ObLSService *ls_svc = MTL(ObLSService *);

  if (OB_ISNULL(ls_svc)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ls service is null", K(ret), K(MTL_ID()));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql proxy is null", K(ret));
  } else if (OB_FAIL(table_schema.get_tablet_ids(data_tablet_ids))) {
    SERVER_LOG(WARN, "get data tablet ids failed", K(ret), K(data_table_id));
  } else if (data_tablet_ids.empty()) {
    // No data tablets: leave index_target_snapshot as -1, skip this data table.
  } else if (timeout_ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    SERVER_LOG(WARN, "resolve data tablet locations timed out", KR(ret), K(data_table_id));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
      *GCTX.sql_proxy_, MTL_ID(), data_tablet_ids, data_ls_ids))) {
    // Propagate location lookup errors for centralized retry classification.
    SERVER_LOG(WARN, "failed to batch get ls", KR(ret),
               K(data_table_id), K(data_tablet_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = data_tablet_ids.at(i);
      const ObLSID &ls_id = data_ls_ids.at(i);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        SERVER_LOG(WARN, "collect data tablet refresh progress timed out", KR(ret), K(tablet_id), K(ls_id));
      } else if (OB_FAIL(ls_svc->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        if (OB_LS_NOT_EXIST == ret) {
          // This node does not host the tablet's authoritative LS.
          SERVER_LOG(TRACE, "skip non-local data tablet while collecting refresh progress",
                     KR(ret), K(tablet_id), K(ls_id));
          ret = OB_SUCCESS;
          continue;
        } else {
          SERVER_LOG(WARN, "failed to get data tablet's mapped LS while collecting refresh progress",
                     KR(ret), K(tablet_id), K(ls_id));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "mapped ls in handle is null", K(ret), K(tablet_id), K(ls_id));
      } else {
        ObTabletHandle handle;
        ObTabletAsyncIndexRefreshInfo info;
        int64_t timeout_us = 0;
        if (OB_FAIL(ls->get_tablet(
            tablet_id,
            handle,
            0 /*timeout_us*/,
            ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
          SERVER_LOG(WARN, "failed to get mapped data tablet by LS while collecting refresh progress",
                     KR(ret), K(tablet_id), K(ls_id));
        } else if (OB_ISNULL(handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "tablet is null", K(ret), K(tablet_id), K(ls_id));
        } else if (OB_UNLIKELY((timeout_us = timeout_ctx.get_timeout()) <= 0)) {
          ret = OB_TIMEOUT;
          SERVER_LOG(WARN, "get async index refresh info timed out", KR(ret), K(tablet_id), K(ls_id));
        } else if (OB_FAIL(handle.get_obj()->get_async_index_refresh_info(share::SCN::max_scn(), info, timeout_us))) {
          if (OB_EMPTY_RESULT == ret) {
            // No visible refresh info: there is no refresh target to aggregate.
            ret = OB_SUCCESS;
          } else {
            SERVER_LOG(WARN, "get async index refresh info failed",
                       KR(ret), K(tablet_id), K(ls_id));
          }
        } else {
          const ObIArray<int64_t> &snapshots = info.get_refresh_snapshots();
          if (!snapshots.empty()) {
            // Non-empty refresh snapshots determine participation, regardless
            // of the MDS membership flag.
            const int64_t per_tablet_max = snapshots.at(snapshots.count() - 1);
            max_of_max = OB_MAX(max_of_max, per_tablet_max);
            any_valid = true;
            // Correlate each data tablet with its index tablet on the same LS.
            if (OB_FAIL(valid_data_tablets.push_back(DataTabletLoc(per_tablet_max, ls_id, tablet_id)))) {
              SERVER_LOG(WARN, "push back valid data tablet failed", K(ret), K(tablet_id), K(ls_id));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && any_valid) {
    index_target_snapshot = max_of_max;
    share::SCN data_snapshot = async_index_snapshot_to_scn(max_of_max);
    scn_valid = data_snapshot.is_valid_and_not_min() && !data_snapshot.is_base_scn();
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::get_or_create_row_(
    uint64_t data_table_id,
    uint64_t index_table_id,
    ProgressRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  for (int64_t i = 0; nullptr == row && i < output_rows_.count(); ++i) {
    ProgressRow &r = output_rows_.at(i);
    if (r.data_table_id_ == data_table_id && r.index_table_id_ == index_table_id) {
      row = &r;
    }
  }
  if (nullptr == row) {
    ProgressRow new_row;
    new_row.data_table_id_ = data_table_id;
    new_row.index_table_id_ = index_table_id;
    if (OB_FAIL(output_rows_.push_back(new_row))) {
      SERVER_LOG(WARN, "push back progress row failed", K(ret));
    } else {
      row = &output_rows_.at(output_rows_.count() - 1);
    }
  }
  return ret;
}

int ObAllVirtualAsyncIndexRefreshProgress::fill_row_(const ProgressRow &r)
{
  int ret = OB_SUCCESS;
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
        // index_table_id (rowkey)
        cur_row_.cells_[i].set_int(static_cast<int64_t>(r.index_table_id_));
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // data_table_id
        cur_row_.cells_[i].set_int(static_cast<int64_t>(r.data_table_id_));
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        // index_target_snapshot
        cur_row_.cells_[i].set_uint64(r.index_target_snapshot_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // index_published_snapshot (not null; 1 means never published)
        cur_row_.cells_[i].set_uint64(r.index_published_snapshot_);
        break;
      case OB_APP_MIN_COLUMN_ID + 7: {
        // refresh_delay_ms, set NULL when refresh is disabled by _enable_async_index_refresh = false or ASYNC_REFRESH_INTERVAL = -1
        if (!r.enable_refresh_) {
          cur_row_.cells_[i].set_null();
        } else {
          int64_t delay_ms = 0;
          if (r.has_pending_) {
            delay_ms = static_cast<int64_t>(r.max_delay_ns_ / 1000000UL);
          }
          cur_row_.cells_[i].set_int(delay_ms);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
        break;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
