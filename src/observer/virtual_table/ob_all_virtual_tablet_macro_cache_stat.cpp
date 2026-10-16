/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_all_virtual_tablet_macro_cache_stat.h"

#include "common/ob_timeout_ctx.h"
#include "lib/charset/ob_charset.h"
#include "lib/thread/thread.h"
#include "observer/ob_server_struct.h"
#include "share/ob_i_tablet_scan.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"

#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;

ObAllVirtualTabletMacroCacheStat::ObAllVirtualTabletMacroCacheStat()
    : ObVirtualTableScannerIterator(),
      omt::ObMultiTenantOperator(),
      predicates_extracted_(false),
      tenant_prepared_(false),
      index_type_(INDEX_TYPE_MAX),
      target_tenant_id_(OB_INVALID_TENANT_ID),
      target_table_ids_(),
      target_tablet_ids_(),
      tablet_idx_(0),
      tablet_ids_(),
      ls_ids_(),
      table_ids_()
{
  ip_buf_[0] = '\0';
}

ObAllVirtualTabletMacroCacheStat::~ObAllVirtualTabletMacroCacheStat()
{
  reset();
}

void ObAllVirtualTabletMacroCacheStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  reset_state_();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTabletMacroCacheStat::reset_state_()
{
  predicates_extracted_ = false;
  index_type_ = INDEX_TYPE_MAX;
  target_tenant_id_ = OB_INVALID_TENANT_ID;
  target_table_ids_.reset();
  target_tablet_ids_.reset();
  reset_tenant_state_();
  ip_buf_[0] = '\0';
}

void ObAllVirtualTabletMacroCacheStat::reset_tenant_state_()
{
  tenant_prepared_ = false;
  tablet_idx_ = 0;
  tablet_ids_.reset();
  ls_ids_.reset();
  table_ids_.reset();
}

void ObAllVirtualTabletMacroCacheStat::set_index_type(const INDEX_TYPE index_type)
{
  index_type_ = index_type;
}

int ObAllVirtualTabletMacroCacheStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (!predicates_extracted_) {
    if (OB_FAIL(extract_predicates_())) {
      SERVER_LOG(WARN, "fail to extract tablet macro cache predicates",
          KR(ret), K(index_type_), K(key_ranges_));
    } else {
      predicates_extracted_ = true;
    }
  }

  if (FAILEDx(execute(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to execute tablet macro cache virtual table", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::extract_predicates_()
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(effective_tenant_id_, "VTTabCachePred");
  target_table_ids_.set_attr(mem_attr);
  target_tablet_ids_.set_attr(mem_attr);
  if (OB_UNLIKELY(INDEX_TYPE_MAX == index_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table_id or tablet_id must be specified");
    SERVER_LOG(WARN, "table or tablet index access path is required",
        KR(ret), K(index_type_), K(key_ranges_));
  } else if (OB_UNLIKELY(key_ranges_.empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table_id or tablet_id must be specified");
    SERVER_LOG(WARN, "tablet macro cache key range is empty", KR(ret), K(index_type_));
  } else if (OB_FAIL(check_exclusive_target_filter_())) {
    SERVER_LOG(WARN, "fail to check exclusive tablet macro cache target filter",
        KR(ret), K(index_type_));
  } else if (OB_FAIL(resolve_target_tenant_id_())) {
    SERVER_LOG(WARN, "fail to resolve target tenant id",
        KR(ret), K(effective_tenant_id_), K(index_type_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
      if (OB_FAIL(handle_key_range_(key_ranges_.at(i)))) {
        SERVER_LOG(WARN, "fail to handle tablet macro cache key range",
            KR(ret), K(i), K(index_type_), "key_range", key_ranges_.at(i));
      }
    }
    if (OB_SUCC(ret)
        && OB_UNLIKELY((INDEX_TYPE_TABLE == index_type_ && target_table_ids_.empty())
            || (INDEX_TYPE_TABLET == index_type_ && target_tablet_ids_.empty()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "table_id or tablet_id must be specified");
      SERVER_LOG(WARN, "no tablet macro cache point target extracted",
          KR(ret), K(index_type_), K(key_ranges_));
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::check_exclusive_target_filter_()
{
  int ret = OB_SUCCESS;
  bool has_conflicting_filter = false;
  uint64_t conflicting_column_id = OB_INVALID_ID;

  // The selected virtual index determines the primary target. A pushdown filter
  // on the other target column would make the scan scope depend on optimizer
  // index selection, so reject the query before reading tablet metadata.
  if (INDEX_TYPE_TABLE == index_type_) {
    conflicting_column_id = TABLET_ID;
  } else if (INDEX_TYPE_TABLET == index_type_) {
    conflicting_column_id = TABLE_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected tablet macro cache index type", KR(ret), K(index_type_));
  }

  if (FAILEDx(has_filter_column_(conflicting_column_id, has_conflicting_filter))) {
    SERVER_LOG(WARN, "fail to check conflicting tablet macro cache filter",
        KR(ret), K(index_type_), K(conflicting_column_id));
  } else if (has_conflicting_filter) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specifying both table_id and tablet_id is");
    SERVER_LOG(WARN, "table and tablet target filters cannot be specified together",
        KR(ret), K(index_type_), K(conflicting_column_id));
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::has_filter_column_(
    const uint64_t target_column_id,
    bool &has_filter)
{
  int ret = OB_SUCCESS;
  has_filter = false;
  const ObVTableScanParam *scan_param = get_scan_param();
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "virtual table scan parameter is null", KR(ret), K(target_column_id));
  } else {
    // SQL plan generation records every column referenced by virtual-table
    // pushdown filters in vt_pd_colums_ids_, including columns used inside
    // IN, OR, or wrapper expressions. Use that list instead of parsing filters again.
    for (int64_t i = 0;
         OB_SUCC(ret) && !has_filter && i < scan_param->vt_pd_colums_ids_.count();
         ++i) {
      has_filter = (target_column_id == scan_param->vt_pd_colums_ids_.at(i));
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::resolve_target_tenant_id_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(effective_tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid effective tenant id", KR(ret), K(effective_tenant_id_));
  } else if (!is_sys_tenant(effective_tenant_id_)) {
    target_tenant_id_ = effective_tenant_id_;
  } else {
    int64_t tenant_id = 0;
    bool tenant_id_found = false;
    bool has_tenant_id_filter = false;
    if (OB_FAIL(get_int_filter_by_column_name("tenant_id", tenant_id, tenant_id_found))) {
      SERVER_LOG(WARN, "fail to extract target tenant id filter",
          KR(ret), K(effective_tenant_id_));
    } else if (tenant_id_found) {
      if (OB_UNLIKELY(tenant_id <= 0 || !is_valid_tenant_id(static_cast<uint64_t>(tenant_id)))) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid target tenant id", KR(ret), K(tenant_id));
      } else {
        target_tenant_id_ = static_cast<uint64_t>(tenant_id);
      }
    } else if (OB_FAIL(has_filter_column_(TENANT_ID, has_tenant_id_filter))) {
      SERVER_LOG(WARN, "fail to check tenant id filter",
          KR(ret), K(effective_tenant_id_), K(index_type_));
    } else if (has_tenant_id_filter) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "a non-equality or multi-value tenant_id predicate is");
      SERVER_LOG(WARN, "tenant id filter must be a single equality predicate",
          KR(ret), K(effective_tenant_id_), K(index_type_));
    } else {
      // An unqualified sys-tenant query inspects sys-tenant tablets only.
      target_tenant_id_ = OB_SYS_TENANT_ID;
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::handle_key_range_(const ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  const int64_t start_obj_count = key_range.get_start_key().get_obj_cnt();
  const int64_t end_obj_count = key_range.get_end_key().get_obj_cnt();
  if (OB_UNLIKELY(start_obj_count < MIN_INDEX_ROWKEY_COL_CNT
      || start_obj_count != end_obj_count)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected tablet macro cache rowkey column count",
        KR(ret), K(start_obj_count), K(end_obj_count), K(index_type_));
  } else {
    const ObObj *low = key_range.get_start_key().get_obj_ptr();
    const ObObj *high = key_range.get_end_key().get_obj_ptr();
    const ObObj &target_low = low[INDEX_TARGET_KEY_IDX];
    const ObObj &target_high = high[INDEX_TARGET_KEY_IDX];
    const bool target_specified = (!target_low.is_min_value()
        && !target_high.is_max_value()
        && (target_low == target_high));
    if (OB_UNLIKELY(!target_specified)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "table_id or tablet_id must be specified");
      SERVER_LOG(WARN, "table or tablet point predicate is required",
          KR(ret), K(index_type_), K(target_low), K(target_high));
    } else if (OB_UNLIKELY(target_low.get_int() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid target object id",
          KR(ret), K(index_type_), K(target_low));
    } else if (INDEX_TYPE_TABLE == index_type_) {
      const uint64_t table_id = static_cast<uint64_t>(target_low.get_int());
      if (OB_FAIL(add_target_table_id_(table_id))) {
        SERVER_LOG(WARN, "fail to add target table id", KR(ret), K(table_id));
      }
    } else if (INDEX_TYPE_TABLET == index_type_) {
      const ObTabletID tablet_id(static_cast<uint64_t>(target_low.get_int()));
      if (OB_UNLIKELY(!tablet_id.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid target tablet id", KR(ret), K(tablet_id));
      } else if (OB_FAIL(add_target_tablet_id_(tablet_id))) {
        SERVER_LOG(WARN, "fail to add target tablet id", KR(ret), K(tablet_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected tablet macro cache index type", KR(ret), K(index_type_));
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::add_target_table_id_(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  for (int64_t i = 0; !exists && i < target_table_ids_.count(); ++i) {
    exists = (table_id == target_table_ids_.at(i));
  }
  if (!exists && OB_FAIL(target_table_ids_.push_back(table_id))) {
    SERVER_LOG(WARN, "fail to append target table id", KR(ret), K(table_id));
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::add_target_tablet_id_(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  for (int64_t i = 0; !exists && i < target_tablet_ids_.count(); ++i) {
    exists = (tablet_id == target_tablet_ids_.at(i));
  }
  if (!exists && OB_FAIL(target_tablet_ids_.push_back(tablet_id))) {
    SERVER_LOG(WARN, "fail to append target tablet id", KR(ret), K(tablet_id));
  }
  return ret;
}

bool ObAllVirtualTabletMacroCacheStat::is_need_process(const uint64_t tenant_id)
{
  return !is_virtual_tenant_id(tenant_id) && (tenant_id == target_tenant_id_);
}

int ObAllVirtualTabletMacroCacheStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!tenant_prepared_) {
    if (OB_FAIL(prepare_curr_tenant_())) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to prepare tablet macro cache tenant",
            KR(ret), K(index_type_));
      }
    }
  }

  while (OB_SUCC(ret) && tablet_idx_ < tablet_ids_.count() && nullptr == row) {
    TabletCacheStat stat;
    bool has_row = false;
    const ObTabletID &tablet_id = tablet_ids_.at(tablet_idx_);
    const ObLSID &ls_id = ls_ids_.at(tablet_idx_);
    const uint64_t table_id = table_ids_.at(tablet_idx_);
    ++tablet_idx_;
    if (OB_FAIL(collect_tablet_stat_(table_id, ls_id, tablet_id, stat, has_row))) {
      SERVER_LOG(WARN, "fail to collect tablet macro cache stat",
          KR(ret), K(table_id), K(ls_id), K(tablet_id));
    } else if (!has_row) {
      // This observer does not host the target LS/tablet. Continue with the next target.
    } else if (OB_FAIL(fill_row_(stat, row))) {
      SERVER_LOG(WARN, "fail to fill tablet macro cache row",
          KR(ret), K(stat));
    }
  }

  if (OB_SUCC(ret) && nullptr == row) {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObAllVirtualTabletMacroCacheStat::release_last_tenant()
{
  reset_tenant_state_();
}

int ObAllVirtualTabletMacroCacheStat::prepare_curr_tenant_()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  int64_t retry_interval_us = INITIAL_PREPARE_RETRY_INTERVAL_US;
  int64_t retry_count = 0;
  bool prepare_finished = false;

  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
      timeout_ctx, GCONF.internal_sql_execute_timeout))) {
    SERVER_LOG(WARN, "fail to set tablet macro cache preparation timeout", KR(ret));
  } else {
    // Build scan targets from two metadata sources that do not share a snapshot:
    // - The schema guard builds aligned table_ids_ and tablet_ids_.
    // - __all_tablet_to_ls fills ls_ids_ for those tablet_ids_.
    //
    // If DROP/TRUNCATE commits between these steps, table_ids_ and tablet_ids_ still
    // contain old tablets while their __all_tablet_to_ls rows have been removed.
    // batch_get_ls() then returns OB_ITEM_NOT_MATCH. Because preparation has not
    // emitted any row, clear all three arrays, acquire a fresh schema guard, and
    // rebuild the targets from the first step.
    while (OB_SUCC(ret) && !prepare_finished) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        SERVER_LOG(WARN, "tablet macro cache preparation is interrupted", KR(ret));
      } else if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        SERVER_LOG(WARN, "tablet macro cache preparation timed out", KR(ret));
      } else if (OB_FAIL(prepare_curr_tenant_once_())) {
        if (OB_ITEM_NOT_MATCH == ret) {
          if (retry_count >= MAX_PREPARE_RETRY_COUNT) {
            SERVER_LOG(WARN, "tablet macro cache preparation retry count exhausted",
                KR(ret), K(retry_count), K(index_type_));
          } else {
            const int prepare_ret = ret;
            ++retry_count;
            ret = OB_SUCCESS;
            SERVER_LOG(INFO, "retry tablet macro cache preparation after tablet mapping changed",
                KR(prepare_ret), K(retry_count), K(retry_interval_us), K(index_type_));
            if (OB_FAIL(wait_for_prepare_retry_(timeout_ctx, retry_interval_us))) {
              SERVER_LOG(WARN, "fail to wait for tablet macro cache preparation retry",
                  KR(ret), KR(prepare_ret), K(retry_count), K(index_type_));
            }
          }
        } else {
          SERVER_LOG(WARN, "fail to prepare tablet macro cache tenant",
              KR(ret), K(index_type_));
        }
      } else {
        prepare_finished = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::prepare_curr_tenant_once_()
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObSchemaGetterGuard schema_guard;
  const ObMemAttr mem_attr(MTL_ID(), "VTTabCacheStat");
  reset_tenant_state_();
  tablet_ids_.set_attr(mem_attr);
  ls_ids_.set_attr(mem_attr);
  table_ids_.set_attr(mem_attr);

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    SERVER_LOG(WARN, "fail to get tenant schema guard", KR(ret));
  } else if (INDEX_TYPE_TABLE == index_type_) {
    if (OB_FAIL(prepare_table_query_(schema_guard))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to prepare table query",
            KR(ret), K(target_table_ids_));
      }
    }
  } else if (INDEX_TYPE_TABLET == index_type_) {
    if (OB_FAIL(prepare_tablet_query_(schema_guard))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to prepare tablet query",
            KR(ret), K(target_tablet_ids_));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected tablet macro cache index type", KR(ret), K(index_type_));
  }

  if (OB_SUCC(ret) && OB_FAIL(resolve_ls_ids_())) {
    SERVER_LOG(WARN, "fail to resolve tablet ls ids",
        KR(ret), K(tablet_ids_), K(table_ids_));
  }
  if (OB_SUCC(ret)) {
    tenant_prepared_ = true;
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::wait_for_prepare_retry_(
    const ObTimeoutCtx &timeout_ctx,
    int64_t &retry_interval_us)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_remain_us = timeout_ctx.get_timeout();
  if (timeout_remain_us <= 0 || timeout_ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    SERVER_LOG(WARN, "tablet macro cache preparation retry timed out before wait",
        KR(ret), K(timeout_remain_us), K(retry_interval_us));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    SERVER_LOG(WARN, "tablet macro cache preparation retry is interrupted",
        KR(ret));
  } else {
    const int64_t sleep_us = MIN(retry_interval_us, timeout_remain_us);
    {
      lib::Thread::WaitGuard guard(lib::Thread::WAIT_FOR_LOCAL_RETRY);
      THIS_WORKER.sched_wait();
      ob_usleep(static_cast<uint32_t>(sleep_us));
      THIS_WORKER.sched_run();
    }
    if (OB_FAIL(THIS_WORKER.check_status())) {
      SERVER_LOG(WARN, "tablet macro cache preparation retry is interrupted",
          KR(ret));
    } else if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      SERVER_LOG(WARN, "tablet macro cache preparation retry timed out after wait",
          KR(ret), K(sleep_us), K(retry_interval_us));
    } else {
      retry_interval_us *= 2;
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::prepare_table_query_(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), "VTTabCacheTbl");
  for (int64_t i = 0; OB_SUCC(ret) && i < target_table_ids_.count(); ++i) {
    const uint64_t table_id = target_table_ids_.at(i);
    const ObTableSchema *table_schema = nullptr;
    ObSEArray<ObTabletID, 16> table_tablet_ids;
    table_tablet_ids.set_attr(mem_attr);
    if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
      SERVER_LOG(WARN, "fail to get target table schema",
          KR(ret), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      // Ignore a table that is not visible at the current schema version.
    } else if (OB_FAIL(table_schema->get_tablet_ids(table_tablet_ids))) {
      SERVER_LOG(WARN, "fail to get target table tablet ids",
          KR(ret), K(table_id));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_tablet_ids.count(); ++j) {
        if (OB_FAIL(tablet_ids_.push_back(table_tablet_ids.at(j)))) {
          SERVER_LOG(WARN, "fail to append table tablet id",
              KR(ret), K(table_id), K(j), "tablet_id", table_tablet_ids.at(j));
        } else if (OB_FAIL(table_ids_.push_back(table_id))) {
          SERVER_LOG(WARN, "fail to append tablet table id",
              KR(ret), K(table_id), K(j), "tablet_id", table_tablet_ids.at(j));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::prepare_tablet_query_(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < target_tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = target_tablet_ids_.at(i);
    uint64_t table_id = OB_INVALID_ID;
    if (OB_FAIL(resolve_table_id_(schema_guard, tablet_id, table_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to resolve target tablet table id",
            KR(ret), K(tablet_id));
      }
    } else if (OB_FAIL(tablet_ids_.push_back(tablet_id))) {
      SERVER_LOG(WARN, "fail to append target tablet id", KR(ret), K(tablet_id));
    } else if (OB_FAIL(table_ids_.push_back(table_id))) {
      SERVER_LOG(WARN, "fail to append target tablet table id",
          KR(ret), K(tablet_id), K(table_id));
    }
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::resolve_table_id_(
    ObSchemaGetterGuard &schema_guard,
    const ObTabletID &tablet_id,
    uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  int64_t schema_version = OB_INVALID_VERSION;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  const ObMemAttr mem_attr(MTL_ID(), "VTTabCacheMap");
  tablet_ids.set_attr(mem_attr);
  table_ids.set_attr(mem_attr);
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(MTL_ID(), schema_version))) {
    SERVER_LOG(WARN, "fail to get tenant schema version",
        KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    SERVER_LOG(WARN, "fail to add tablet id for table mapping",
        KR(ret), K(tablet_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tablet_to_table_history(
      MTL_ID(), tablet_ids, schema_version, table_ids))) {
    SERVER_LOG(WARN, "fail to get tablet to table mapping",
        KR(ret), K(schema_version), K(tablet_id));
  } else if (OB_UNLIKELY(1 != table_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected tablet to table mapping count",
        KR(ret), K(tablet_id), K(table_ids));
  } else if (OB_INVALID_ID == table_ids.at(0)) {
    ret = OB_ITER_END;
  } else {
    table_id = table_ids.at(0);
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::resolve_ls_ids_()
{
  int ret = OB_SUCCESS;
  if (tablet_ids_.empty()) {
    // A valid table can have no tablets visible at the current schema version.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
      *GCTX.sql_proxy_, MTL_ID(), tablet_ids_, ls_ids_))) {
    SERVER_LOG(WARN, "fail to batch get tablet ls ids",
        KR(ret), K(tablet_ids_), K(table_ids_));
  } else if (OB_UNLIKELY(tablet_ids_.count() != ls_ids_.count()
      || tablet_ids_.count() != table_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet, ls and table id count mismatch",
        KR(ret), K(tablet_ids_), K(ls_ids_), K(table_ids_));
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::collect_tablet_stat_(
    const uint64_t table_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    TabletCacheStat &stat,
    bool &has_row)
{
  int ret = OB_SUCCESS;
  stat.reset();
  has_row = false;
  ObLSService *ls_service = MTL(ObLSService *);
  ObStorageCachePolicyService *scp_service = MTL(ObStorageCachePolicyService *);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMacroInfo *macro_info = nullptr;
  ObArenaAllocator macro_allocator(ObMemAttr(MTL_ID(), "VTTabCacheMac"));
  ObMacroInfoIterator macro_iter;
  ObTabletBlockInfo block_info;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id || !ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid tablet macro cache target",
        KR(ret), K(table_id), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service) || OB_ISNULL(scp_service) || OB_ISNULL(macro_cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet macro cache service is null",
        KR(ret), KP(ls_service), KP(scp_service), KP(macro_cache_mgr));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_CACHE_POLICY_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      SERVER_LOG(TRACE, "skip non-local ls for tablet macro cache stat", K(ls_id), K(tablet_id));
    } else {
      SERVER_LOG(WARN, "fail to get tablet ls", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ls is null", KR(ret), K(ls_id), K(tablet_id), KP(ls));
  } else if (OB_ISNULL(ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet service is null", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(
      tablet_id,
      tablet_handle,
      0 /*timeout_us*/,
      ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
      const int get_tablet_ret = ret;
      ret = OB_SUCCESS;
      SERVER_LOG(TRACE, "skip unavailable tablet for tablet macro cache stat",
          KR(get_tablet_ret), K(ls_id), K(tablet_id));
    } else {
      SERVER_LOG(WARN, "fail to get readable committed tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet is null", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(scp_service->get_tablet_policy_status(tablet_id.id(), stat.policy_status_))) {
    SERVER_LOG(WARN, "fail to get tablet storage cache policy",
        KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet->load_macro_info(ls->get_ls_epoch(), macro_allocator, macro_info))) {
    SERVER_LOG(WARN, "fail to load tablet macro info", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(macro_info)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet macro info is null", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
    SERVER_LOG(WARN, "fail to init tablet macro iterator", KR(ret), K(ls_id), K(tablet_id));
  } else {
    stat.table_id_ = table_id;
    stat.ls_id_ = ls_id;
    stat.tablet_id_ = tablet_id;
    stat.transfer_scn_ = tablet->get_reorganization_scn();
    while (OB_SUCC(ret)) {
      block_info.reset();
      if (OB_FAIL(macro_iter.get_next(block_info))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next tablet macro block",
              KR(ret), K(ls_id), K(tablet_id), K(block_info));
        }
      } else if (SSObjUtil::is_macro_data(block_info.macro_id())
          || SSObjUtil::is_macro_meta(block_info.macro_id())) {
        bool is_exist = false;
        ObSSMacroCacheMetaHandle meta_handle;
        ++stat.macro_block_count_;
        stat.macro_block_required_size_ += block_info.occupy_size();
        if (OB_FAIL(macro_cache_mgr->get_macro_cache_meta(
            block_info.macro_id(), is_exist, meta_handle))) {
          SERVER_LOG(WARN, "fail to get macro cache meta",
              KR(ret), K(ls_id), K(tablet_id), K(block_info));
        } else if (is_exist && OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "macro cache meta handle is invalid",
              KR(ret), K(ls_id), K(tablet_id), K(block_info), K(meta_handle));
        } else if (is_exist) {
          ++stat.cached_macro_block_count_;
          stat.cached_macro_block_required_size_ += meta_handle()->get_size();
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      has_row = true;
    }
  }

  if (OB_NOT_NULL(macro_info)) {
    macro_info->reset();
  }
  return ret;
}

int ObAllVirtualTabletMacroCacheStat::fill_row_(
    const TabletCacheStat &stat,
    ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  const ObAddr addr = GCTX.self_addr();
  const char *policy_str = nullptr;
  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "virtual table cells are null", KR(ret), KP(cells));
  } else if (OB_UNLIKELY(!addr.ip_to_string(ip_buf_, sizeof(ip_buf_)))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to convert server ip", KR(ret), K(addr));
  } else if (OB_FAIL(ObStorageCachePolicyStatus::safely_get_str(stat.policy_status_, policy_str))) {
    SERVER_LOG(WARN, "fail to get storage cache policy string",
        KR(ret), K(stat.policy_status_), K(stat.tablet_id_));
  } else if (OB_ISNULL(policy_str)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "storage cache policy string is null",
        KR(ret), K(stat.policy_status_), K(stat.tablet_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP:
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cells[i].set_int(addr.get_port());
          break;
        case TENANT_ID:
          cells[i].set_int(MTL_ID());
          break;
        case LS_ID:
          cells[i].set_int(stat.ls_id_.id());
          break;
        case TABLET_ID:
          cells[i].set_int(stat.tablet_id_.id());
          break;
        case TRANSFER_SCN:
          cells[i].set_uint64(stat.transfer_scn_.get_val_for_inner_table_field());
          break;
        case TABLE_ID:
          cells[i].set_int(stat.table_id_);
          break;
        case STORAGE_CACHE_POLICY:
          cells[i].set_varchar(policy_str);
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case MACRO_BLOCK_COUNT:
          cells[i].set_int(stat.macro_block_count_);
          break;
        case CACHED_MACRO_BLOCK_COUNT:
          cells[i].set_int(stat.cached_macro_block_count_);
          break;
        case MACRO_BLOCK_REQUIRED_SIZE:
          cells[i].set_int(stat.macro_block_required_size_);
          break;
        case CACHED_MACRO_BLOCK_REQUIRED_SIZE:
          cells[i].set_int(stat.cached_macro_block_required_size_);
          break;
        case INFO:
          cells[i].set_varchar("");
          cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected tablet macro cache column", KR(ret), K(column_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
