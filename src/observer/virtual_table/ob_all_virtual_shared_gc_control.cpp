/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_shared_gc_control.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;

ObAllVirtualSharedGCControl::ObAllVirtualSharedGCControl()
    : ObVirtualTableScannerIterator(),
      schema_service_(NULL),
      tenant_ids_(),
      tenant_idx_(0),
      tenant_ids_built_(false)
{
}

ObAllVirtualSharedGCControl::~ObAllVirtualSharedGCControl()
{
  reset();
}

int ObAllVirtualSharedGCControl::init(ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    schema_service_ = schema_service;
  }
  return ret;
}

void ObAllVirtualSharedGCControl::reset()
{
  schema_service_ = NULL;
  tenant_ids_.reset();
  tenant_idx_ = 0;
  tenant_ids_built_ = false;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSharedGCControl::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next shared gc control row", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSharedGCControl::build_tenant_ids_()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> candidate_tenant_ids;
  tenant_ids_.reset();
  tenant_idx_ = 0;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", KR(ret));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get sys schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_available_tenant_ids(candidate_tenant_ids))) {
      LOG_WARN("fail to get available tenant ids", KR(ret));
    }
  } else if (OB_FAIL(candidate_tenant_ids.push_back(effective_tenant_id_))) {
    LOG_WARN("fail to push back tenant id", KR(ret), K_(effective_tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_tenant_ids.count(); ++i) {
    const uint64_t tenant_id = candidate_tenant_ids.at(i);
    if (!is_tenant_id_in_key_ranges_(tenant_id)) {
      // tenant_id is outside the queried ranges, skip it.
    } else if (OB_FAIL(tenant_ids_.push_back(tenant_id))) {
      LOG_WARN("fail to push back tenant id", KR(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    tenant_ids_built_ = true;
  }
  return ret;
}

bool ObAllVirtualSharedGCControl::is_tenant_id_in_key_ranges_(const uint64_t tenant_id) const
{
  bool in_range = key_ranges_.empty();
  ObObj tenant_id_obj;
  tenant_id_obj.set_int(static_cast<int64_t>(tenant_id));
  ObRowkey tenant_id_rowkey(&tenant_id_obj, 1);
  for (int64_t i = 0; !in_range && i < key_ranges_.count(); ++i) {
    const ObNewRange &range = key_ranges_.at(i);
    const ObRowkey &start_key = range.get_start_key();
    const ObRowkey &end_key = range.get_end_key();
    if (range.is_false_range()) {
      // Empty range, continue checking the next one.
    } else if (range.is_whole_range()) {
      in_range = true;
    } else if (1 != start_key.get_obj_cnt() || 1 != end_key.get_obj_cnt()
        || OB_ISNULL(start_key.get_obj_ptr()) || OB_ISNULL(end_key.get_obj_ptr())) {
      in_range = true;
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
          "unexpected tenant id key range, disable range pruning", K(range));
    } else {
      const int start_cmp = tenant_id_rowkey.compare(start_key);
      const int end_cmp = tenant_id_rowkey.compare(end_key);
      const bool after_start = start_cmp > 0 || (0 == start_cmp && range.border_flag_.inclusive_start());
      const bool before_end = end_cmp < 0 || (0 == end_cmp && range.border_flag_.inclusive_end());
      in_range = after_start && before_end;
    }
  }
  return in_range;
}

int ObAllVirtualSharedGCControl::get_next_tenant_(uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  if (!tenant_ids_built_ && OB_FAIL(build_tenant_ids_())) {
    LOG_WARN("fail to build tenant ids", KR(ret), K_(effective_tenant_id));
  } else if (tenant_idx_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    tenant_id = tenant_ids_.at(tenant_idx_++);
  }
  return ret;
}

int ObAllVirtualSharedGCControl::fill_cells_(
    const int64_t gmt_create,
    const int64_t gmt_modified,
    const ObSharedGCControlInfo &info,
    const SCN &safe_meta_scn)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    switch (output_column_ids_.at(i)) {
      case TENANT_ID:
        cur_row_.cells_[i].set_int(static_cast<int64_t>(info.tenant_id_));
        break;
      case GMT_CREATE:
        cur_row_.cells_[i].set_timestamp(gmt_create);
        break;
      case GMT_MODIFIED:
        cur_row_.cells_[i].set_timestamp(gmt_modified);
        break;
      case EXECUTOR_SERVER_ID:
        cur_row_.cells_[i].set_int(info.executor_server_id_);
        break;
      case STATUS:
        cur_row_.cells_[i].set_varchar(shared_gc_control_status_to_str(info.status_));
        cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case GC_ROUND_ID:
        cur_row_.cells_[i].set_int(info.gc_round_id_);
        break;
      case ALLOW_GC_ROUND_ID:
        cur_row_.cells_[i].set_int(info.allow_gc_round_id_);
        break;
      case GC_META_SCN:
        cur_row_.cells_[i].set_uint64(info.gc_meta_scn_.get_val_for_inner_table_field());
        break;
      case LAST_SUCC_GC_META_SCN:
        cur_row_.cells_[i].set_uint64(info.last_succ_gc_meta_scn_.get_val_for_inner_table_field());
        break;
      case START_TIME:
        if (info.start_time_ > 0) {
          cur_row_.cells_[i].set_timestamp(info.start_time_);
        } else {
          cur_row_.cells_[i].set_null();
        }
        break;
      case SAFE_META_SCN:
        cur_row_.cells_[i].set_uint64(safe_meta_scn.get_val_for_inner_table_field());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected shared gc control column",
            KR(ret), "column_id", output_column_ids_.at(i));
        break;
    }
  }
  return ret;
}

int ObAllVirtualSharedGCControl::get_next_row_(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool need_safe_meta_scn = false;
  row = NULL;
  for (int64_t i = 0; !need_safe_meta_scn && i < output_column_ids_.count(); ++i) {
    need_safe_meta_scn = SAFE_META_SCN == output_column_ids_.at(i);
  }
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is null", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else {
    while (OB_SUCC(ret) && OB_ISNULL(row)) {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t gmt_create = 0;
      int64_t gmt_modified = 0;
      ObSharedGCControlInfo info;
      SCN safe_meta_scn;
      if (OB_FAIL(get_next_tenant_(tenant_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next tenant", KR(ret));
        }
      } else if (OB_FAIL(ObSharedGCControlOperator::read_row(
          *GCTX.sql_proxy_, tenant_id, info, gmt_create, gmt_modified))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to read shared gc control row", KR(ret), K(tenant_id));
        }
      } else if (need_safe_meta_scn) {
        SCN cur_gts;
        const ObVTableScanParam *scan_param = get_scan_param();
        if (OB_ISNULL(scan_param)) {
          ret = OB_NOT_INIT;
          LOG_WARN("scan param is null", KR(ret), K(tenant_id));
        } else {
          const int64_t remain_us = scan_param->timeout_ - ObTimeUtility::current_time_us();
          const int64_t gts_timeout_us = MIN(
              remain_us, ObAtomicProtocolUtil::DEFAULT_GET_GTS_TIMEOUT_US);
          if (OB_UNLIKELY(remain_us <= 0)) {
            ret = OB_TIMEOUT;
            LOG_WARN("virtual table scan timeout before getting tenant gts", KR(ret),
                K(tenant_id), K(scan_param->timeout_));
          } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(
              tenant_id, gts_timeout_us, cur_gts))) {
            LOG_WARN("fail to get tenant gts", KR(ret), K(tenant_id), K(gts_timeout_us));
          } else if (OB_FAIL(ObSSGCUtil::get_safe_meta_scn(
              tenant_id, cur_gts, safe_meta_scn))) {
            LOG_WARN("fail to get safe meta scn", KR(ret), K(tenant_id), K(cur_gts));
          }
        }
      }

      if (OB_SUCC(ret) && OB_INVALID_TENANT_ID != info.tenant_id_) {
        if (OB_FAIL(fill_cells_(gmt_create, gmt_modified, info, safe_meta_scn))) {
          LOG_WARN("fail to fill shared gc control row", KR(ret), K(tenant_id));
        } else {
          row = &cur_row_;
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
