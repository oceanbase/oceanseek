/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_key_column_usage.h"

#include "observer/ob_inner_sql_result.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;

ObAllVirtualKeyColumnUsage::ObAllVirtualKeyColumnUsage()
  : initialized_(false), empty_result_(false), tenant_idx_(0),
    tenant_ids_(), filter_sql_(), result_()
{
}

ObAllVirtualKeyColumnUsage::~ObAllVirtualKeyColumnUsage()
{
  reset();
}

void ObAllVirtualKeyColumnUsage::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to close key column usage iterator", KR(ret));
  }
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualKeyColumnUsage::inner_close()
{
  // Also called when the outer query stops early (LIMIT, cancellation, error).
  result_.reset();
  tenant_ids_.reset();
  filter_sql_.reset();
  tenant_idx_ = 0;
  initialized_ = false;
  empty_result_ = false;
  return OB_SUCCESS;
}

int ObAllVirtualKeyColumnUsage::append_string_filter_(const char *column_name)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> values;
  bool found = false;
  values.set_attr(ObMemAttr(effective_tenant_id_, "CdbKcu"));
  if (OB_FAIL(get_string_filter_values_by_column_name(column_name, values, found))) {
    LOG_WARN("extract key column usage filter failed", KR(ret), K(column_name));
  } else if (!found) {
    // Expressions which cannot be extracted remain filters of the outer scan.
  } else if (values.empty()) {
    empty_result_ = true;
  } else if (OB_FAIL(filter_sql_.append_fmt(" AND `%s` IN (", column_name))) {
    LOG_WARN("append filter column failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
      // Hex literals preserve quotes/backslashes independently of SQL_MODE.
      if (OB_FAIL(filter_sql_.append(i == 0 ? "CONVERT(X'" : ",CONVERT(X'"))) {
        LOG_WARN("append filter literal failed", KR(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < values.at(i).length(); ++j) {
        if (OB_FAIL(filter_sql_.append_fmt("%02x", static_cast<unsigned char>(values.at(i).ptr()[j])))) {
          LOG_WARN("append filter value failed", KR(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(filter_sql_.append("' USING utf8mb4) COLLATE utf8mb4_general_ci"))) {
        LOG_WARN("append filter collation failed", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(filter_sql_.append(")"))) {
      LOG_WARN("finish filter failed", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualKeyColumnUsage::init_scan_()
{
  int ret = OB_SUCCESS;
  int64_t tenant_filter = OB_INVALID_TENANT_ID;
  bool has_tenant_filter = false;
  ObArray<uint64_t> tenant_ids;
  if (!is_sys_tenant(effective_tenant_id_)) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("cross tenant key column usage requires sys tenant", KR(ret), K_(effective_tenant_id));
  } else if (OB_ISNULL(schema_guard_) || OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(get_scan_param())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key column usage scan is not initialized", KR(ret));
  } else {
    const ObMemAttr attr(effective_tenant_id_, "CdbKcu");
    filter_sql_.set_attr(attr);
    tenant_ids_.set_attr(attr);
    tenant_ids.set_attr(attr);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_int_filter_by_column_name("TENANT_ID", tenant_filter, has_tenant_filter))) {
    LOG_WARN("extract tenant filter failed", KR(ret));
  } else if (OB_FAIL(append_string_filter_("TABLE_SCHEMA"))
             || OB_FAIL(append_string_filter_("TABLE_NAME"))
             || OB_FAIL(append_string_filter_("CONSTRAINT_NAME"))
             || OB_FAIL(append_string_filter_("COLUMN_NAME"))) {
    LOG_WARN("build key column usage filters failed", KR(ret));
  } else if (empty_result_) {
    // A contradictory predicate needs no tenant SQL.
  } else if (OB_FAIL(schema_guard_->get_available_tenant_ids(tenant_ids))) {
    LOG_WARN("get available tenants failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      const ObSimpleTenantSchema *tenant = nullptr;
      if (has_tenant_filter && (tenant_filter < 0 || tenant_id != static_cast<uint64_t>(tenant_filter))) {
      } else if (!is_sys_tenant(tenant_id) && (!is_user_tenant(tenant_id) || is_meta_tenant(tenant_id))) {
      } else if (OB_FAIL(schema_guard_->get_tenant_info(tenant_id, tenant))) {
        LOG_WARN("get tenant schema failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant disappeared during key column usage scan", KR(ret), K(tenant_id));
      } else if (is_sys_tenant(tenant_id) || MYSQL_MODE == tenant->get_compatibility_mode()) {
        if (OB_FAIL(tenant_ids_.push_back(tenant_id))) {
          LOG_WARN("append target tenant failed", KR(ret), K(tenant_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    initialized_ = true;
  }
  return ret;
}

int ObAllVirtualKeyColumnUsage::open_tenant_()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
  sql.set_attr(ObMemAttr(effective_tenant_id_, "CdbKcu"));
  // The inner connection also inherits the worker's remaining deadline.
  const int64_t remaining = get_scan_param()->timeout_ - ObTimeUtility::current_time();
  // Bucket the hint, as ObAgentTableBase does, to avoid a new SQL text for
  // every remaining microsecond. The worker deadline remains the hard limit.
  int64_t timeout = 1;
  while (timeout < remaining && timeout < OB_MAX_USER_SPECIFIED_TIMEOUT / 10) {
    timeout *= 10;
  }
  if (remaining <= 0) {
    ret = OB_TIMEOUT;
  } else if (OB_FAIL(sql.append_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ "
      "CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_CATALOG,"
      "TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,"
      "POSITION_IN_UNIQUE_CONSTRAINT,REFERENCED_TABLE_SCHEMA,"
      "REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME "
      "FROM information_schema.KEY_COLUMN_USAGE WHERE 1=1%.*s",
      timeout, static_cast<int>(filter_sql_.length()), filter_sql_.ptr()))) {
    LOG_WARN("build tenant key column usage query failed", KR(ret));
  } else if (OB_FAIL(GCTX.sql_proxy_->read(result_, tenant_id, sql.ptr()))) {
    LOG_WARN("read tenant key column usage failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(result_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant key column usage result is null", KR(ret), K(tenant_id));
  } else {
    LOG_TRACE("opened tenant key column usage", K(tenant_id), K(sql));
  }
  return ret;
}

int ObAllVirtualKeyColumnUsage::project_row_(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObNewRow *source = static_cast<ObInnerSQLResult *>(result_.get_result())->get_row();
  if (OB_ISNULL(source) || source->get_count() != COLUMN_COUNT - 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key column usage result columns", KR(ret), KPC(source));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      if (column_id == TENANT_ID) {
        cur_row_.cells_[i].set_int(tenant_ids_.at(tenant_idx_));
      } else if (column_id > TENANT_ID && column_id < TENANT_ID + COLUMN_COUNT) {
        // Keep NULLs and duplicate rows. The result owns strings until next().
        cur_row_.cells_[i] = source->get_cell(column_id - TENANT_ID - 1);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key column usage column", KR(ret), K(column_id));
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualKeyColumnUsage::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!initialized_ && OB_FAIL(init_scan_())) {
    LOG_WARN("initialize key column usage scan failed", KR(ret));
  }
  while (OB_SUCC(ret) && OB_ISNULL(row)) {
    if (tenant_idx_ >= tenant_ids_.count()) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(result_.get_result()) && OB_FAIL(open_tenant_())) {
      LOG_WARN("open key column usage tenant failed", KR(ret), K_(tenant_idx));
    } else if (OB_FAIL(result_.get_result()->next())) {
      if (OB_ITER_END == ret) {
        result_.reset();
        ++tenant_idx_;
        ret = OB_SUCCESS;
      } else {
        // Never replay a tenant after returning rows: that would duplicate them.
        LOG_WARN("iterate tenant key column usage failed", KR(ret), K_(tenant_idx));
      }
    } else if (OB_FAIL(project_row_(row))) {
      LOG_WARN("project key column usage row failed", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
