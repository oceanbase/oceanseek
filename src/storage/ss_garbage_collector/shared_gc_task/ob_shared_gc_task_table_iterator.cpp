/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_task_table_iterator.h"

#include "lib/allocator/page_arena.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

// ============================================================================
// ObSharedGCTaskTableIteratorBase
// ============================================================================
ObSharedGCTaskTableIteratorBase::ObSharedGCTaskTableIteratorBase()
  : is_inited_(false),
    opened_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    real_tenant_id_(OB_INVALID_TENANT_ID),
    client_(nullptr),
    result_(nullptr),
    returned_row_count_(0)
{
}

ObSharedGCTaskTableIteratorBase::~ObSharedGCTaskTableIteratorBase()
{
  reset();
}

void ObSharedGCTaskTableIteratorBase::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    mtl_free(result_);
    result_ = nullptr;
  }
}

void ObSharedGCTaskTableIteratorBase::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  real_tenant_id_ = OB_INVALID_TENANT_ID;
  client_ = nullptr;
  returned_row_count_ = 0;
}

int ObSharedGCTaskTableIteratorBase::open_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator already opened", KR(ret));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObMySQLProxy::ReadResult), "SharedGCIter"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read result", KR(ret));
  } else if (OB_ISNULL(result_ = new (buf) ObMySQLProxy::ReadResult())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct read result", KR(ret));
  } else if (OB_FAIL(build_select_sql_(sql))) {
    LOG_WARN("failed to build select sql", KR(ret));
  } else if (OB_ISNULL(client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client is null", KR(ret));
  } else if (OB_FAIL(client_->read(*result_, real_tenant_id_, sql.ptr(), OBCG_STORAGE))) {
    LOG_WARN("failed to read sql", KR(ret), K(sql));
  } else if (OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after read", KR(ret));
  } else {
    opened_ = true;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSharedGCTaskTableIteratorBase::fetch_next_row_(ObMySQLResult *&result)
{
  int ret = OB_SUCCESS;
  result = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!opened_) {
    if (OB_FAIL(open_())) {
      LOG_WARN("failed to open iterator", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(result_) || OB_ISNULL(result_->get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result_->get_result()->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to fetch next row", KR(ret));
      } else {
        LOG_TRACE("iterator reached end", KR(ret), K_(returned_row_count));
      }
    } else {
      result = result_->get_result();
      ++returned_row_count_;
    }
  }
  return ret;
}


// ============================================================================
// ObSharedGCTaskTableRangeIterator
// ============================================================================
ObSharedGCTaskTableRangeIterator::ObSharedGCTaskTableRangeIterator()
  : ObSharedGCTaskTableIteratorBase(),
    type_(ObSharedGCTaskType::INVALID_TYPE),
    round_id_(0),
    slice_range_()
{
}

ObSharedGCTaskTableRangeIterator::~ObSharedGCTaskTableRangeIterator()
{
  reset();
}

void ObSharedGCTaskTableRangeIterator::reset()
{
  ObSharedGCTaskTableIteratorBase::reset();
  type_ = ObSharedGCTaskType::INVALID_TYPE;
  round_id_ = 0;
  slice_range_ = ObSSGCTaskIdRange();
}

int ObSharedGCTaskTableRangeIterator::init(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    const int64_t round_id,
    const ObSSGCTaskIdRange &slice_range,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_shared_data_gc_task_type(type)
      || round_id < 0
      || slice_range.start_task_id < 0
      || slice_range.end_task_id < slice_range.start_task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(type), K(round_id), K(slice_range));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_real_tenant_id_(
      tenant_id, real_tenant_id_))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    type_ = type;
    round_id_ = round_id;
    slice_range_ = slice_range;
    client_ = &client;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedGCTaskTableRangeIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(!is_shared_data_gc_task_type(type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gc task type", KR(ret), K_(type));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_sql_timeout_us_(tenant_id_, timeout))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld "
      "AND round_id = %ld "
      "AND task_id >= %ld AND task_id <= %ld "
      "ORDER BY task_id",
      timeout,
      OB_ALL_SHARED_GC_TASK_TNAME,
      tenant_id_,
      static_cast<int64_t>(type_),
      round_id_,
      slice_range_.start_task_id, slice_range_.end_task_id))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  return ret;
}

int ObSharedGCTaskTableRangeIterator::get_next(
    ObIAllocator &allocator, ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  ObMySQLResult *row = nullptr;
  if (OB_FAIL(fetch_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::parse_row_(*row, allocator, task))) {
    LOG_WARN("failed to parse row", KR(ret));
  }
  return ret;
}


// ============================================================================
// ObSharedGCTaskTypeFilter
// ============================================================================
int ObSharedGCTaskTypeFilter::assign(const ObSharedGCTaskTypeFilter &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(types_.assign(other.types_))) {
    LOG_WARN("failed to assign types", KR(ret));
  } else {
    is_filter_enabled_ = other.is_filter_enabled_;
  }
  return ret;
}


// ============================================================================
// ObSharedGCTaskTableFullIterator
// ============================================================================
ObSharedGCTaskTableFullIterator::ObSharedGCTaskTableFullIterator()
  : ObSharedGCTaskTableIteratorBase(),
    table_name_(nullptr),
    type_filter_(),
    round_id_filter_()
{
}

ObSharedGCTaskTableFullIterator::~ObSharedGCTaskTableFullIterator()
{
  reset();
}

void ObSharedGCTaskTableFullIterator::reset()
{
  ObSharedGCTaskTableIteratorBase::reset();
  table_name_ = nullptr;
  type_filter_.reset();
  round_id_filter_.reset();
}

int ObSharedGCTaskTableFullIterator::init(
    const uint64_t tenant_id,
    const char *table_name,
    const ObSharedGCTaskTypeFilter &type_filter,
    const ObSharedGCTaskRoundIdFilter &round_id_filter,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(table_name)
                         || !round_id_filter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(table_name));
  } else if (OB_UNLIKELY((0 != STRCMP(table_name, OB_ALL_SHARED_GC_TASK_TNAME))
      && (0 != STRCMP(table_name, OB_ALL_SHARED_GC_TASK_HISTORY_TNAME)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported table name", KR(ret), K(tenant_id), K(table_name));
  } else if (OB_FAIL(type_filter_.assign(type_filter))) {
    LOG_WARN("failed to assign type filter", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_real_tenant_id_(
      tenant_id, real_tenant_id_))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    table_name_ = table_name;
    client_ = &client;
    round_id_filter_ = round_id_filter;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedGCTaskTableFullIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_ISNULL(table_name_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table name is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_sql_timeout_us_(tenant_id_, timeout))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "gmt_create, gmt_modified, value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu",
      timeout,
      table_name_,
      tenant_id_))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  if (OB_SUCC(ret) && type_filter_.need_filter()) {
    // pushdown: type IN (v1, v2, ...)
    if (OB_FAIL(sql.append(" AND type IN ("))) {
      LOG_WARN("failed to append type in", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < type_filter_.types_.count()); ++i) {
      if (OB_FAIL(sql.append_fmt("%s%ld",
          (0 == i) ? "" : ", ", static_cast<int64_t>(type_filter_.types_.at(i))))) {
        LOG_WARN("failed to append type value", KR(ret), K(i));
      }
    }
    if (FAILEDx(sql.append(")"))) {
      LOG_WARN("failed to append type in close", KR(ret));
    }
  }
  if (OB_SUCC(ret) && round_id_filter_.need_filter()) {
    // pushdown: round_id BETWEEN low AND high (skipped when filter disabled)
    if (OB_FAIL(sql.append_fmt(" AND round_id >= %ld AND round_id <= %ld",
        round_id_filter_.round_id_low_, round_id_filter_.round_id_high_))) {
      LOG_WARN("failed to append round id range", KR(ret), K_(round_id_filter));
    }
  }
  if (FAILEDx(sql.append(" ORDER BY type, round_id, task_id"))) {
    LOG_WARN("failed to append order by", KR(ret));
  }
  LOG_INFO("finish to build select sql", K(ret), K(sql));
  return ret;
}

int ObSharedGCTaskTableFullIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task,
    int64_t &gmt_create,
    int64_t &gmt_modified)
{
  int ret = OB_SUCCESS;
  ObMySQLResult *row = nullptr;
  if (OB_FAIL(fetch_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::parse_row_(
      *row, allocator, task, gmt_create, gmt_modified))) {
    LOG_WARN("failed to parse row", KR(ret));
  }
  return ret;
}


// ============================================================================
// ObSharedGCTaskLSMetaIterator
// ============================================================================
ObSharedGCTaskLSMetaIterator::ObSharedGCTaskLSMetaIterator()
  : ObSharedGCTaskTableIteratorBase()
{
}

ObSharedGCTaskLSMetaIterator::~ObSharedGCTaskLSMetaIterator()
{
  reset();
}

void ObSharedGCTaskLSMetaIterator::reset()
{
  ObSharedGCTaskTableIteratorBase::reset();
}

int ObSharedGCTaskLSMetaIterator::init(
    const uint64_t tenant_id,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_real_tenant_id_(
      tenant_id, real_tenant_id_))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    client_ = &client;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedGCTaskLSMetaIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_FAIL(ObSharedGCTaskTableOperator::get_sql_timeout_us_(tenant_id_, timeout))) {
    LOG_WARN("failed to get sql timeout", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld "
      "ORDER BY task_id",
      timeout,
      OB_ALL_SHARED_GC_TASK_TNAME,
      tenant_id_,
      static_cast<int64_t>(ObSharedGCTaskType::LS_META_GC)))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  return ret;
}

int ObSharedGCTaskLSMetaIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  ObMySQLResult *row = nullptr;
  task.reset();
  if (OB_FAIL(fetch_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::parse_row_(*row, allocator, task))) {
    LOG_WARN("failed to parse row", KR(ret));
  } else if (OB_UNLIKELY(!is_ls_meta_gc_type(task.key_.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non ls meta gc task type", KR(ret), K(task.key_));
  }
  return ret;
}

int ObSharedGCTaskLSMetaIterator::get_next(ObSharedGCLSMetaInfo &ls_meta_info)
{
  int ret = OB_SUCCESS;
  ObSharedGCTask task;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "SharedGCLSIt"));
  ls_meta_info.reset();
  if (OB_FAIL(get_next(allocator, task))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next ls meta gc task", KR(ret));
    }
  } else if (OB_FAIL(ls_meta_info.from_value(task.value_))) {
    LOG_WARN("failed to deserialize ls meta info", KR(ret), K(task.key_));
  }
  return ret;
}

// ============================================================================
// ObDeleteTenantTaskIterator 实现
// ============================================================================
ObDeleteTenantTaskIterator::ObDeleteTenantTaskIterator()
  : ObSharedGCTaskTableIteratorBase()
{
}

ObDeleteTenantTaskIterator::~ObDeleteTenantTaskIterator()
{
  reset();
}

void ObDeleteTenantTaskIterator::reset()
{
  ObSharedGCTaskTableIteratorBase::reset();
}

int ObDeleteTenantTaskIterator::init(ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else {
    tenant_id_ = OB_SYS_TENANT_ID;
    real_tenant_id_ = OB_SYS_TENANT_ID;
    client_ = &client;
    is_inited_ = true;
  }
  return ret;
}

int ObDeleteTenantTaskIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_FAIL(ObSharedGCTaskTableOperator::get_sql_timeout_us_(
      OB_SYS_TENANT_ID, timeout))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld AND round_id = 0 "
      "ORDER BY task_id",
      timeout,
      OB_ALL_SHARED_GC_TASK_TNAME,
      OB_SYS_TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::DELETE_TENANT)))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  return ret;
}

int ObDeleteTenantTaskIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  ObMySQLResult *row = nullptr;
  task.reset();
  if (OB_FAIL(fetch_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::parse_row_(*row, allocator, task))) {
    LOG_WARN("failed to parse row", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != task.key_.tenant_id_
      || !is_delete_tenant_type(task.key_.type_)
      || 0 != task.key_.round_id_
      || task.key_.task_id_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected delete tenant task key", KR(ret), K(task.key_));
  }
  return ret;
}

int ObDeleteTenantTaskIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task,
    ObDeleteTenantTaskValue &value)
{
  int ret = OB_SUCCESS;
  value.reset();
  if (OB_FAIL(get_next(allocator, task))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next delete tenant task", KR(ret));
    }
  } else if (OB_FAIL(value.from_value(task.value_))) {
    LOG_WARN("failed to deserialize delete tenant task value", KR(ret), K(task.key_));
  } else if (OB_UNLIKELY(!value.is_valid()
      || static_cast<uint64_t>(task.key_.task_id_) != value.tenant_id_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid delete tenant task value", KR(ret), K(task.key_), K(value));
  }
  return ret;
}

// ============================================================================
// ObDeleteServerTaskIterator 实现
// ============================================================================
ObDeleteServerTaskIterator::ObDeleteServerTaskIterator()
  : ObSharedGCTaskTableIteratorBase()
{
}

ObDeleteServerTaskIterator::~ObDeleteServerTaskIterator()
{
  reset();
}

void ObDeleteServerTaskIterator::reset()
{
  ObSharedGCTaskTableIteratorBase::reset();
}

int ObDeleteServerTaskIterator::init(ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else {
    tenant_id_ = OB_SYS_TENANT_ID;
    real_tenant_id_ = OB_SYS_TENANT_ID;
    client_ = &client;
    is_inited_ = true;
  }
  return ret;
}

int ObDeleteServerTaskIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_FAIL(ObSharedGCTaskTableOperator::get_sql_timeout_us_(
      OB_SYS_TENANT_ID, timeout))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, round_id, task_id, "
      "value, extra_info, server_id, trace_id FROM %s "
      "WHERE tenant_id = %lu AND type = %ld AND round_id = 0 "
      "ORDER BY task_id",
      timeout,
      OB_ALL_SHARED_GC_TASK_TNAME,
      OB_SYS_TENANT_ID,
      static_cast<int64_t>(ObSharedGCTaskType::DELETE_SERVER)))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  return ret;
}

int ObDeleteServerTaskIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task)
{
  int ret = OB_SUCCESS;
  ObMySQLResult *row = nullptr;
  task.reset();
  if (OB_FAIL(fetch_next_row_(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::parse_row_(*row, allocator, task))) {
    LOG_WARN("failed to parse row", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != task.key_.tenant_id_
      || !is_delete_server_type(task.key_.type_)
      || 0 != task.key_.round_id_
      || task.key_.task_id_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected delete server task key", KR(ret), K(task.key_));
  }
  return ret;
}

int ObDeleteServerTaskIterator::get_next(
    ObIAllocator &allocator,
    ObSharedGCTask &task,
    ObDeleteServerTaskValue &value)
{
  int ret = OB_SUCCESS;
  value.reset();
  if (OB_FAIL(get_next(allocator, task))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next delete server task", KR(ret));
    }
  } else if (OB_FAIL(value.from_value(task.value_))) {
    LOG_WARN("failed to deserialize delete server task value", KR(ret), K(task.key_));
  } else if (OB_UNLIKELY(!value.is_valid()
      || static_cast<uint64_t>(task.key_.task_id_) != value.server_id_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid delete server task value", KR(ret), K(task.key_), K(value));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
