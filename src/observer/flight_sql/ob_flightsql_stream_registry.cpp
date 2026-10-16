/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SERVER

#include "observer/flight_sql/ob_flightsql_stream_registry.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace observer
{

static const int64_t STREAM_REGISTRY_MAP_BUCKET_NUM = 64;

ObArrowStreamRegistry::ObArrowStreamRegistry()
    : mutex_(common::ObLatchIds::HASH_MAP_LOCK), entries_()
{
}

ObArrowStreamRegistry::~ObArrowStreamRegistry()
{
  if (entries_.created()) {
    int ret = entries_.destroy();
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to destroy Flight SQL stream registry map", K(ret));
    }
  }
}

int ObArrowStreamRegistry::ensure_entries_created_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!entries_.created()) {
    ret = entries_.create(STREAM_REGISTRY_MAP_BUCKET_NUM,
                          common::ObMemAttr(tenant_id, "FlightSQLReg"));
  }
  return ret;
}

int ObArrowStreamRegistry::register_schema(
    uint64_t tenant_id,
    uint64_t stream_id,
    std::shared_ptr<arrow::Schema> schema,
    const ObFlightSqlColumnMapping &mapping,
    std::string db_name,
    std::string table_name)
{
  int ret = OB_SUCCESS;
  ObArrowStreamEntryPtr entry(
      OB_NEW(ObArrowStreamEntry, common::ObMemAttr(tenant_id, "FlightSQLStream")),
      ObArrowStreamEntryDeleter());
  if (OB_ISNULL(entry)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FlightSQL] registry: failed to allocate stream entry", K(ret), K(stream_id));
  } else {
    entry->column_mapping.set_attr(
        common::ObMemAttr(tenant_id, "FlightSQLMap"));
    if (OB_FAIL(entry->column_mapping.assign(mapping))) {
      LOG_WARN("[FlightSQL] registry: failed to copy column mapping",
               K(ret), K(stream_id), K(tenant_id));
    } else {
      entry->arrow_schema = std::move(schema);
      entry->db_name      = std::move(db_name);
      entry->table_name   = std::move(table_name);
      lib::ObMutexGuard guard(mutex_);
      if (OB_FAIL(ensure_entries_created_(tenant_id))) {
        LOG_WARN("failed to initialize Flight SQL stream registry map", K(ret), K(stream_id));
      } else if (OB_FAIL(entries_.set_refactored(stream_id, entry, 1 /* overwrite */))) {
        LOG_WARN("failed to register Flight SQL stream entry", K(ret), K(stream_id));
      }
    }
  }
  return ret;
}

int ObArrowStreamRegistry::attach_reader(uint64_t stream_id,
                                          arrow::RecordBatchReader *reader)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObArrowStreamEntryPtr entry;
  if (!entries_.created()
      || OB_FAIL(entries_.get_refactored(stream_id, entry))
      || OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FlightSQL] registry: stream entry not found for attach_reader",
             K(ret), K(stream_id));
  } else {
    lib::ObMutexGuard reader_guard(entry->read_mu);
    entry->reader          = reader;
    entry->stream_attached = true;
  }
  return ret;
}

ObArrowStreamEntryPtr ObArrowStreamRegistry::get(uint64_t stream_id)
{
  ObArrowStreamEntryPtr entry;
  lib::ObMutexGuard guard(mutex_);
  if (entries_.created()) {
    int ret = entries_.get_refactored(stream_id, entry);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get Flight SQL stream entry", K(ret), K(stream_id));
    }
  }
  return entry;
}

int ObArrowStreamRegistry::unregister(uint64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObArrowStreamEntryPtr entry;
  {
    lib::ObMutexGuard guard(mutex_);
    if (entries_.created()) {
      ret = entries_.erase_refactored(stream_id, &entry);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        LOG_WARN("failed to unregister Flight SQL stream entry", K(ret), K(stream_id));
      }
    }
  }
  if (OB_NOT_NULL(entry)) {
    // The DoPut handler owns the reader on its stack.  Serialize invalidation
    // with ReadNext() so an operator that retained the entry cannot start (or
    // continue) a read after this handler returns.
    lib::ObMutexGuard guard(entry->read_mu);
    int expected = OB_SUCCESS;
    (void)entry->terminal_ret.compare_exchange_strong(
        expected, OB_CANCELED, std::memory_order_acq_rel);
    entry->reader = nullptr;
    entry->stream_attached = false;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
