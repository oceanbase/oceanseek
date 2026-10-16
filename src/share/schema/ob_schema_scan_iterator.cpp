/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_scan_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

namespace {
template <typename SchemaT>
int assign_schema_ptrs(const ObIArray<const SchemaT *> &schemas,
                       ObIArray<const void *> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_FAIL(out.reserve(schemas.count()))) {
    LOG_WARN("fail to reserve schemas", KR(ret), "cnt", schemas.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
      if (OB_FAIL(out.push_back(static_cast<const void *>(schemas.at(i))))) {
        LOG_WARN("fail to push schema ptr", KR(ret), K(i));
      }
    }
  }
  return ret;
}
} // namespace

ObSchemaIdScanIterator::ObSchemaIdScanIterator()
  : is_inited_(false),
    mode_(ObSchemaScanIterMode::INVALID),
    guard_(NULL),
    tenant_id_(OB_INVALID_TENANT_ID),
    idx_(0),
    ids_(),
    schemas_()
{
}

void ObSchemaIdScanIterator::reset()
{
  is_inited_ = false;
  mode_ = ObSchemaScanIterMode::INVALID;
  guard_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
  idx_ = 0;
  ids_.reset();
  schemas_.reset();
}

int64_t ObSchemaIdScanIterator::count() const
{
  int64_t cnt = 0;
  if (ObSchemaScanIterMode::BY_SCHEMA == mode_) {
    cnt = schemas_.count();
  } else {
    cnt = ids_.count();
  }
  return cnt;
}

int ObSchemaIdScanIterator::init_common_(ObSchemaGetterGuard &guard, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_can_full_scan(tenant_id))) {
    LOG_WARN("full scan not allowed", KR(ret), K(tenant_id));
  } else {
    mode_ = ObSchemaScanIterMode::BY_ID;
    guard_ = &guard;
    tenant_id_ = tenant_id;
    idx_ = 0;
  }
  return ret;
}

int ObSchemaIdScanIterator::init_by_schemas_(const ObIArray<const void *> &schemas)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(schemas_.assign(schemas))) {
    LOG_WARN("fail to assign schemas", KR(ret), "cnt", schemas.count());
  } else {
    mode_ = ObSchemaScanIterMode::BY_SCHEMA;
    idx_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSchemaIdScanIterator::init(ObSchemaGetterGuard &guard, const ObSchemaScanSpec &spec)
{
  int ret = OB_SUCCESS;
  reset();
  // Must go through Guard so scan-id cache applies (接口设计.md §2.2).
  if (OB_FAIL(init_common_(guard, spec.tenant_id_))) {
    LOG_WARN("fail to init common", KR(ret), K(spec));
  } else if (OB_FAIL(ObOnDemandSchemaGetterGuard::instance().get_table_ids_by_inner_table(
                 guard, spec, ids_))) {
    LOG_WARN("fail to get table ids by inner table", KR(ret), K(spec));
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

int ObSchemaIdScanIterator::init(ObSchemaGetterGuard &guard, const ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  reset();
  const uint64_t tenant_id = guard.get_tenant_id();
  if (OB_FAIL(init_common_(guard, tenant_id))) {
    LOG_WARN("fail to init common", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ids_.assign(ids))) {
    LOG_WARN("fail to assign ids", KR(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSchemaIdScanIterator::get_next_id(uint64_t &id)
{
  int ret = OB_SUCCESS;
  id = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ObSchemaScanIterMode::BY_ID != mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next_id only valid in BY_ID mode", KR(ret), K_(mode));
  } else if (idx_ >= ids_.count()) {
    ret = OB_ITER_END;
  } else {
    id = ids_.at(idx_++);
  }
  return ret;
}

int ObSchemaIdScanIterator::get_next_schema_ptr_(const void *&schema)
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(ObSchemaScanIterMode::BY_SCHEMA != mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next_schema_ptr_ only valid in BY_SCHEMA mode", KR(ret), K_(mode));
  } else {
    while (OB_SUCC(ret) && OB_ISNULL(schema)) {
      if (idx_ >= schemas_.count()) {
        ret = OB_ITER_END;
      } else {
        schema = schemas_.at(idx_++);
        if (OB_ISNULL(schema)) {
          LOG_WARN("schema ptr is null, skip", K_(idx), "cnt", schemas_.count());
        }
      }
    }
  }
  return ret;
}

int ObSimpleTableSchemaIterator::init(const ObIArray<const ObSimpleTableSchemaV2 *> &schemas)
{
  int ret = OB_SUCCESS;
  ObArray<const void *> ptrs;
  if (OB_FAIL(assign_schema_ptrs(schemas, ptrs))) {
    LOG_WARN("fail to assign schema ptrs", KR(ret));
  } else if (OB_FAIL(init_by_schemas_(ptrs))) {
    LOG_WARN("fail to init by schemas", KR(ret));
  }
  return ret;
}

int ObSimpleTableSchemaIterator::get_next(const ObSimpleTableSchemaV2 *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_by_schema()) {
    const void *ptr = NULL;
    if (OB_FAIL(get_next_schema_ptr_(ptr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next schema ptr", KR(ret));
      }
    } else {
      table_schema = static_cast<const ObSimpleTableSchemaV2 *>(ptr);
    }
  } else if (OB_ISNULL(guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("guard is null", KR(ret));
  } else {
    while (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
      uint64_t table_id = OB_INVALID_ID;
      if (OB_FAIL(get_next_id(table_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next id", KR(ret));
        }
      } else if (OB_FAIL(guard_->get_simple_table_schema(tenant_id_, table_id, table_schema))) {
        LOG_WARN("fail to get simple table schema", KR(ret), K_(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        // Dropped concurrently relative to the inner-sql snapshot; skip.
        LOG_WARN("table schema is null, skip", K_(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

int ObTableSchemaIterator::init(const ObIArray<const ObTableSchema *> &schemas)
{
  int ret = OB_SUCCESS;
  ObArray<const void *> ptrs;
  if (OB_FAIL(assign_schema_ptrs(schemas, ptrs))) {
    LOG_WARN("fail to assign schema ptrs", KR(ret));
  } else if (OB_FAIL(init_by_schemas_(ptrs))) {
    LOG_WARN("fail to init by schemas", KR(ret));
  }
  return ret;
}

int ObTableSchemaIterator::get_next(const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_by_schema()) {
    const void *ptr = NULL;
    if (OB_FAIL(get_next_schema_ptr_(ptr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next schema ptr", KR(ret));
      }
    } else {
      table_schema = static_cast<const ObTableSchema *>(ptr);
    }
  } else if (OB_ISNULL(guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("guard is null", KR(ret));
  } else {
    while (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
      uint64_t table_id = OB_INVALID_ID;
      if (OB_FAIL(get_next_id(table_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next id", KR(ret));
        }
      } else if (OB_FAIL(guard_->get_table_schema(tenant_id_, table_id, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        LOG_WARN("table schema is null, skip", K_(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
