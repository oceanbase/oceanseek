/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/schema/ob_schema_guard_context.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

ObSchemaMgrInfo::ObSchemaMgrInfo()
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    snapshot_version_(common::OB_INVALID_VERSION),
    schema_mgr_(NULL),
    publish_state_(NULL),
    mgr_handle_(),
    schema_status_()
{
}

ObSchemaMgrInfo::ObSchemaMgrInfo(
    const uint64_t tenant_id,
    const int64_t snapshot_version,
    const ObSchemaMgr *&schema_mgr,
    const ObSchemaMgrHandle &mgr_handle,
    const ObRefreshSchemaStatus &schema_status)
  : tenant_id_(tenant_id),
    snapshot_version_(snapshot_version),
    schema_mgr_(schema_mgr),
    publish_state_(NULL),
    mgr_handle_(mgr_handle),
    schema_status_(schema_status)
{
}

ObSchemaMgrInfo::ObSchemaMgrInfo(const ObSchemaMgrInfo &other)
  : tenant_id_(common::OB_INVALID_TENANT_ID),
    snapshot_version_(common::OB_INVALID_VERSION),
    schema_mgr_(NULL),
    publish_state_(NULL),
    mgr_handle_(),
    schema_status_()
{
  *this = other;
}

ObSchemaMgrInfo::~ObSchemaMgrInfo()
{
  mgr_handle_.reset();
}

ObSchemaMgrInfo &ObSchemaMgrInfo::operator=(const ObSchemaMgrInfo &other)
{
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    snapshot_version_ = other.snapshot_version_;
    schema_mgr_ = other.schema_mgr_;
    publish_state_ = other.publish_state_;
    mgr_handle_ = other.mgr_handle_;
    schema_status_ = other.schema_status_;
  }
  return *this;
}

void ObSchemaMgrInfo::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  snapshot_version_ = common::OB_INVALID_VERSION;
  schema_mgr_ = NULL;
  publish_state_ = NULL;
  mgr_handle_.reset();
  schema_status_.reset();
}

ObSchemaGuardContext::ObSchemaGuardContext(const ObSchemaMgrItem::Mod mod)
  : local_allocator_(SET_USE_500(common::ObModIds::OB_SCHEMA_MGR_INFO_ARRAY,
                                 common::ObCtxIds::SCHEMA_SERVICE)),
    schema_service_(NULL),
    session_id_(0),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    schema_mgr_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                      common::ModulePageAllocator(local_allocator_)),
    schema_objs_(OB_MALLOC_NORMAL_BLOCK_SIZE,
                 common::ModulePageAllocator(local_allocator_)),
    mod_(mod),
    schema_guard_type_(0),
    restore_tenant_exist_(false),
    is_inited_(false),
    pin_cache_size_(0),
    schema_mode_(ObSimpleSchemaMode::ORIGINAL)
{
}

ObSchemaGuardContext::~ObSchemaGuardContext()
{
}

} // namespace schema
} // namespace share
} // namespace oceanbase
