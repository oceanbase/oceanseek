/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/cmd/ob_catalog_sync_helper.h"
#include "share/catalog/ob_unified_catalog.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

int ObCatalogSyncOperation::prepare_unified_catalog_target(ObUnifiedCatalogSyncTarget &target,
                                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = target.tenant_id();
  if (OB_FAIL(target.capture(allocator))) {
    LOG_WARN("failed to capture Unified Catalog target", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObUnifiedCatalog::validate_sync_target(target))) {
    LOG_WARN("failed to validate Unified Catalog target", K(ret), K(tenant_id));
  }
  return ret;
}

ObCatalogSyncBackend ObCatalogSync::snapshot_backend(const uint64_t tenant_id)
{
  ObCatalogSyncBackend backend = ObCatalogSyncBackend::NONE;
  if (ObCatalogSyncHelper::is_sync_enabled(tenant_id)) {
    backend = ObUnifiedCatalog::is_configured(tenant_id) ? ObCatalogSyncBackend::UNIFIED_CATALOG
                                                         : ObCatalogSyncBackend::HMS;
  }
  return backend;
}

ObCatalogSyncOperationHandle::ObCatalogSyncOperationHandle()
  : allocator_(NULL),
    operation_(NULL),
    deferred_ret_(OB_SUCCESS)
{
}

ObCatalogSyncOperationHandle::~ObCatalogSyncOperationHandle()
{
  reset();
}

void ObCatalogSyncOperationHandle::set(ObIAllocator &allocator,
                                       ObCatalogSyncOperation *operation)
{
  reset();
  allocator_ = &allocator;
  operation_ = operation;
}

void ObCatalogSyncOperationHandle::set_deferred_error(const int deferred_ret)
{
  reset();
  deferred_ret_ = deferred_ret;
}

void ObCatalogSyncOperationHandle::reset()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(operation_)) {
    operation_->~ObCatalogSyncOperation();
    allocator_->free(operation_);
  }
  allocator_ = NULL;
  operation_ = NULL;
  deferred_ret_ = OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
