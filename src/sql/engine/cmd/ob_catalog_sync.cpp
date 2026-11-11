/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_catalog_sync.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

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
