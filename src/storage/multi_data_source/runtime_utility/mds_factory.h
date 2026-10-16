/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_MDS_FACTORY_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_MDS_FACTORY_H
#include "mds_tenant_service.h"
#include "common_define.h"

namespace oceanbase
{
namespace transaction
{
enum class ObTxDataSourceType : int64_t;
class ObTransID;
}
namespace storage
{
namespace mds
{

struct MdsFactory
{
  static int deep_copy_buffer_ctx(const transaction::ObTransID &trans_id,
                                  const BufferCtx &old_ctx,
                                  BufferCtx *&new_ctx,
                                  ObIAllocator &allocator = MTL(ObTenantMdsService*)->get_buffer_ctx_allocator(),
                                  const char *alloc_file = __builtin_FILE(),
                                  const char *alloc_func = __builtin_FUNCTION(),
                                  const int64_t line = __builtin_LINE());
  static int create_buffer_ctx(const transaction::ObTxDataSourceType &data_source_type,
                               const transaction::ObTransID &trans_id,
                               BufferCtx *&buffer_ctx,
                               ObIAllocator &allocator = MTL(ObTenantMdsService*)->get_buffer_ctx_allocator(),
                               const char *alloc_file = __builtin_FILE(),
                               const char *alloc_func = __builtin_FUNCTION(),
                               const int64_t line = __builtin_LINE());
};

}
}
}
#endif
