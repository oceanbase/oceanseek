/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_UTIL_H_

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"

namespace oceanbase
{
namespace storage
{

class ObSSMemMacroCacheUtil
{
public:
  static int alloc_mem_block(const uint64_t tenant_id, const int32_t blk_size, ObSSMacroCacheMemBlockPool &mem_blk_pool,
                             ObSSMacroCacheMemBlock *&mem_blk);
  static int free_mem_block(ObSSMacroCacheMemBlock *mem_blk);
  static int alloc_buckets(const uint64_t tenant_id, const int64_t bucket_cnt, ObSSMacroCacheMemBlockPool &mem_blk_pool,
                           ObSSMacroCacheMemBucket **&buckets);
  static int free_buckets(ObSSMacroCacheMemBucket **buckets, const int64_t bucket_cnt);
  static int64_t get_random_close_ttl_time(const int64_t ttl_time_us, const int64_t max_wait_time_us);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_UTIL_H_ */
