/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_

#include <stdint.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
class ObSSMemBlockManager;
class ObSSMicroMetaManager;
class ObSSMicroRangeManager;
class ObSSPhysicalBlockManager;
class ObTenantFileManager;
class ObSSLAMicroKeyManager;
struct ObSSMicroCacheStat;

struct ObSSMicroCacheTaskCtx
{
public:
  int32_t block_size_;
  common::ObIAllocator *allocator_;
  ObSSMemBlockManager *mem_blk_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr_;
  ObSSMicroRangeManager *micro_range_mgr_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  ObTenantFileManager *tnt_file_mgr_;
  ObSSMicroCacheStat *cache_stat_;
  ObSSLAMicroKeyManager *la_micro_mgr_;

  ObSSMicroCacheTaskCtx()
    : block_size_(0), allocator_(nullptr), mem_blk_mgr_(nullptr), 
      micro_meta_mgr_(nullptr), micro_range_mgr_(nullptr), phy_blk_mgr_(nullptr), 
      tnt_file_mgr_(nullptr), cache_stat_(nullptr), la_micro_mgr_(nullptr)
  {}
  ~ObSSMicroCacheTaskCtx() { reset(); }

  int init(common::ObIAllocator &allocator, ObSSMemBlockManager &mem_blk_mgr, ObSSMicroMetaManager &micro_meta_mgr,
           ObSSMicroRangeManager &micro_range_mgr, ObSSPhysicalBlockManager &phy_blk_mgr,
           ObTenantFileManager &tnt_file_mgr, ObSSMicroCacheStat &cache_stat, ObSSLAMicroKeyManager &la_micro_mgr);
  bool is_valid() const { return (allocator_ != nullptr) && (mem_blk_mgr_ != nullptr) && (micro_meta_mgr_ != nullptr) 
                                  && (micro_range_mgr_ != nullptr) && (phy_blk_mgr_ != nullptr) && (tnt_file_mgr_ != nullptr) 
                                  && (cache_stat_ != nullptr) && (la_micro_mgr_ != nullptr) && (block_size_ > 0); }
  void reset();
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_ */