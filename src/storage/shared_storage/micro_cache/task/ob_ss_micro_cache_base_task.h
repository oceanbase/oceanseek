/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_TASK_H_

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
class ObSSPhysicalBlockManager;
class ObTenantFileManager;
struct ObSSMicroCacheStat;
class ObSSLAMicroKeyManager;
class ObSSMicroCacheTaskCtx;

class ObSSMicroCacheBaseTask
{
public:
  ObSSMicroCacheBaseTask() 
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), ori_interval_us_(0), 
      cur_interval_us_(0), task_ctx_(nullptr)
  {}
  virtual ~ObSSMicroCacheBaseTask() { destroy(); }
  void destroy();
  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);

protected:
  ObSSMemBlockManager* mem_blk_mgr();
  ObSSMicroMetaManager* micro_meta_mgr();
  ObSSPhysicalBlockManager* phy_blk_mgr();
  ObTenantFileManager* tnt_file_mgr();
  ObSSMicroCacheStat* cache_stat();
  ObSSLAMicroKeyManager* la_micro_mgr();

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t ori_interval_us_;
  int64_t cur_interval_us_;
  ObSSMicroCacheTaskCtx *task_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_TASK_H_ */