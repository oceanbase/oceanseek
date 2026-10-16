/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

int ObSSMicroCacheTaskCtx::init(
    ObIAllocator &allocator, 
    ObSSMemBlockManager &mem_blk_mgr,
    ObSSMicroMetaManager &micro_meta_mgr, 
    ObSSMicroRangeManager &micro_range_mgr, 
    ObSSPhysicalBlockManager &phy_blk_mgr,
    ObTenantFileManager &tnt_file_mgr,
    ObSSMicroCacheStat &cache_stat,
    ObSSLAMicroKeyManager &la_micro_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_mgr.is_inited() || !micro_meta_mgr.is_inited() || !micro_range_mgr.is_inited()
      || !phy_blk_mgr.is_inited() || !tnt_file_mgr.is_inited() || !la_micro_mgr.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    allocator_ = &allocator;
    mem_blk_mgr_ = &mem_blk_mgr;
    micro_meta_mgr_ = &micro_meta_mgr;
    micro_range_mgr_ = &micro_range_mgr;
    phy_blk_mgr_ = &phy_blk_mgr;
    tnt_file_mgr_ = &tnt_file_mgr;
    cache_stat_ = &cache_stat;
    la_micro_mgr_ = &la_micro_mgr;
    block_size_ = phy_blk_mgr.get_block_size();
  }
  return ret;
}

void ObSSMicroCacheTaskCtx::reset()
{
  allocator_ = nullptr;
  mem_blk_mgr_ = nullptr;
  micro_meta_mgr_ = nullptr;
  micro_range_mgr_ = nullptr;
  phy_blk_mgr_ = nullptr;
  tnt_file_mgr_ = nullptr;
  cache_stat_ = nullptr;
  la_micro_mgr_ = nullptr;
  block_size_ = 0;
}

} // storage
} // oceanbase
