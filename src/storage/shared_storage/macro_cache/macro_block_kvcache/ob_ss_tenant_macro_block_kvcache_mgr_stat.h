/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_STAT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_stat.h"

namespace oceanbase
{
namespace storage
{

// Tenant 宏块 KVCACHE Manager 的对外统计快照。
// hit/miss 是 Manager 生命周期累计值；persist_stat_ 同时包含 Worker 的累计值和
// 当前值；memory_size_ 和 valid_macro_cnt_ 是查询时的当前值。
struct ObSSTenantMacroBlockKVCacheMgrStat
{
  ObSSTenantMacroBlockKVCacheMgrStat()
  {
    reset();
  }

  void reset()
  {
    hit_cnt_ = 0;
    hit_size_ = 0;
    miss_cnt_ = 0;
    miss_size_ = 0;
    persist_stat_.reset();
    memory_size_ = 0;
    valid_macro_cnt_ = 0;
  }

  TO_STRING_KV(K_(hit_cnt), K_(hit_size), K_(miss_cnt), K_(miss_size),
      K_(persist_stat), K_(memory_size), K_(valid_macro_cnt));

  int64_t hit_cnt_;
  int64_t hit_size_;
  int64_t miss_cnt_;
  int64_t miss_size_;
  // Worker 的 Ring Buffer 和 L2 写统计，字段含义见 ObSSMacroPersistStat。
  ObSSMacroPersistStat persist_stat_;
  // 查询时，全局 KVCACHE 为当前租户保存 Key、Value 和节点元数据占用的字节数。
  int64_t memory_size_;
  // 查询时，全局 KVCACHE 中当前租户保存的 MacroBlockId/Value entry 数。
  int64_t valid_macro_cnt_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_TENANT_MACRO_BLOCK_KVCACHE_MGR_STAT_H_
