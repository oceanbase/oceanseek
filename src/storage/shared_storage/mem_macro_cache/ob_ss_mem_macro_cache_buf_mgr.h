/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_BUF_MGR_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_BUF_MGR_H_

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_struct.h"

namespace oceanbase
{
namespace storage
{
struct ObSSMemMacroCacheStat;

class ObSSMemMacroCacheBufMgr
{
public:
  ObSSMemMacroCacheBufMgr(ObSSMemMacroCacheStat &cache_stat);
  virtual ~ObSSMemMacroCacheBufMgr() {}

  int init(const uint64_t tenant_id, const int32_t blk_size, const int64_t total_blk_cnt);
  void destroy();
  void clear();

  void update_total_mem_block_cnt(const int64_t new_total_cnt);
  int add_macro_block_data(const blocksstable::MacroBlockId &macro_id, const char *buf, const int64_t buf_size,
                           ObSSMacroCacheMemBlkHandle &mem_blk_handle, int32_t &offset, uint32_t &crc);
  int acquire_cold_mem_blocks(const int64_t exp_mem_blk_cnt, common::ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles);
  int free_cold_mem_blocks(common::ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles, int64_t &evict_blk_cnt);

private:
  int inner_create_buckets(const uint64_t tenant_id, const int64_t bucket_cnt);
  void inner_free_buckets();
  void inner_clear_buckets();
  ObSSMacroCacheMemBucket *inner_get_bucket(const blocksstable::MacroBlockId &macro_id, const int64_t buf_size);
  ObSSMacroCacheMemBucket *inner_get_bucket(const int32_t bucket_idx);

private:
  bool is_inited_;
  int64_t bucket_cnt_;
  ObSSMacroCacheMemBucket **buckets_;
  ObSSMacroCacheMemBlockPool mem_blk_pool_;
  ObSSMemMacroCacheStat &cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_BUF_MGR_H_ */
