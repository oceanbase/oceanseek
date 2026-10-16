/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_BLOCK_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_BLOCK_MANAGER_H_

#include <stdint.h>
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

struct ObSSMicroCacheStat;
class ObSSMemBlockManager
{
public:
  ObSSMemBlockManager(ObSSMicroCacheStat &cache_stat);
  virtual ~ObSSMemBlockManager() {}
  void destroy();
  void clear();

  bool is_inited() const { return is_inited_; }
  int init(const uint64_t tenant_id, const uint32_t block_size);

  int add_micro_block_data(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t size,
                           ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc, bool &alloc_new, 
                           const bool is_fg = true);
  int add_into_sealed_block_list(ObSSMemBlockHandle &mem_blk_handle);
  int get_sealed_mem_blocks(ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, const int64_t max_cnt);
  int64_t get_sealed_mem_block_cnt() const;
  int64_t max_mem_block_cnt() const { return max_mem_blk_cnt_; }
  int64_t get_free_mem_blk_cnt(const bool is_fg) const;
private:
  int inner_free_all_mem_blocks();
  int inner_add_fg_micro_data(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t size, 
                              ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc, bool &alloc_new);
  int inner_add_bg_micro_data(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t size, 
                              ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc, bool &alloc_new);
  int inner_calc_data_offset(const ObSSMicroBlockCacheKey &micro_key, const int32_t size, ObSSMemBlockHandle &mem_blk_handle, 
                             int32_t &data_offset, int32_t &idx_offset, const bool is_fg, bool &alloc_new);
  int inner_seal_and_alloc_mem_block(const bool is_fg);
  int do_alloc_mem_block(ObSSMemBlock *&mem_blk, const bool is_fg);
  int do_free_mem_blocks(common::ObFixedQueue<ObSSMemBlock> &mem_blks);
  int do_free_mem_block(ObSSMemBlock *mem_blk);
  int pop_sealed_mem_blocks(const bool is_fg, common::ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, const int64_t max_cnt);
  int handle_uncomplete_sealed_mem_block();

private:
  bool is_inited_;
  uint32_t block_size_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_; // control concurrent io on fg_mem_block
  int64_t max_mem_blk_cnt_;
  ObSSMemBlockPool mem_blk_pool_;
  ObSSMemBlock *fg_mem_blk_;
  ObSSMemBlock *bg_mem_blk_;
  common::ObFixedQueue<ObSSMemBlock> sealed_fg_mem_blks_;
  common::ObFixedQueue<ObSSMemBlock> sealed_bg_mem_blks_;
  common::ObFixedQueue<ObSSMemBlock> uncomplete_sealed_mem_blks_;
  ObSSMicroCacheStat &cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_BLOCK_MANAGER_H_ */
