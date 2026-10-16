/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_H_

#include "lib/allocator/ob_small_allocator.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_meta_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_buf_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_evict_task.h"

namespace oceanbase
{
namespace storage
{

struct SSMemMacroCacheReqGuard
{
public:
  SSMemMacroCacheReqGuard(int64_t &flying_req_cnt) 
    : flying_req_cnt_(flying_req_cnt)
  { 
    ATOMIC_INC(&flying_req_cnt_); 
  }
  ~SSMemMacroCacheReqGuard()
  {
    ATOMIC_DEC(&flying_req_cnt_);
  }

public:
  int64_t &flying_req_cnt_;
};

class ObSSMemMacroCache
{
public:
  ObSSMemMacroCache();
  virtual ~ObSSMemMacroCache() {}
  static int mtl_init(ObSSMemMacroCache *&mem_macro_cache);

  int init(const uint64_t tenant_id, const int64_t memory_limit_size);
  int start();
  void stop();
  int wait();
  void destroy();
  bool is_available() const;
  bool is_cache_enabled() const;
  void enable_cache();
  void disable_cache();
  int clear_mem_macro_cache();
  void update_memory_limit_size(const int64_t new_mem_limit_size);

  int32_t get_blk_size() const { return blk_size_; }
  ObSSMemMacroCacheStat &get_cache_stat() { return cache_stat_; }
  common::ObSmallAllocator &get_meta_allocator() { return meta_allocator_; }

  // @need_fast_persist if it is true, that means we need to evict the macro_block
  // as soon as possible and persist it into disk
  int put(const blocksstable::MacroBlockId &macro_id,
          const uint64_t effective_tablet_id,
          const char *buf,
          const int64_t buf_size,
          bool need_fast_persist = false);

  int get(const blocksstable::MacroBlockId &macro_id,
          common::ObIOInfo &io_info,
          blocksstable::ObStorageObjectHandle &obj_handle,
          bool &is_hit_cache);

  int check_exist(const blocksstable::MacroBlockId &macro_id,
                  bool &is_exist);

  int async_evict_macro_block(
      const blocksstable::MacroBlockId &macro_id,
      const bool need_persist);

private:
  int handle_hit_macro_block(const blocksstable::MacroBlockId &macro_id,
                             const char *io_buf,
                             const int64_t io_buf_size,
                             common::ObIOInfo &io_info,
                             blocksstable::ObStorageObjectHandle &obj_handle);
  void inner_check_macro_meta_cnt();

private:
  const static int64_t INVALID_TG_ID = -1;

private:
  bool is_inited_;
  bool is_stopped_;
  bool is_enabled_;
  bool is_clear_;
  int32_t blk_size_;
  int tg_id_;
  uint64_t tenant_id_;
  int64_t mem_limit_size_;
  int64_t flying_req_cnt_;
  ObSSMemMacroCacheStat cache_stat_;
  ObSSMemMacroCacheMetaMgr meta_mgr_;
  ObSSMemMacroCacheBufMgr buf_mgr_;
  ObSSMemMacroCacheEvictTask evict_task_;
  common::ObSmallAllocator meta_allocator_; // for allocating macro_block meta
};

#define SSMemMacroMetaAlloc (MTL(ObSSMemMacroCache *)->get_meta_allocator())
#define SSMemMacroCacheStat (MTL(ObSSMemMacroCache *)->get_cache_stat())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_H_ */
