/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STAT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

struct ObSSMemMacroCacheHitStat
{
public:
  ObSSMemMacroCacheHitStat() { reset(); }
  ~ObSSMemMacroCacheHitStat() { reset(); }

  void reset()
  {
    reuse();
  }

  void reuse()
  {
    hit_cnt_ = 0;
    hit_size_ = 0;
    miss_cnt_ = 0;
    miss_size_ = 0;
    new_add_cnt_ = 0;
    new_add_size_ = 0;
    fail_add_cnt_ = 0;
    fail_add_size_ = 0;
  }

  void update_hit_stat(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&hit_cnt_, delta_cnt);
    ATOMIC_AAF(&hit_size_, delta_size);
  }

  void update_miss_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&miss_cnt_, delta_cnt);
    ATOMIC_AAF(&miss_size_, delta_size);
  }

  void update_new_add_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&new_add_cnt_, delta_cnt);
    ATOMIC_AAF(&new_add_size_, delta_size);
  }

  void update_fail_add_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&fail_add_cnt_, delta_cnt);
    ATOMIC_AAF(&fail_add_size_, delta_size);
  }

  int64_t get_fail_add_cnt() const { return ATOMIC_LOAD(&fail_add_cnt_); }

  TO_STRING_KV(K_(hit_cnt), K_(hit_size), K_(miss_cnt), K_(miss_size), K_(new_add_cnt), K_(new_add_size),
    K_(fail_add_cnt), K_(fail_add_size));

public:
  int64_t hit_cnt_;
  int64_t hit_size_;
  int64_t miss_cnt_;
  int64_t miss_size_;
  int64_t new_add_cnt_;
  int64_t new_add_size_;
  int64_t fail_add_cnt_; // fail due to space not enough
  int64_t fail_add_size_;
};

struct ObSSMemMacroCacheMacroBlkStat
{
public:
  ObSSMemMacroCacheMacroBlkStat() { reset(); }
  ~ObSSMemMacroCacheMacroBlkStat() { reset(); }
  
  void reset()
  {
    reuse();
  }

  void reuse()
  {
    valid_macro_cnt_ = 0;
    valid_macro_size_ = 0;
    small_macro_cnt_ = 0;
    small_macro_size_ = 0;
    write_back_macro_cnt_ = 0;
    write_back_macro_size_ = 0;
    failed_write_back_macro_cnt_ = 0;   // count of macro blocks that failed write-back
    retry_prewarm_macro_cnt_ = 0;       // count of macro blocks that failed write-back but successfully entered prewarm queue
    evict_macro_cnt_ = 0;
    expire_macro_cnt_ = 0;
    fast_persist_macro_cnt_ = 0;        // count of macro blocks requested for fast persist to disk
    async_evict_macro_cnt_ = 0;         // count of macro blocks requested for async eviction
  }

  void update_valid_macro_info(const int64_t delta_cnt, const int64_t delta_size, const bool is_small_macro)
  {
    ATOMIC_AAF(&valid_macro_cnt_, delta_cnt);
    ATOMIC_AAF(&valid_macro_size_, delta_size);
    if (is_small_macro) {
      ATOMIC_AAF(&small_macro_cnt_, delta_cnt);
      ATOMIC_AAF(&small_macro_size_, delta_size);
    }
  }

  void update_write_back_macro_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&write_back_macro_cnt_, delta_cnt);
    ATOMIC_AAF(&write_back_macro_size_, delta_size);
  }

  void inc_failed_write_back_macro_cnt()
  {
    ATOMIC_INC(&failed_write_back_macro_cnt_);
  }

  void inc_retry_prewarm_macro_cnt()
  {
    ATOMIC_INC(&retry_prewarm_macro_cnt_);
  }

  void update_erased_macro_info(const bool is_for_ttl, const int64_t delta_cnt)
  {
    if (is_for_ttl) {
      ATOMIC_AAF(&expire_macro_cnt_, delta_cnt);
    } else {
      ATOMIC_AAF(&evict_macro_cnt_, delta_cnt);
    }
  }

  void inc_fast_persist_macro_cnt()
  {
    ATOMIC_INC(&fast_persist_macro_cnt_);
  }

  void inc_async_evict_macro_cnt()
  {
    ATOMIC_INC(&async_evict_macro_cnt_);
  }

  int64_t get_fast_evict_macro_cnt() const
  {
    return ATOMIC_LOAD(&fast_persist_macro_cnt_) + ATOMIC_LOAD(&async_evict_macro_cnt_);
  }

  TO_STRING_KV(K_(valid_macro_cnt), K_(valid_macro_size), K_(small_macro_cnt), K_(small_macro_size),
    K_(write_back_macro_cnt), K_(write_back_macro_size), K_(failed_write_back_macro_cnt),
    K_(retry_prewarm_macro_cnt), K_(evict_macro_cnt), K_(expire_macro_cnt), K_(fast_persist_macro_cnt),
    K_(async_evict_macro_cnt));

public:
  int64_t valid_macro_cnt_;
  int64_t valid_macro_size_;
  int64_t small_macro_cnt_;
  int64_t small_macro_size_;
  int64_t write_back_macro_cnt_;          // count of macro blocks that written back to disk successfully
  int64_t write_back_macro_size_;
  int64_t failed_write_back_macro_cnt_;   // count of macro blocks that failed write-back
  int64_t retry_prewarm_macro_cnt_;       // count of macro blocks that failed write-back but successfully entered prewarm queue
  int64_t evict_macro_cnt_;
  int64_t expire_macro_cnt_;
  int64_t fast_persist_macro_cnt_;        // count of macro blocks requested for fast persist to disk
  int64_t async_evict_macro_cnt_;         // count of macro blocks requested for async eviction
};

struct ObSSMemMacroCacheMemBlkStat
{
public:
  ObSSMemMacroCacheMemBlkStat() { reset(); }
  ~ObSSMemMacroCacheMemBlkStat() { reset(); }

  void reset()
  {
    reuse();
    total_blk_cnt_ = 0;
    mem_blk_size_ = 0;
  }

  void reuse()
  {
    alloc_blk_cnt_ = 0;
    evict_blk_cnt_ = 0;
    expire_blk_cnt_ = 0;
    extra_evict_blk_cnt_ = 0;
  }

  void set_mem_block_info(const int32_t blk_size, const int64_t total_cnt) 
  { 
    ATOMIC_STORE(&mem_blk_size_, blk_size);
    ATOMIC_STORE(&total_blk_cnt_, total_cnt);
  }
  void update_mem_block_alloc_cnt(const int64_t delta_cnt) { ATOMIC_AAF(&alloc_blk_cnt_, delta_cnt); }
  void update_mem_block_erased_cnt(const bool is_for_ttl, const int64_t delta_cnt) 
  {
    if (is_for_ttl) {
      ATOMIC_AAF(&expire_blk_cnt_, delta_cnt);   
    } else {
      ATOMIC_AAF(&evict_blk_cnt_, delta_cnt); 
    }
  }
  void update_extra_evict_blk_cnt(const int64_t delta_cnt) { ATOMIC_AAF(&extra_evict_blk_cnt_, delta_cnt); }

  TO_STRING_KV(K_(mem_blk_size), K_(total_blk_cnt), K_(alloc_blk_cnt), K_(evict_blk_cnt), K_(expire_blk_cnt),
    K_(extra_evict_blk_cnt));

public:
  int32_t mem_blk_size_;
  int64_t total_blk_cnt_;
  int64_t alloc_blk_cnt_;
  int64_t evict_blk_cnt_;
  int64_t expire_blk_cnt_;
  int64_t extra_evict_blk_cnt_; // extra evict mem blocks due to tenant memory usage is too high
};

struct ObSSMemMacroCacheIOStat
{
public:
  struct ObSSMemMacroCacheIOStatParam
  {
  public:
    int64_t add_cnt_;
    int64_t add_bytes_;
    int64_t get_cnt_;
    int64_t get_bytes_;

    ObSSMemMacroCacheIOStatParam() { reset(); }
    void reset()
    {
      add_cnt_ = 0;
      add_bytes_ = 0;
      get_cnt_ = 0;
      get_bytes_ = 0;
    }

    void update_io_stat_param(const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes)
    {
      if (is_read) {
        ATOMIC_AAF(&get_cnt_, delta_cnt);
        ATOMIC_AAF(&get_bytes_, delta_bytes);
      } else {
        ATOMIC_AAF(&add_cnt_, delta_cnt);
        ATOMIC_AAF(&add_bytes_, delta_bytes);
      }
    }

    int64_t add_cnt() const { return ATOMIC_LOAD(&add_cnt_); }
    int64_t add_bytes() const { return ATOMIC_LOAD(&add_bytes_); }
    int64_t get_cnt() const { return ATOMIC_LOAD(&get_cnt_); }
    int64_t get_bytes() const { return ATOMIC_LOAD(&get_bytes_); }

    TO_STRING_KV(K_(add_cnt), K_(add_bytes), K_(get_cnt), K_(get_bytes));
  };

public:
  ObSSMemMacroCacheIOStat() { reset(); }
  ~ObSSMemMacroCacheIOStat() { reset(); }

  void reset() { reuse(); }
  void reuse() { io_param_.reset(); }
  void update_io_stat_info(const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes) 
  {
    io_param_.update_io_stat_param(is_read, delta_cnt, delta_bytes);
  }

  TO_STRING_KV(K_(io_param));
public:
  ObSSMemMacroCacheIOStatParam io_param_;
};

struct ObSSMemMacroCacheMemUsageStat
{
public:
  ObSSMemMacroCacheMemUsageStat() { reset(); }
  ~ObSSMemMacroCacheMemUsageStat() { reset(); }
  void reset()
  {
    reuse();
  }
  void reuse()
  {
    macro_meta_cnt_ = 0;
    macro_meta_usage_ = 0;
  }

  void update_macro_meta_usage(const int64_t delta_cnt, const int64_t usage_size)
  {
    ATOMIC_AAF(&macro_meta_cnt_, delta_cnt);
    ATOMIC_STORE(&macro_meta_usage_, usage_size);
  }

  TO_STRING_KV(K_(macro_meta_cnt), K_(macro_meta_usage));

public:
  int64_t macro_meta_cnt_;
  int64_t macro_meta_usage_;
};

struct ObSSMemMacroCacheStat
{
public:
  ObSSMemMacroCacheStat() { reset(); }
  ~ObSSMemMacroCacheStat() { reset(); }

  void reset()
  {
    hit_stat_.reset();
    macro_blk_stat_.reset();
    mem_blk_stat_.reset();
    io_stat_.reset();
    mem_usage_stat_.reset();
  }
  void reuse()
  {
    hit_stat_.reuse();
    macro_blk_stat_.reuse();
    mem_blk_stat_.reuse();
    io_stat_.reuse();
    mem_usage_stat_.reuse();
  }

  ObSSMemMacroCacheHitStat &hit_stat() { return hit_stat_; }
  ObSSMemMacroCacheMacroBlkStat &macro_blk_stat() { return macro_blk_stat_; }
  ObSSMemMacroCacheMemBlkStat &mem_blk_stat() { return mem_blk_stat_; }
  ObSSMemMacroCacheIOStat &io_stat() { return io_stat_; }
  ObSSMemMacroCacheMemUsageStat &mem_usage_stat() { return mem_usage_stat_; }

  TO_STRING_KV(K_(hit_stat), K_(macro_blk_stat), K_(mem_blk_stat), K_(io_stat), K_(mem_usage_stat));

public:
  ObSSMemMacroCacheHitStat hit_stat_;
  ObSSMemMacroCacheMacroBlkStat macro_blk_stat_;
  ObSSMemMacroCacheMemBlkStat mem_blk_stat_;
  ObSSMemMacroCacheIOStat io_stat_;
  ObSSMemMacroCacheMemUsageStat mem_usage_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_STAT_H_ */
