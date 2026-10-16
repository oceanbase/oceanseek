/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_

#include <stdint.h>
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

struct ObSSMicroCacheIOStatParam
{
public:
  int64_t add_cnt_;
  int64_t add_bytes_;
  int64_t get_cnt_;
  int64_t get_bytes_;

  ObSSMicroCacheIOStatParam() { reset(); }
  void reset()
  {
    add_cnt_ = 0;
    add_bytes_ = 0;
    get_cnt_ = 0;
    get_bytes_ = 0;
  }
  void inc_io_stat_param(const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes)
  {
    if (is_read) {
      inc_get_info(delta_cnt, delta_bytes);
    } else {
      inc_add_info(delta_cnt, delta_bytes);
    }
  }
  void inc_add_info(const int64_t delta_cnt, const int64_t delta_bytes)
  {
    ATOMIC_AAF(&add_cnt_, delta_cnt);
    ATOMIC_AAF(&add_bytes_, delta_bytes);
  }
  void inc_get_info(const int64_t delta_cnt, const int64_t delta_bytes)
  {
    ATOMIC_AAF(&get_cnt_, delta_cnt);
    ATOMIC_AAF(&get_bytes_, delta_bytes);
  }
  int64_t add_cnt() const { return ATOMIC_LOAD(&add_cnt_); }
  int64_t add_bytes() const { return ATOMIC_LOAD(&add_bytes_); }
  int64_t get_cnt() const { return ATOMIC_LOAD(&get_cnt_); }
  int64_t get_bytes() const { return ATOMIC_LOAD(&get_bytes_); }

  TO_STRING_KV(K_(add_cnt), K_(add_bytes), K_(get_cnt), K_(get_bytes));
public:
  static const int64_t OB_SS_MICRO_CACHE_IO_STAT_PARAM_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_IO_STAT_PARAM_VERSION);
};

/*
 *  To shows the upper layer's IO request pressure, include add_op and get_op
 *  Also, we will distinguish io type into two types: common_io & prewarm_io;
 */
struct ObSSMicroCacheIOStat
{
public:
  ObSSMicroCacheIOStatParam common_io_param_;
  ObSSMicroCacheIOStatParam prewarm_io_param_;
  ObSSMicroCacheIOStat() { reset(); }
  void reset()
  {
    common_io_param_.reset();
    prewarm_io_param_.reset();
  }

  void update_io_stat_info(const bool is_prewarm, const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes) 
  { 
    if (is_prewarm) {
      prewarm_io_param_.inc_io_stat_param(is_read, delta_cnt, delta_bytes);
    } else {
      common_io_param_.inc_io_stat_param(is_read, delta_cnt, delta_bytes);
    }
  }

  int64_t get_add_cnt() const
  {
    return common_io_param_.add_cnt_ + prewarm_io_param_.add_cnt_;
  }

  TO_STRING_KV(K_(common_io_param), K_(prewarm_io_param));
public:
  static const int64_t OB_SS_MICRO_CACHE_IO_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_IO_STAT_VERSION);
};

/*
 * Some detailed stat of common io
 */
struct ObSSMicroCacheHitStat
{
public:
  int64_t cache_hit_cnt_;
  int64_t cache_hit_bytes_;
  int64_t cache_miss_cnt_;
  int64_t cache_miss_bytes_;
  int64_t fail_get_cnt_;
  int64_t fail_add_cnt_; // for common_io
  int64_t new_add_cnt_; // for common_io
  int64_t reject_add_cnt_; // count of micro blocks rejected due to large size in common_io
  int64_t reject_get_cnt_; // count of micro blocks rejected due to large size when reading in common_io
  int64_t hot_micro_lack_cnt_;
private:
  int64_t temp_hot_micro_lack_cnt_;

public:
  ObSSMicroCacheHitStat() { reset(); }
  void reset()
  {
    cache_hit_cnt_ = 0;
    cache_hit_bytes_ = 0;
    cache_miss_cnt_ = 0;
    cache_miss_bytes_ = 0;
    fail_get_cnt_ = 0;
    fail_add_cnt_ = 0;
    new_add_cnt_ = 0;
    reject_add_cnt_ = 0;
    reject_get_cnt_ = 0;
    hot_micro_lack_cnt_ = 0;
    temp_hot_micro_lack_cnt_ = 0;
  }

  OB_INLINE int64_t get_fail_add_cnt() const { return ATOMIC_LOAD(&fail_add_cnt_); }
  OB_INLINE int64_t get_new_add_cnt() const { return ATOMIC_LOAD(&new_add_cnt_); }

  void update_cache_hit(const int64_t delta_cnt, const int64_t delta_size) 
  { 
    ATOMIC_AAF(&cache_hit_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_hit_bytes_, delta_size);
  }
  void update_cache_miss(const int64_t delta_cnt, const int64_t delta_size) 
  { 
    ATOMIC_AAF(&cache_miss_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_miss_bytes_, delta_size);
  }
  void update_fail_get_cnt(const int64_t delta_cnt) 
  { 
    ATOMIC_AAF(&fail_get_cnt_, delta_cnt);
  }
  void update_fail_add_cnt(const int64_t delta_cnt) 
  { 
    ATOMIC_AAF(&fail_add_cnt_, delta_cnt);
  }
  void update_new_add_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&new_add_cnt_, delta_cnt);
  }
  void update_reject_add_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&reject_add_cnt_, delta_cnt);
  }
  void update_reject_get_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&reject_get_cnt_, delta_cnt);
  }

  void update_hot_micro_lack_cnt_info(const bool update_formal_val, const int64_t lack_cnt_delta)
  {
    if (lack_cnt_delta >= 0) {
      const int64_t temp_val = ATOMIC_AAF(&temp_hot_micro_lack_cnt_, lack_cnt_delta);
      if (update_formal_val) {
        ATOMIC_STORE(&hot_micro_lack_cnt_, temp_val);
        ATOMIC_STORE(&temp_hot_micro_lack_cnt_, 0);
      }
    }
  }

  TO_STRING_KV(K_(cache_hit_cnt), K_(cache_hit_bytes), K_(cache_miss_cnt), K_(cache_miss_bytes), K_(fail_get_cnt), 
      K_(fail_add_cnt), K_(new_add_cnt), K_(reject_add_cnt), K_(hot_micro_lack_cnt), K_(temp_hot_micro_lack_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_HIT_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_HIT_STAT_VERSION);
};

/*
 * Some detailed info of prewarm io
 */
struct ObSSMicroCachePrewarmStat
{
public:
  int64_t new_add_cnt_;
  int64_t fail_add_cnt_;
  int64_t reject_add_cnt_; // count of micro blocks rejected due to large size in prewarm_io
  int64_t reject_get_cnt_; // count of micro blocks rejected due to large size when reading in prewarm_io
  ObSSMicroCacheIOStatParam major_compaction_pwm_stat_;
  ObSSMicroCacheIOStatParam migration_pwm_stat_;
  ObSSMicroCacheIOStatParam replica_pwm_stat_;
  ObSSMicroCacheIOStatParam ddl_pwm_stat_;
  ObSSMicroCacheIOStatParam scp_pwm_stat_;
  ObSSMicroCacheIOStatParam minor_compaction_pwm_stat_;
  ObSSMicroCacheIOStatParam preread_task_pwm_stat_;
  ObSSMicroCacheIOStatParam split_part_pwm_stat_;
  ObSSMicroCacheIOStatParam try_write_lcache_pwm_stat_;

  ObSSMicroCachePrewarmStat() { reset(); }
  void reset()
  {
    new_add_cnt_ = 0;
    fail_add_cnt_ = 0;
    reject_add_cnt_ = 0;
    reject_get_cnt_ = 0;
    major_compaction_pwm_stat_.reset();
    migration_pwm_stat_.reset();
    replica_pwm_stat_.reset();
    ddl_pwm_stat_.reset();
    scp_pwm_stat_.reset();
    minor_compaction_pwm_stat_.reset();
    preread_task_pwm_stat_.reset();
    split_part_pwm_stat_.reset();
    try_write_lcache_pwm_stat_.reset();
  }

  void update_fail_add_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&fail_add_cnt_, delta_cnt);
  }
  void update_reject_add_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&reject_add_cnt_, delta_cnt);
  }
  void update_reject_get_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&reject_get_cnt_, delta_cnt);
  }

  OB_INLINE int64_t get_fail_add_cnt() const { return ATOMIC_LOAD(&fail_add_cnt_); }
  void update_add_info(const ObSSMicroCacheAccessInfo &access_info, const bool first_add)
  {
    const ObSSMicroCacheAccessType access_type = access_info.access_type_;
    const int64_t delta_cnt = access_info.delta_cnt_;
    const int64_t delta_size = access_info.delta_size_;
    switch (access_type) {
      case ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE: {
        major_compaction_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::MIGRATION_PREWARM_TYPE: {
        migration_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE: {
        replica_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::DDL_PREWARM_TYPE: {
        ddl_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE: {
        scp_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::MINOR_COMPACTION_PREWARM_TYPE: {
        minor_compaction_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::PREREAD_TASK_PREWARM_TYPE: {
        preread_task_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE: {
        split_part_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::TRY_WRITE_LCACHE_PREWARM_TYPE: {
        try_write_lcache_pwm_stat_.inc_add_info(delta_cnt, delta_size);
        break;
      }
      default: {
        break;
      }
    }
    if (first_add) {
      ATOMIC_AAF(&new_add_cnt_, delta_cnt);
    }
  }

  void update_get_info(const ObSSMicroCacheAccessInfo &access_info)
  {
    const ObSSMicroCacheAccessType access_type = access_info.access_type_;
    const int64_t delta_cnt = access_info.delta_cnt_;
    const int64_t delta_size = access_info.delta_size_;
    switch (access_type) {
      case ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE: {
        major_compaction_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::MIGRATION_PREWARM_TYPE: {
        migration_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE: {
        replica_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::DDL_PREWARM_TYPE: {
        ddl_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE: {
        scp_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::MINOR_COMPACTION_PREWARM_TYPE: {
        minor_compaction_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::PREREAD_TASK_PREWARM_TYPE: {
        preread_task_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE: {
        split_part_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::TRY_WRITE_LCACHE_PREWARM_TYPE: {
        try_write_lcache_pwm_stat_.inc_get_info(delta_cnt, delta_size);
        break;
      }
      default: {
        break;
      }
    }
  }

  void get_prewarm_add_cnt(const ObSSMicroCacheAccessType access_type, int64_t &add_cnt) const
  {
    switch (access_type) {
      case ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE: {
        add_cnt = major_compaction_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::MIGRATION_PREWARM_TYPE: {
        add_cnt = migration_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE: {
        add_cnt = replica_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::DDL_PREWARM_TYPE: {
        add_cnt = ddl_pwm_stat_.add_cnt();
        break;
      } 
      case ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE: {
        add_cnt = scp_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::MINOR_COMPACTION_PREWARM_TYPE: {
        add_cnt = minor_compaction_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::PREREAD_TASK_PREWARM_TYPE: {
        add_cnt = preread_task_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE: {
        add_cnt = split_part_pwm_stat_.add_cnt();
        break;
      }
      case ObSSMicroCacheAccessType::TRY_WRITE_LCACHE_PREWARM_TYPE: {
        add_cnt = try_write_lcache_pwm_stat_.add_cnt();
        break;
      }
      default: {
        break;  
      }
    }
  }

  TO_STRING_KV(K_(new_add_cnt), K_(fail_add_cnt), K_(reject_add_cnt), K_(reject_get_cnt),
    K_(major_compaction_pwm_stat), K_(migration_pwm_stat), K_(replica_pwm_stat), 
    K_(ddl_pwm_stat), K_(scp_pwm_stat), K_(minor_compaction_pwm_stat), K_(preread_task_pwm_stat),
    K_(split_part_pwm_stat), K_(try_write_lcache_pwm_stat));

public:
  static const int64_t OB_SS_MICRO_CACHE_PREWARM_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_PREWARM_STAT_VERSION);
};

struct ObSSMicroCachePhyBlkStat
{
public:
  int32_t block_size_;
  int64_t cache_file_size_;
  int64_t total_blk_cnt_;
  int64_t super_blk_cnt_;
  int64_t reusable_blk_cnt_;
  int64_t sparse_blk_cnt_;
  int64_t phy_ckpt_blk_cnt_;
  int64_t phy_ckpt_blk_used_cnt_;
  int64_t meta_blk_cnt_;
  int64_t meta_blk_used_cnt_;
  int64_t data_blk_cnt_;
  int64_t data_blk_used_cnt_;
  int64_t shared_blk_cnt_;
  int64_t shared_blk_used_cnt_;
  int64_t reorgan_blk_used_cnt_;

  ObSSMicroCachePhyBlkStat() { reset(); }
  void reset() 
  { 
    block_size_ = 0;
    cache_file_size_ = 0; 
    total_blk_cnt_ = 0;
    super_blk_cnt_ = 0;
    shared_blk_cnt_ = 0;
    meta_blk_cnt_ = 0;
    data_blk_cnt_ = 0;
    phy_ckpt_blk_cnt_ = 0;
    shared_blk_used_cnt_ = 0;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    phy_ckpt_blk_used_cnt_ = 0;
    meta_blk_used_cnt_ = 0;
    data_blk_used_cnt_ = 0;
    reusable_blk_cnt_ = 0;
    sparse_blk_cnt_ = 0;
    reorgan_blk_used_cnt_ = 0;
  }
  void set_cache_file_size(const int64_t size)
  {
    ATOMIC_STORE(&cache_file_size_, size);
  }
  void set_block_size(const int64_t block_size)
  {
    ATOMIC_STORE(&block_size_, block_size);
  }
  void set_super_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&super_blk_cnt_, cnt);
  }
  void set_total_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&total_blk_cnt_, cnt);
  }
  void set_reusable_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&reusable_blk_cnt_, cnt);
  }
  void set_sparse_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&sparse_blk_cnt_, cnt);
  }
  void set_shared_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&shared_blk_cnt_, cnt);
  }
  void set_shared_block_used_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&shared_blk_used_cnt_, cnt);
  }
  void update_shared_block_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&shared_blk_used_cnt_, delta);
  }
  void set_phy_ckpt_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&phy_ckpt_blk_cnt_, cnt);
  }
  void update_phy_ckpt_block_used_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&phy_ckpt_blk_used_cnt_, delta);
  }
  void set_data_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&data_blk_cnt_, cnt);
  }
  void update_data_block_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&data_blk_cnt_, delta);
  }
  void update_data_block_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&data_blk_used_cnt_, delta);
  }
  void set_meta_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&meta_blk_cnt_, cnt);
  }
  void update_meta_block_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&meta_blk_cnt_, delta);
  }
  void update_meta_block_used_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&meta_blk_used_cnt_, delta);
  }
  void update_reorgan_block_used_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&reorgan_blk_used_cnt_, delta);
  }
  void try_dec_reorgan_block_used_cnt()
  {
    bool is_finish = true;
    do {
      is_finish = true;
      int64_t cur_used_cnt = ATOMIC_LOAD(&reorgan_blk_used_cnt_);
      if (cur_used_cnt > 0) {
        is_finish = ATOMIC_BCAS(&reorgan_blk_used_cnt_, cur_used_cnt, cur_used_cnt - 1);
      }
    } while (!is_finish);
  }
  int64_t get_used_disk_size() const
  {
    const int64_t cur_used_blk_cnt = ATOMIC_LOAD(&super_blk_cnt_) + ATOMIC_LOAD(&phy_ckpt_blk_used_cnt_) +
                                     ATOMIC_LOAD(&meta_blk_used_cnt_) + ATOMIC_LOAD(&data_blk_used_cnt_);
    const int64_t used_disk_size = cur_used_blk_cnt * block_size_;
    return used_disk_size;
  }

  TO_STRING_KV(K_(cache_file_size), K_(block_size), K_(total_blk_cnt), K_(super_blk_cnt), K_(shared_blk_cnt),
      K_(shared_blk_used_cnt), K_(phy_ckpt_blk_cnt), K_(phy_ckpt_blk_used_cnt), K_(meta_blk_cnt), K_(meta_blk_used_cnt),
      K_(data_blk_cnt), K_(data_blk_used_cnt), K_(reusable_blk_cnt), K_(sparse_blk_cnt), K_(reorgan_blk_used_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_PHY_BLK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_PHY_BLK_STAT_VERSION);
};

struct ObSSMicroCacheMemBlkStat
{
public:
  int64_t mem_blk_fg_max_cnt_;
  int64_t mem_blk_fg_used_cnt_;
  int64_t mem_blk_bg_max_cnt_;
  int64_t mem_blk_bg_used_cnt_;

  ObSSMicroCacheMemBlkStat() { reset(); }
  void reset()
  {
    mem_blk_fg_max_cnt_ = 0;
    mem_blk_bg_max_cnt_ = 0;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    mem_blk_fg_used_cnt_ = 0;
    mem_blk_bg_used_cnt_ = 0;
  }
  bool check_abnormal() const
  {
    bool is_abnormal = false;
    if ((ATOMIC_LOAD(&mem_blk_fg_used_cnt_) < 0) || (ATOMIC_LOAD(&mem_blk_bg_used_cnt_) < 0)) {
      is_abnormal = true;
    }
    return is_abnormal;
  }
  void set_mem_blk_max_cnt(const int64_t fg_max_cnt, const int64_t bg_max_cnt)
  { 
    ATOMIC_STORE(&mem_blk_fg_max_cnt_, fg_max_cnt);
    ATOMIC_STORE(&mem_blk_bg_max_cnt_, bg_max_cnt);
  }
  void update_mem_blk_fg_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&mem_blk_fg_used_cnt_, delta);
  }
  void update_mem_blk_bg_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&mem_blk_bg_used_cnt_, delta);
  }
  int64_t get_total_mem_blk_used_cnt() 
  {
    return ATOMIC_LOAD(&mem_blk_fg_used_cnt_) + ATOMIC_LOAD(&mem_blk_bg_used_cnt_);
  }
  TO_STRING_KV(K_(mem_blk_fg_max_cnt), K_(mem_blk_fg_used_cnt), K_(mem_blk_bg_max_cnt), K_(mem_blk_bg_used_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_MEM_BLK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_MEM_BLK_STAT_VERSION);
};

struct ObSSMicroCacheMicroStat
{
public:
  int64_t total_micro_cnt_;// total count of micro in micro_map
  int64_t total_micro_size_;// total size of micro in micro_map
  int64_t valid_micro_cnt_; // total count of micro in micro_map which belongs to T1/T2
  int64_t valid_micro_size_;// total size of micro in micro_map which belongs to T1/T2
  int64_t mark_del_micro_cnt_;// total count of micro which is mark deleted in micro_map
  int64_t mark_del_micro_size_;
  int64_t mark_del_ghost_micro_cnt_;// total count of micro which is mark deleted in B1/B2 in micro_map
  int64_t mark_del_ghost_micro_size_;
  int64_t pending_delete_tablet_cnt_; // count of tablet_id that are pending for deletion
  int64_t max_micro_meta_cnt_; // Maximum micro_meta count that map has ever contained

  ObSSMicroCacheMicroStat() { reset(); }
  void reset_meta_dynamic_stats()
  {
    total_micro_cnt_ = 0;
    total_micro_size_ = 0;
    valid_micro_cnt_ = 0;
    valid_micro_size_ = 0;
    mark_del_micro_cnt_ = 0;
    mark_del_micro_size_ = 0;
    mark_del_ghost_micro_cnt_ = 0;
    mark_del_ghost_micro_size_ = 0;
  }
  void reset()
  {
    reset_meta_dynamic_stats();
    pending_delete_tablet_cnt_ = 0;
    max_micro_meta_cnt_ = 0;
  }
  bool check_abnormal() const
  {
    return (ATOMIC_LOAD(&total_micro_cnt_) < 0) || (ATOMIC_LOAD(&total_micro_size_) < 0) ||
           (ATOMIC_LOAD(&valid_micro_cnt_) < 0) || (ATOMIC_LOAD(&valid_micro_size_) < 0) ||
           (ATOMIC_LOAD(&mark_del_micro_cnt_) < 0) || (ATOMIC_LOAD(&mark_del_micro_size_) < 0) ||
           (ATOMIC_LOAD(&mark_del_ghost_micro_cnt_) < 0) || (ATOMIC_LOAD(&mark_del_ghost_micro_size_) < 0);
  }
  int64_t get_avg_micro_size() const
  {
    const int64_t valid_cnt = ATOMIC_LOAD(&valid_micro_cnt_);
    const int64_t valid_size = ATOMIC_LOAD(&valid_micro_size_);
    const int64_t avg_size = (valid_cnt > 0 ? (valid_size / valid_cnt) : 0);
    return avg_size;
  }
  void update_micro_map_valid_info(const int64_t valid_micro_cnt, const int64_t valid_micro_size)
  {
    ATOMIC_STORE(&valid_micro_cnt_, valid_micro_cnt);
    ATOMIC_STORE(&valid_micro_size_, valid_micro_size);
  }
  void update_mark_del_micro_info(const bool is_in_ghost, const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&mark_del_micro_cnt_, delta_cnt);
    ATOMIC_AAF(&mark_del_micro_size_, delta_size);
    if (is_in_ghost) {
      ATOMIC_AAF(&mark_del_ghost_micro_cnt_, delta_cnt);
      ATOMIC_AAF(&mark_del_ghost_micro_size_, delta_size);
    }
  }
  void update_micro_map_total_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&total_micro_size_, delta_size);
    ATOMIC_AAF(&total_micro_cnt_, delta_cnt);
  }
  void set_pending_delete_tablet_cnt(const int64_t tablet_cnt)
  {
    ATOMIC_STORE(&pending_delete_tablet_cnt_, tablet_cnt);
  }
  void set_max_micro_meta_cnt(const int64_t micro_meta_cnt)
  {
    if (micro_meta_cnt > ATOMIC_LOAD(&max_micro_meta_cnt_)) {
      ATOMIC_STORE(&max_micro_meta_cnt_, micro_meta_cnt);
    }
  }
  void set_total_micro_info(const int64_t cnt, const int64_t size)
  {
    ATOMIC_STORE(&total_micro_cnt_, cnt);
    ATOMIC_STORE(&total_micro_size_, size);
  }
  void set_mark_del_micro_info(const int64_t cnt, const int64_t size)
  {
    ATOMIC_STORE(&mark_del_micro_cnt_, cnt);
    ATOMIC_STORE(&mark_del_micro_size_, size);
  }
  void set_mark_del_ghost_micro_info(const int64_t cnt, const int64_t size)
  {
    ATOMIC_STORE(&mark_del_ghost_micro_cnt_, cnt);
    ATOMIC_STORE(&mark_del_ghost_micro_size_, size);
  }
  int64_t get_valid_micro_size() const { return ATOMIC_LOAD(&valid_micro_size_); }
  int64_t get_valid_micro_cnt() const { return ATOMIC_LOAD(&valid_micro_cnt_); }
  int64_t get_total_micro_cnt() const { return ATOMIC_LOAD(&total_micro_cnt_); }
  int64_t get_deleted_micro_cnt() const { return ATOMIC_LOAD(&mark_del_micro_cnt_); }
  int64_t get_ghost_micro_cnt() const { return get_total_micro_cnt() - get_valid_micro_cnt() - get_deleted_micro_cnt(); }
  int64_t get_pending_delete_tablet_cnt() const { return ATOMIC_LOAD(&pending_delete_tablet_cnt_); }
  
  TO_STRING_KV(K_(total_micro_cnt), K_(total_micro_size), K_(valid_micro_cnt), K_(valid_micro_size),
      K_(mark_del_micro_cnt), K_(mark_del_micro_size), K_(mark_del_ghost_micro_cnt), K_(mark_del_ghost_micro_size),
      K_(pending_delete_tablet_cnt), K_(max_micro_meta_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_MICRO_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_MICRO_STAT_VERSION);
};

struct ObSSMicroCacheRangeStat
{
public:
  int64_t init_range_cnt_;
  int64_t sub_range_cnt_;
  int64_t fe_sub_range_cnt_; // count of sub_range which contains erased persisted ghost micro_meta

  ObSSMicroCacheRangeStat() { reset(); }
  void reset() { init_range_cnt_ = 0; reset_dynamic_info(); }
  void reset_dynamic_info() 
  { 
    sub_range_cnt_ = 0;
    fe_sub_range_cnt_ = 0;
  }

  void set_init_cnt(const int64_t init_cnt) { ATOMIC_STORE(&init_range_cnt_, init_cnt); }
  void set_sub_range_cnt(const int64_t sub_range_cnt) { ATOMIC_STORE(&sub_range_cnt_, sub_range_cnt); }
  void update_sub_range_cnt(const int64_t delta) { ATOMIC_AAF(&sub_range_cnt_, delta); }
  void update_fe_sub_range_cnt(const int64_t delta) { ATOMIC_AAF(&fe_sub_range_cnt_, delta); }

  TO_STRING_KV(K_(init_range_cnt), K_(sub_range_cnt), K_(fe_sub_range_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_RANGE_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_RANGE_STAT_VERSION);
};

struct ObSSMicroCacheTaskStat
{
public:
  bool is_mini_mode_;
  uint32_t reorgan_invoke_time_;
  int64_t reorgan_cnt_;
  int64_t reorgan_free_blk_cnt_;
  int64_t t1_evict_cnt_; // total count of T1 micro_meta evicted from memory
  int64_t t2_evict_cnt_; // total count of T2 micro_meta evicted from memory
  int64_t b1_delete_cnt_; // total count of B1 micro_meta deleted from memory
  int64_t b2_delete_cnt_; // total count of B2 micro_meta deleted from memory
  int64_t b1_persisted_delete_cnt_; // total count of B1 micro_meta deleted from disk
  int64_t b2_persisted_delete_cnt_; // total count of B2 micro_meta deleted from disk
  int64_t blk_ckpt_cnt_;
  int64_t blk_ckpt_item_cnt_; // latest completed blk_ckpt persisted item count
  int64_t micro_ckpt_op_cnt_; // micro_meta ckpt operation count
  int64_t micro_ckpt_cnt_; // full micro_meta ckpt count
  int64_t micro_ckpt_op_item_cnt_; // latest unfull micro_ckpt persisted item count
  int64_t micro_ckpt_item_cnt_; // latest completed full micro_ckpt persisted item count
  int32_t phy_blk_min_len_;
  int64_t expired_cnt_; // total expired micro count of T1/T2
  int64_t expired_ghost_cnt_; // total expired micro count of B1/B2
  int64_t force_erase_cnt_; // total force erase micro count of T1/T2
  int64_t force_erase_ghost_cnt_; // total force erase micro count of B1/B2
  int64_t erase_persisted_cnt_; // total count of persisted ghost micro_meta which was moved from mem to disk
  int64_t readd_persisted_cnt_; // total count of persisted ghost micro_meta which was moved from disk to mem
  int64_t dropped_persisted_cnt_; // total count of persisted ghost micro_meta which was dropped cuz by meta_ckpt failure
  int64_t entry_exist_persisted_cnt_; // total count of persisted ghost micro_meta which was already in memory when loading from disk
  int64_t task_start_time_us_arr_[SS_MICRO_CACHE_TASK_OP_TYPE_CNT]; // task start time for each task op type
  uint64_t abnormal_flag_;      // Atomic bitmap for abnormal status flags
  int64_t calibration_cnt_; // total calibration count (incremented on successful completion)

  ObSSMicroCacheTaskStat() { reset(); }
  void reset()
  {
    is_mini_mode_ = true;
    reorgan_invoke_time_ = 0;
    reorgan_cnt_ = 0; 
    reorgan_free_blk_cnt_ = 0; 
    t1_evict_cnt_ = 0;
    t2_evict_cnt_ = 0;
    b1_delete_cnt_ = 0;
    b2_delete_cnt_ = 0;
    b1_persisted_delete_cnt_ = 0;
    b2_persisted_delete_cnt_ = 0;
    blk_ckpt_cnt_ = 0; 
    blk_ckpt_item_cnt_ = 0;
    micro_ckpt_op_cnt_ = 0;
    micro_ckpt_cnt_ = 0;
    micro_ckpt_op_item_cnt_ = 0;
    micro_ckpt_item_cnt_ = 0;
    phy_blk_min_len_ = 0;
    expired_cnt_ = 0;
    expired_ghost_cnt_ = 0;
    force_erase_cnt_ = 0;
    force_erase_ghost_cnt_ = 0;
    erase_persisted_cnt_ = 0;
    readd_persisted_cnt_ = 0;
    dropped_persisted_cnt_ = 0;
    entry_exist_persisted_cnt_ = 0;
    MEMSET(task_start_time_us_arr_, 0, sizeof(task_start_time_us_arr_));
    abnormal_flag_ = 0;
    calibration_cnt_ = 0;
  }
  void set_mini_mode(const bool is_mini_mode)
  {
    ATOMIC_STORE(&is_mini_mode_, is_mini_mode);
  }
  void update_reorgan_invoke_time(const uint32_t cur_time_s) 
  {
    ATOMIC_STORE(&reorgan_invoke_time_, cur_time_s);
  }
  void update_reorgan_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&reorgan_cnt_, delta);
  }
  OB_INLINE void update_reorgan_free_blk_cnt(const int64_t delta) { ATOMIC_AAF(&reorgan_free_blk_cnt_, delta); }
  void update_blk_ckpt_cnt(const int64_t delta, const int64_t item_cnt)
  { 
    ATOMIC_AAF(&blk_ckpt_cnt_, delta);
    ATOMIC_STORE(&blk_ckpt_item_cnt_, item_cnt);
  }
  void update_micro_ckpt_op_cnt(const int64_t delta, const int64_t item_cnt) 
  { 
    ATOMIC_AAF(&micro_ckpt_op_cnt_, delta);
    ATOMIC_STORE(&micro_ckpt_op_item_cnt_, item_cnt);
  }
  void update_micro_ckpt_cnt(const int64_t delta, const int64_t item_cnt) 
  { 
    ATOMIC_AAF(&micro_ckpt_cnt_, delta);
    ATOMIC_STORE(&micro_ckpt_item_cnt_, item_cnt);
  }
  void update_arc_evict_cnt(const bool is_in_l1, const bool is_in_ghost, const int64_t delta) 
  {
    if (is_in_l1 && !is_in_ghost) { // T1
      ATOMIC_AAF(&t1_evict_cnt_, delta); 
    } else if (!is_in_l1 && !is_in_ghost) { // T2
      ATOMIC_AAF(&t2_evict_cnt_, delta); 
    }
  }
  void update_expired_cnt(const bool is_in_ghost, const int64_t delta)
  {
    if (is_in_ghost) {
      ATOMIC_AAF(&expired_ghost_cnt_, delta); 
    } else {
      ATOMIC_AAF(&expired_cnt_, delta);
    }
  }
  void update_force_erase_cnt(const bool is_in_ghost, const int64_t delta)
  {
    if (is_in_ghost) {
      ATOMIC_AAF(&force_erase_ghost_cnt_, delta); 
    } else {
      ATOMIC_AAF(&force_erase_cnt_, delta);
    }
  }
  void set_erase_persisted_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&erase_persisted_cnt_, cnt);
  }
  void update_erase_persisted_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&erase_persisted_cnt_, delta);
  }
  bool exist_erase_persisted_micro() 
  {
    return (erase_persisted_cnt_ > 0);
  }
  void update_readd_persisted_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&readd_persisted_cnt_, delta);
  }
  void update_dropped_persisted_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&dropped_persisted_cnt_, delta);
  }
  void update_entry_exist_persisted_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&entry_exist_persisted_cnt_, delta);
  }
  bool exist_arc_evict() const
  {
    return ((t1_evict_cnt_ > 0) || (t2_evict_cnt_ > 0));
  }
  void update_arc_delete_cnt(const bool is_in_l1, const bool is_in_ghost, const int64_t delta) 
  { 
    if (is_in_l1 && is_in_ghost) { // B1
      ATOMIC_AAF(&b1_delete_cnt_, delta); 
    } else if (!is_in_l1 && is_in_ghost) { // B2
      ATOMIC_AAF(&b2_delete_cnt_, delta); 
    }
  }
  void update_arc_persisted_delete_cnt(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_cnt) 
  { 
    if (is_in_l1 && is_in_ghost) { // B1
      ATOMIC_AAF(&b1_persisted_delete_cnt_, delta_cnt); 
    } else if (!is_in_l1 && is_in_ghost) { // B2
      ATOMIC_AAF(&b2_persisted_delete_cnt_, delta_cnt); 
    }
  }
  bool exist_arc_delete() const
  {
    return ((b1_delete_cnt_ > 0) || (b2_delete_cnt_ > 0));
  }
  bool exist_arc_evict_or_delete() const
  {
    return exist_arc_evict() || exist_arc_delete();
  }
  void set_phy_blk_min_len(const int32_t min_len) 
  {
    ATOMIC_STORE(&phy_blk_min_len_, min_len);
  }
  int64_t get_total_evict_cnt() const
  {
    return (ATOMIC_LOAD(&t1_evict_cnt_) + ATOMIC_LOAD(&t2_evict_cnt_));
  }
  int64_t get_total_delete_cnt() const
  {
    return (ATOMIC_LOAD(&b1_delete_cnt_) + ATOMIC_LOAD(&b2_delete_cnt_));
  }
  int64_t get_total_persisted_delete_cnt() const
  {
    return (ATOMIC_LOAD(&b1_persisted_delete_cnt_) + ATOMIC_LOAD(&b2_persisted_delete_cnt_));
  }
  int64_t get_micro_ckpt_cnt() const { return ATOMIC_LOAD(&micro_ckpt_cnt_); }
  int64_t get_micro_ckpt_op_cnt() const { return ATOMIC_LOAD(&micro_ckpt_op_cnt_); }
  void update_task_start_time(const ObSSMicroCacheTaskOpType op_type, const int64_t start_time_us)
  {
    const int64_t idx = static_cast<int64_t>(op_type);
    if (idx >= 0 && idx < SS_MICRO_CACHE_TASK_OP_TYPE_CNT) {
      ATOMIC_STORE(&task_start_time_us_arr_[idx], start_time_us);
    }
  }
  bool is_task_abnormal(const int64_t timeout_us) const
  {
    bool is_abnormal = false;
    for (int64_t i = 0; (i < SS_MICRO_CACHE_TASK_OP_TYPE_CNT) && !is_abnormal; ++i) {
      const int64_t start_time_us = ATOMIC_LOAD(&task_start_time_us_arr_[i]);
      if (start_time_us > 0 && (ObTimeUtility::current_time_us() - start_time_us > timeout_us)) {
        is_abnormal = true;
      }
    }
    return is_abnormal;
  }
  // Fill buf with comma-separated stuck task names.
  int get_stuck_task_names(const int64_t timeout_us, char *buf, const int64_t buf_len) const;
  uint64_t get_abnormal_flag() const { return ATOMIC_LOAD(&abnormal_flag_); }
  void set_abnormal_flag(const uint64_t flag) { ATOMIC_STORE(&abnormal_flag_, flag); }
  void clear_abnormal_flag() { ATOMIC_STORE(&abnormal_flag_, 0); }
  void add_abnormal_flag(const uint64_t flag)
  {
    uint64_t old_val = ATOMIC_LOAD(&abnormal_flag_);
    uint64_t new_val = old_val | flag;
    while (old_val != new_val && !ATOMIC_BCAS(&abnormal_flag_, old_val, new_val)) {
      old_val = ATOMIC_LOAD(&abnormal_flag_);
      new_val = old_val | flag;
    }
  }
  bool is_abnormal() const { return (ATOMIC_LOAD(&abnormal_flag_) != 0); }

  int64_t get_calibration_cnt() const { return ATOMIC_LOAD(&calibration_cnt_); }
  void add_calibration_cnt(const int64_t delta) { ATOMIC_AAF(&calibration_cnt_, delta); }
  void clear_calibration_cnt() { ATOMIC_STORE(&calibration_cnt_, 0); }

public:
  TO_STRING_KV(K_(is_mini_mode), K_(reorgan_invoke_time), K_(reorgan_cnt), K_(reorgan_free_blk_cnt), K_(blk_ckpt_cnt),
    K_(blk_ckpt_item_cnt), K_(micro_ckpt_op_cnt), K_(micro_ckpt_cnt), K_(micro_ckpt_op_item_cnt), 
    K_(micro_ckpt_item_cnt), K_(phy_blk_min_len), K_(t1_evict_cnt), K_(t2_evict_cnt), K_(b1_delete_cnt), 
    K_(b2_delete_cnt), K_(b1_persisted_delete_cnt), K_(b2_persisted_delete_cnt), 
    K_(expired_cnt), K_(expired_ghost_cnt), K_(force_erase_cnt), K_(force_erase_ghost_cnt),
    K_(erase_persisted_cnt), K_(readd_persisted_cnt), K_(dropped_persisted_cnt), K_(entry_exist_persisted_cnt),
    K_(abnormal_flag));

public:
  static constexpr uint64_t FLAG_WRITE_STUCK = 0x01;  // write stuck
  static constexpr uint64_t FLAG_INVALID_STAT = 0x02; // arc_info or cache_stat invalid
  static constexpr uint64_t FLAG_TASK_STUCK = 0x04;   // background task stuck
  static const int64_t OB_SS_MICRO_CACHE_TASK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_TASK_STAT_VERSION);
};

struct ObSSMicroCacheMemoryStat
{
public:
  int64_t micro_alloc_cnt_; // actual allocated micro_meta count
  int64_t micro_cnt_limit_; // max micro_meta count which is limited by memory
  int64_t micro_meta_usage_;
  int64_t micro_map_usage_;
  int64_t phy_blk_usage_;
  int64_t mem_blk_usage_;
  int64_t micro_range_usage_;

  ObSSMicroCacheMemoryStat() { reset(); }
  void reset() { reset_dynamic_info(); micro_alloc_cnt_ = 0; micro_cnt_limit_ = 0; phy_blk_usage_ = 0; 
                 mem_blk_usage_ = 0; micro_range_usage_ = 0; }
  void reset_dynamic_info() { micro_meta_usage_ = 0; micro_map_usage_ = 0; }
  bool check_abnormal() const
  {
    return (ATOMIC_LOAD(&micro_alloc_cnt_) < 0) || (ATOMIC_LOAD(&mem_blk_usage_) < 0) ||
           (ATOMIC_LOAD(&micro_range_usage_) < 0);
  }
  int64_t get_micro_alloc_cnt() const { return ATOMIC_LOAD(&micro_alloc_cnt_); }
  int64_t get_total_mem_usage() const 
  { 
    return ATOMIC_LOAD(&micro_meta_usage_) + ATOMIC_LOAD(&micro_map_usage_) + ATOMIC_LOAD(&phy_blk_usage_) +
           ATOMIC_LOAD(&mem_blk_usage_) + ATOMIC_LOAD(&micro_range_usage_);
  }
  void set_micro_cnt_limit(const int64_t micro_cnt_limit) { ATOMIC_STORE(&micro_cnt_limit_, micro_cnt_limit); }
  void set_phy_blk_usage(const int64_t used_mem_size) { ATOMIC_STORE(&phy_blk_usage_, used_mem_size); }
  void set_micro_alloc_cnt(const int64_t cnt) { ATOMIC_STORE(&micro_alloc_cnt_, cnt); }
  void set_micro_map_usage(const int64_t size) { ATOMIC_STORE(&micro_map_usage_, size); }
  void update_micro_mem_usage(const int64_t delta_cnt, const int64_t used_mem_size)
  {
    ATOMIC_AAF(&micro_alloc_cnt_, delta_cnt);
    ATOMIC_STORE(&micro_meta_usage_, used_mem_size);
  }
  void update_micro_map_usage(const int64_t delta_mem_size) { ATOMIC_AAF(&micro_map_usage_, delta_mem_size); }
  void update_micro_range_usage(const int64_t delta_mem_size) { ATOMIC_AAF(&micro_range_usage_, delta_mem_size); }
  void update_mem_blk_usage(const int64_t delta_mem_size) { ATOMIC_AAF(&mem_blk_usage_, delta_mem_size); }
  TO_STRING_KV(K_(micro_alloc_cnt), K_(micro_cnt_limit), K_(micro_meta_usage), K_(micro_map_usage), K_(phy_blk_usage), 
    K_(mem_blk_usage), K_(micro_range_usage), "total_usage", get_total_mem_usage());

public:
  static const int64_t OB_SS_MICRO_CACHE_MEMORY_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_MEMORY_STAT_VERSION);
};

struct ObSSMicroCacheLoad
{
public:
  static const int64_t COMMON_IO_HIGH_THROUGHPUT = 6291456; // 6MB/s
public:
  struct ObSSMicroCacheLoadParam
  {
  public:
    int64_t interval_s_;
    int64_t pre_cnt_;
    int64_t pre_bytes_;
    int64_t cur_cnt_;
    int64_t cur_bytes_;

    ObSSMicroCacheLoadParam() { reset(); }
    void reset()
    {
      interval_s_ = 0;
      pre_cnt_ = 0;
      pre_bytes_ = 0;
      cur_cnt_ = 0;
      cur_bytes_ = 0;
    }

    void update_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      interval_s_ = interval_s;
      pre_cnt_ = cur_cnt_;
      pre_bytes_ = cur_bytes_;
      cur_cnt_ = cur_cnt;
      cur_bytes_ = cur_bytes;
    }

    int64_t get_iops() const
    {
      int64_t iops_result = 0;
      if (interval_s_ > 0) {
        iops_result = (cur_cnt_ - pre_cnt_) / interval_s_;
      }
      return iops_result;
    }

    int64_t get_throughput() const
    {
      int64_t throughput_result = 0;
      if (interval_s_ > 0) {
        throughput_result = (cur_bytes_ - pre_bytes_) / interval_s_;
      }
      return throughput_result;
    }

    TO_STRING_KV("iops", get_iops(), "throughput", get_throughput());
  };

  struct ObSSMicroCacheLoadInfo
  {
  public:
    ObSSMicroCacheLoadParam read_load_;
    ObSSMicroCacheLoadParam write_load_;

    ObSSMicroCacheLoadInfo() { reset(); }
    void reset()
    {
      read_load_.reset();
      write_load_.reset();
    }

    void update_read_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      read_load_.update_load_param(interval_s, cur_cnt, cur_bytes);
    }

    void update_write_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      write_load_.update_load_param(interval_s, cur_cnt, cur_bytes);
    }

    TO_STRING_KV(K_(read_load), K_(write_load));
  };
  
  ObSSMicroCacheLoadInfo common_io_load_;
  ObSSMicroCacheLoadInfo prewarm_io_load_;

  ObSSMicroCacheLoad() { reset(); }
  void reset()
  {
    common_io_load_.reset();
    prewarm_io_load_.reset();
  }

  void update_common_io_load_info(const ObSSMicroCacheIOStatParam &io_stat, const int64_t interval_s)
  {
    common_io_load_.update_write_load_param(interval_s, io_stat.add_cnt_, io_stat.add_bytes_);
    common_io_load_.update_read_load_param(interval_s, io_stat.get_cnt_, io_stat.get_bytes_);
  }

  void update_prewarm_io_load_info(const ObSSMicroCacheIOStatParam &io_stat, const int64_t interval_s)
  {
    prewarm_io_load_.update_write_load_param(interval_s, io_stat.add_cnt_, io_stat.add_bytes_);
    prewarm_io_load_.update_read_load_param(interval_s, io_stat.get_cnt_, io_stat.get_bytes_);
  }

  bool is_common_io_high_throughput() const
  {
    return common_io_load_.write_load_.get_throughput() >= COMMON_IO_HIGH_THROUGHPUT;
  }

  TO_STRING_KV(K_(common_io_load), K_(prewarm_io_load));
};

struct ObSSMicroCacheStat
{
public:
  ObSSMicroCacheIOStat io_stat_;
  ObSSMicroCacheHitStat hit_stat_;
  ObSSMicroCachePrewarmStat prewarm_stat_;
  ObSSMicroCachePhyBlkStat phy_blk_stat_;
  ObSSMicroCacheMemBlkStat mem_blk_stat_;
  ObSSMicroCacheMicroStat micro_stat_;
  ObSSMicroCacheRangeStat range_stat_;
  ObSSMicroCacheTaskStat task_stat_;
  ObSSMicroCacheMemoryStat mem_stat_;
  ObSSMicroCacheLoad cache_load_;

  ObSSMicroCacheStat() { reset(); }
  void reset() 
  { 
    io_stat_.reset();
    hit_stat_.reset(); 
    prewarm_stat_.reset();
    phy_blk_stat_.reset(); 
    mem_blk_stat_.reset(); 
    micro_stat_.reset(); 
    range_stat_.reset();
    task_stat_.reset();
    mem_stat_.reset();
    cache_load_.reset();
  }

  // Reset stats for calibration. 
  // Do not reset: io_stat/hit_stat/prewarm_stat(lifecycle), task_stat(ckpt_cnt etc), mem_stat/cache_load
  void reset_for_calibration()
  {
    micro_stat_.reset_meta_dynamic_stats();
    range_stat_.reset_dynamic_info();
  }

  // only used when clear micro cache
  void clear_cache_stat_dynamic_info()
  {
    io_stat_.reset();
    hit_stat_.reset();
    prewarm_stat_.reset();
    task_stat_.reset();
    micro_stat_.reset();
    range_stat_.reset_dynamic_info();
    mem_stat_.reset_dynamic_info();
    cache_load_.reset();
  }
  ObSSMicroCacheIOStat &io_stat() { return io_stat_; }
  ObSSMicroCacheHitStat &hit_stat() { return hit_stat_; }
  ObSSMicroCachePrewarmStat &prewarm_stat() { return prewarm_stat_; }
  ObSSMicroCachePhyBlkStat &phy_blk_stat() { return phy_blk_stat_; }
  ObSSMicroCacheMemBlkStat &mem_blk_stat() { return mem_blk_stat_; }
  ObSSMicroCacheMicroStat &micro_stat() { return micro_stat_; }
  ObSSMicroCacheRangeStat &range_stat() { return range_stat_; }
  ObSSMicroCacheTaskStat &task_stat() { return task_stat_; }
  ObSSMicroCacheMemoryStat &mem_stat() { return mem_stat_; }
  ObSSMicroCacheLoad &cache_load() { return cache_load_; }
  bool is_state_abnormal() const
  { 
    bool is_abnormal = false;
    if ((mem_blk_stat_.check_abnormal()) || (mem_stat_.check_abnormal()) || (micro_stat_.check_abnormal())) {
      is_abnormal = true;
    }
    return is_abnormal;
  }

  TO_STRING_KV(K_(io_stat), K_(hit_stat), K_(prewarm_stat), K_(phy_blk_stat), K_(mem_blk_stat), K_(micro_stat),
      K_(range_stat), K_(task_stat), K_(mem_stat), K_(cache_load));
  
public:
  static const int64_t OB_SS_MICRO_CACHE_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_STAT_VERSION);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_ */
