/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "lib/random/ob_random.h"
#include "lib/utility/ob_sort.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace hash;

/*-----------------------------------------ObSSPhysicalBlockManager------------------------------------------*/
ObSSPhysicalBlockManager::ObSSPhysicalBlockManager(ObSSMicroCacheStat &cache_stat, ObConcurrentFIFOAllocator &allocator)
  : is_inited_(false),
    block_size_(0),
    sub_arr_blk_cnt_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    cache_file_size_(0),
    phy_blk_reorgan_pct_(0),
    can_alloc_blk_{},
    blk_cnt_info_(cache_stat),
    newer_super_blk_index_(0),
    super_blk_(),
    resize_lock_(common::ObLatchIds::OB_SS_PHYSICAL_BLOCK_MANAGER_RESIZE_LOCK),
    info_lock_(common::ObLatchIds::OB_SS_PHYSICAL_BLOCK_MANAGER_INFO_LOCK),
    free_bitmap_(nullptr),
    reusable_blks_(),
    sparse_blk_map_(),
    phy_blk_arr_(),
    cache_stat_(cache_stat),
    allocator_(allocator)
{}

void ObSSPhysicalBlockManager::check_phy_block_safety()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!IS_INIT)) {
    LOG_WARN("not init", K_(is_inited)); // ignore ret
  } else {
    bool all_released = false;
    bool timeout_reached = false;
    const int64_t start_us = ObTimeUtility::current_time();
    int64_t total_blk_cnt = 0;
    {
      SpinRLockGuard resize_guard(resize_lock_);
      total_blk_cnt = blk_cnt_info_.total_blk_cnt_;
    }
    
    common::ObArray<int64_t> held_blk_idxes;
    do {
      all_released = true;
      held_blk_idxes.reset();
      {
        SpinRLockGuard resize_guard(resize_lock_);
        for (int64_t i = SS_SUPER_BLK_COUNT; i < total_blk_cnt; ++i) {
          ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(i);
          if (OB_NOT_NULL(phy_blk) && (phy_blk->get_ref_count() > 1)) {
            all_released = false;
            if (OB_TMP_FAIL(held_blk_idxes.push_back(i))) {
              LOG_WARN("fail to push back held blk index", K(i)); // ignore ret
              tmp_ret = OB_SUCCESS;
            }
          }
        }
      }
      if (!all_released) {
        const int64_t elapsed_us = ObTimeUtility::current_time() - start_us;
        if (elapsed_us >= PHY_BLK_HANDLE_WAIT_TIMEOUT_US) {
          timeout_reached = true;
          LOG_ERROR("wait too long for phy_blk_handle release, some phy_blk_handle still held",
                    K(PHY_BLK_HANDLE_WAIT_TIMEOUT_US), K(elapsed_us), "held_blk_cnt", held_blk_idxes.count()); // ignore ret
        } else {
          if (TC_REACH_TIME_INTERVAL(5 * 1000 * 1000L)) {  // 5s
            LOG_WARN("waiting for phy_blk_handle release", K(elapsed_us), K(PHY_BLK_HANDLE_WAIT_TIMEOUT_US)); // ignore ret
          }
          ob_usleep(PHY_BLK_HANDLE_WAIT_SLEEP_US);
        }
      }
    } while (!all_released && !timeout_reached);
    
    // Print held phy_blk info if any remain after timeout
    if (held_blk_idxes.count() > 0) {
      SpinRLockGuard resize_guard(resize_lock_);
      const int64_t held_blk_cnt = held_blk_idxes.count();
      for (int64_t i = 0; i < held_blk_cnt; ++i) {
        const int64_t blk_idx = held_blk_idxes.at(i);
        ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(blk_idx);
        if (OB_NOT_NULL(phy_blk)) {
          LOG_ERROR("phy_blk still held after waiting", K(blk_idx), "ref_cnt", phy_blk->get_ref_count(),
                    "blk_type", phy_blk->get_block_type(), KPC(phy_blk)); // ignore ret
        }
      }
    }
  }
}

void ObSSPhysicalBlockManager::destroy()
{
  // wait for phy_blk_handles release, which only needs resize_lock_ read lock (held inside)
  check_phy_block_safety();
  SpinWLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  if (nullptr != free_bitmap_) {
    free_bitmap_->~ObBitmap();
    allocator_.free(free_bitmap_);
    free_bitmap_ = nullptr;
  }
  phy_blk_arr_.destroy();
  cache_stat_.mem_stat().set_phy_blk_usage(0);
  reusable_blks_.destroy();
  sparse_blk_map_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_file_size_ = 0;
  phy_blk_reorgan_pct_ = 0;
  newer_super_blk_index_ = 0;
  blk_cnt_info_.reset();
  sub_arr_blk_cnt_ = 0;
  block_size_ = 0;
  super_blk_.reset();
  is_inited_ = false;
}

int ObSSPhysicalBlockManager::init(
    const uint64_t tenant_id,
    const int64_t cache_file_size, 
    const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || cache_file_size <= 0 || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_file_size), K(block_size));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K(tenant_id));
  } else {
    const int64_t real_blk_cnt = cache_file_size / block_size;
    const int64_t real_file_size = real_blk_cnt * block_size;
    sub_arr_blk_cnt_ = PhyBlockArr::BLOCK_CAPACITY;
    block_size_ = block_size;
    const int64_t DEFAULT_BUCKET_NUM = 128;
    const bool is_mini_cache = ObSSMicroCacheUtil::check_mini_cache(cache_file_size);
    if (OB_FAIL(reusable_blks_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSReuseBlks")))) {
      LOG_WARN("fail to create reusable_blks", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sparse_blk_map_.init(ObMemAttr(tenant_id, "SSSparseBlks")))) {
      LOG_WARN("fail to init sparse_blk_map", KR(ret), K(tenant_id));
    } else if (OB_FAIL(init_phy_block_array(tenant_id, real_blk_cnt))) {
      LOG_WARN("fail to init phy_block_array", KR(ret), K(tenant_id), K(real_blk_cnt));
    } else if (OB_FAIL(init_free_bitmap(tenant_id, real_blk_cnt, true/*first_init*/))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(tenant_id), K(real_blk_cnt));
    } else if (OB_FAIL(init_block_count(real_file_size, block_size, is_mini_cache))) {
      LOG_WARN("fail to init phy_block count", KR(ret), K(real_file_size), K(block_size), K(is_mini_cache));
    } else {
      for (uint8_t i = 0; i < SS_PHY_BLK_TYPE_CNT; ++i) {
        can_alloc_blk_[i] = true;
      }
      tenant_id_ = tenant_id;
      cache_file_size_ = real_file_size;
      phy_blk_reorgan_pct_ = SS_PHY_BLK_REORGAN_USAGE_PCT;
      is_inited_ = true;
      set_phy_blk_stat(cache_file_size);
      LOG_INFO("succ to init ss_phy_blk_mgr", K_(tenant_id), K_(cache_file_size), K_(block_size), K_(blk_cnt_info));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_phy_block_array(const uint64_t tenant_id, const int64_t blk_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(blk_cnt <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(blk_cnt));
  } else {
    phy_blk_arr_.set_tenant_id(tenant_id);
    phy_blk_arr_.set_label("SSPhyBlk");

    const int64_t actual_cnt = MAX(blk_cnt, sub_arr_blk_cnt_);
    if (OB_FAIL(phy_blk_arr_.prepare_allocate(actual_cnt))) {
      LOG_WARN("fail to prepare allocate", KR(ret), K(actual_cnt), K(blk_cnt));
    } else {
      cache_stat_.mem_stat().set_phy_blk_usage(actual_cnt * sizeof(ObSSPhysicalBlock));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_free_bitmap(
    const uint64_t tenant_id, 
    const int64_t blk_cnt,
    const bool first_init)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(blk_cnt <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(blk_cnt));
  } else if (first_init && OB_UNLIKELY(nullptr != free_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("free bitmap is not null", KR(ret), K(blk_cnt), KP_(free_bitmap));
  } else {
    ObMemAttr attr(tenant_id, "SSBlkBitMap");
    ObBitmap *tmp_free_bitmap = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), "size", sizeof(ObBitmap));
    } else if (FALSE_IT(tmp_free_bitmap = new (buf) ObBitmap(allocator_))) {
    } else if (OB_ISNULL(tmp_free_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("created free_bitmap should not be null", KR(ret));
    } else if (OB_FAIL(tmp_free_bitmap->init(blk_cnt, true/*is_all_true*/))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(blk_cnt));
    } else if (!first_init && nullptr != free_bitmap_) {
      if (OB_UNLIKELY(blk_cnt_info_.total_blk_cnt_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("blk_cnt_info is invalid", KR(ret), K_(blk_cnt_info));
      } else if (OB_FAIL(tmp_free_bitmap->copy_from(*free_bitmap_, 0, blk_cnt_info_.total_blk_cnt_))) {
        LOG_WARN("fail to copy_from another bitmap", KR(ret), K(blk_cnt), K_(blk_cnt_info));
      }
    }

    if (OB_SUCC(ret)) {
      ObBitmap *ori_bitmap = free_bitmap_;
      free_bitmap_ = tmp_free_bitmap;
      if (nullptr != ori_bitmap) {
        ori_bitmap->~ObBitmap();
        allocator_.free(ori_bitmap);
      }
    } else if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
    tmp_free_bitmap = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_block_count(
    const int64_t cache_file_size,
    const int32_t block_size,
    const bool is_mini_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cache_file_size <= 0 || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_file_size), K(block_size));
  } else {
    const int64_t total_blk_cnt = cache_file_size / block_size;
    blk_cnt_info_.init_blk_cnt_info(total_blk_cnt, block_size, is_mini_cache);
    if (OB_UNLIKELY(!blk_cnt_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("blk_cnt_info is invalid", KR(ret), K(is_mini_cache), K_(blk_cnt_info));
    }
  }
  return ret;
}

bool ObSSPhysicalBlockManager::is_valid_block_idx(const int64_t blk_idx) const
{
  return (blk_idx >= SS_SUPER_BLK_COUNT) && (blk_idx < blk_cnt_info_.total_blk_cnt_);
}

ObSSPhysicalBlock* ObSSPhysicalBlockManager::inner_get_phy_block_by_idx(const int64_t blk_idx)
{
  ObSSPhysicalBlock *phy_blk = nullptr;
  // NOTICE: upper layer needs to take resize_lock
  if (IS_INIT && OB_LIKELY(is_valid_block_idx(blk_idx))) {
    phy_blk = &(phy_blk_arr_.at(blk_idx));
  }
  return phy_blk;
}

int ObSSPhysicalBlockManager::resize_file_size(const int64_t new_file_size, const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(do_resize_file_size(new_file_size, block_size))) {
    LOG_WARN("fail to do resize file size", KR(ret), K_(tenant_id), K_(cache_file_size), 
      K(new_file_size), K(block_size));
  } else if (OB_FAIL(get_and_update_cache_limit_size())) {
    LOG_WARN("fail to get and update cache limit size", KR(ret), K(new_file_size));
  }
  return ret;
}

// Adjust super_blk.cache_file_size, free_bitmap_, blk_cnt_info, etc.
// NOTE: super_blk.meta_ckpt_info will be adjusted while executing meta ckpt
int ObSSPhysicalBlockManager::do_resize_file_size(
    const int64_t new_file_size, 
    const int32_t block_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t real_blk_cnt = 0;
  int64_t real_file_size = 0;
  ObSSMicroCacheSuperBlk new_super_blk;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);
    const int64_t ori_file_size = cache_file_size_;
    bool is_mini_cache = true;
    // Now only support 'increase file size'
    if (OB_UNLIKELY(new_file_size < ori_file_size || block_size <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(new_file_size), K(ori_file_size), K(block_size));
    } else if (OB_FAIL(inner_get_ss_super_block(new_super_blk))) {
      LOG_WARN("fail to inner get ss_super_block", KR(ret), K_(super_blk));
    } else if (FALSE_IT(is_mini_cache = ObSSMicroCacheUtil::check_mini_cache(new_file_size))) {
    } else if (FALSE_IT(new_super_blk.update_cache_file_size(new_file_size))) {
    } else if (FALSE_IT(real_blk_cnt = new_file_size / block_size)) {
    } else if (FALSE_IT(real_file_size = real_blk_cnt * block_size)) {
    } else if (real_file_size == cache_file_size_) { // if cache_file_size not changed
    } else if (OB_FAIL(init_phy_block_array(tenant_id_, real_blk_cnt))) {
      LOG_WARN("fail to init phy_block_array", KR(ret), K(real_blk_cnt), K_(blk_cnt_info));
    } else if (OB_FAIL(init_free_bitmap(tenant_id_, real_blk_cnt, false/*first_init*/))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(real_blk_cnt), K_(blk_cnt_info));
    } else if (OB_FAIL(init_block_count(real_file_size, block_size, is_mini_cache))) {
      LOG_WARN("fail to init block count", KR(ret), K(real_file_size), K(block_size), K(is_mini_cache));
    } else {
      cache_file_size_ = real_file_size;
      set_phy_blk_stat(new_file_size);
    }

    if (FAILEDx(inner_write_ss_super_block(new_super_blk, false/*is_format*/, true/*allow_failed*/))) {
      LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
    } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
      FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(new_super_blk));
    }
    FLOG_INFO("finish to resize ss_micro_cache file size", KR(ret), KR(tmp_ret), K(ori_file_size),
      K(new_file_size), K_(blk_cnt_info));
  }
  return ret;
}

void ObSSPhysicalBlockManager::adjust_micro_meta_block()
{
  int64_t available_cnt = 0;
  reserve_micro_meta_block(available_cnt);
}

void ObSSPhysicalBlockManager::reserve_micro_meta_block(int64_t &available_cnt)
{
  if (IS_INIT) {
    const int64_t total_micro_cnt = cache_stat_.micro_stat().get_total_micro_cnt();
    const int64_t estimate_cnt = total_micro_cnt * SS_AVG_MICRO_META_PERSIST_SIZE / block_size_ + 1;

    SpinRLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);

    blk_cnt_info_.try_reserve_dynamic_blk(ObSSPhyBlockType::SS_MICRO_META_BLK, estimate_cnt, available_cnt);
  }
}

void ObSSPhysicalBlockManager::reserve_reorganize_block(int64_t &available_cnt)
{
  if (IS_INIT) {
    // estimate the count of blocks needed for reorganize_task based on current avg_micro_size
    int64_t estimate_cnt = SS_MIN_REORGAN_BLK_CNT;
    const int64_t avg_micro_size = cache_stat_.micro_stat().get_avg_micro_size();
    if (avg_micro_size > SS_REORGAN_MIN_MICRO_SIZE) {
      const int64_t scale_cnt = (avg_micro_size - SS_REORGAN_MIN_MICRO_SIZE) / SS_REORGAN_BLK_SCALING_FACTOR;
      estimate_cnt = scale_cnt + SS_MIN_REORGAN_BLK_CNT;
    }

    SpinRLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);

    estimate_cnt = MIN(estimate_cnt, blk_cnt_info_.reorgan_blk_cnt_);
    blk_cnt_info_.try_reserve_dynamic_blk(ObSSPhyBlockType::SS_MICRO_DATA_BLK, estimate_cnt, available_cnt);
  }
}

void ObSSPhysicalBlockManager::check_all_blocks_length()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(block_size_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), K_(block_size));
  } else {
    int64_t empty_cnt = 0;
    int64_t can_reorgan_cnt = 0;
    int64_t can_reuse_cnt = 0;
    const int64_t ori_reusable_blk_cnt = reusable_blks_.size();
    SpinRLockGuard resize_guard(resize_lock_);
    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(i);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i));
      } else {
        const int64_t PRINT_EMPTY_BLK_MAX_CNT = 5;
        const uint64_t valid_len = phy_blk->valid_len_;
        if (0 == valid_len) {
          if (phy_blk->can_reuse()) {
            ++can_reuse_cnt;
            if (OB_TMP_FAIL(add_reusable_block(i))) {
              LOG_WARN("fail to add block into reusable_set", KR(tmp_ret), K(i), K_(blk_cnt_info), KPC(phy_blk));
              tmp_ret = OB_SUCCESS;
            } 
          } else if (empty_cnt < PRINT_EMPTY_BLK_MAX_CNT) { // choose at most 5 phy_blocks to print
            FLOG_INFO("this phy_blk can't be reused", K(empty_cnt), K(i), KPC(phy_blk));
          }
          ++empty_cnt;
        } else {
          const double blk_usage = static_cast<double>(valid_len) / block_size_;
          if (blk_usage < phy_blk_reorgan_pct_) {
            ++can_reorgan_cnt;
          }
        }
      }
    }
    LOG_INFO("check current phy_blocks length", K_(phy_blk_reorgan_pct), K_(blk_cnt_info), K(empty_cnt),
      K(can_reuse_cnt), K(can_reorgan_cnt), K(ori_reusable_blk_cnt), K(sparse_blk_map_.count()), 
      K(reusable_blks_.size()));
  }
}

void ObSSPhysicalBlockManager::check_blocks_info(ObIArray<int64_t> &blk_list)
{
  int ret = OB_SUCCESS;
  const int64_t blk_cnt = blk_list.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (blk_cnt > 0) {
    ObSSPhysicalBlock *phy_blk = nullptr;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = 0; i < blk_cnt; ++i) {
      const int64_t blk_idx = blk_list.at(i);
      phy_blk = inner_get_phy_block_by_idx(blk_idx);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K(blk_idx), K(blk_cnt), K_(blk_cnt_info));
      } else {
        const bool is_bitmap_free = free_bitmap_->test(blk_idx);
        FLOG_INFO("check phy_block info", K(i), K(blk_cnt), K(blk_idx), K(is_bitmap_free), KPC(phy_blk));
      }
    }
  }
}

bool ObSSPhysicalBlockManager::inner_can_alloc_blk(const ObSSPhyBlockType blk_type) const
{
  return can_alloc_blk_[static_cast<uint8_t>(blk_type)];
}

void ObSSPhysicalBlockManager::inner_update_alloc_blk(const ObSSPhyBlockType blk_type, const bool is_allowed)
{
  can_alloc_blk_[static_cast<uint8_t>(blk_type)] = is_allowed;
}

void ObSSPhysicalBlockManager::inner_update_alloc_all_blk(const bool is_allowed)
{
  for (uint8_t i = 0; i < static_cast<uint8_t>(SS_PHY_BLK_TYPE_CNT); ++i) {
    can_alloc_blk_[i] = is_allowed;
  }
}

void ObSSPhysicalBlockManager::allow_alloc_all_blk()
{
  SpinWLockGuard info_guard(info_lock_);
  inner_update_alloc_all_blk(true);
}

void ObSSPhysicalBlockManager::forbid_alloc_all_blk()
{
  SpinWLockGuard info_guard(info_lock_);
  inner_update_alloc_all_blk(false);
}

int ObSSPhysicalBlockManager::alloc_block(
    int64_t &blk_idx, 
    ObSSPhyBlockHandle &phy_blk_handle,
    const ObSSPhyBlockType blk_type)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  blk_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_TYPE == blk_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_type));
  } else if (OB_UNLIKELY(!inner_can_alloc_blk(blk_type))) {
    ret = OB_EAGAIN;
    LOG_WARN("fail to alloc cuz can't be allowed to alloc blk", KR(ret), K(blk_type), K_(can_alloc_blk));
  } else if (OB_UNLIKELY(!blk_cnt_info_.has_free_blk(blk_type))) {
    ret = OB_EAGAIN;
    // reduce printing log frequency
    if (TC_REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("fail to alloc cuz not exist free blk", KR(ret), K(blk_type), K_(blk_cnt_info));
    }
  } else if (OB_FAIL(free_bitmap_->next_valid_idx(SS_SUPER_BLK_COUNT,
             blk_cnt_info_.total_blk_cnt_ - SS_SUPER_BLK_COUNT, false, blk_idx))) {
    LOG_ERROR("fail to get next valid idx", KR(ret), K(blk_type), K_(blk_cnt_info));
  } else if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid phy_block idx", KR(ret), K(blk_idx), K(blk_type), K_(blk_cnt_info), 
      "is_all_false", free_bitmap_->is_all_false());
  } else if (FALSE_IT(free_bitmap_->set(blk_idx, false))) {
  } else if (OB_FAIL(blk_cnt_info_.increase_blk_used_cnt(blk_type))) {
    LOG_WARN("fail to increase blk_used_cnt", KR(ret), K(blk_type), K_(blk_cnt_info));
  } else {
    ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(blk_idx);
    if (OB_ISNULL(phy_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx), K_(blk_cnt_info));
    } else if (OB_FAIL(phy_blk->mark_used(blk_type, false/*is_sealed*/))) {
      LOG_ERROR("fail to first set phy_block used", KR(ret), K(blk_idx), K(blk_type), KPC(phy_blk));
      // revise this phy_blk's state, wait for scaning to reuse, avoid this phy_blk 'leak'
      phy_blk->mark_reusable(blk_type);
    } else {
      phy_blk_handle.set_ptr(phy_blk);
      LOG_TRACE("ss_micro_cache: succ to alloc phy_blk", K(blk_idx), K(blk_type), K_(blk_cnt_info), KPC(phy_blk));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::free_block(const int64_t blk_idx, bool &succ_free)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  succ_free = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid phy_block idx", KR(ret), K(blk_idx), K_(blk_cnt_info));
  } else {
    ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(blk_idx);
    if (OB_ISNULL(phy_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx), K_(blk_cnt_info));
    } else if (OB_UNLIKELY(phy_blk->is_free())){
      // prevent 'double free'.
      const bool is_bitmap_free = free_bitmap_->test(blk_idx);
      LOG_ERROR("phy_block has been freed", K(is_bitmap_free), K(blk_idx), KPC(phy_blk));
    } else {
      const ObSSPhyBlockType blk_type = phy_blk->get_block_type();
      if (is_ss_ckpt_block(blk_type)) {
        phy_blk->set_valid_len(0);
      }
      
      const bool is_sealed = phy_blk->is_sealed();
      if (OB_FAIL(phy_blk->try_free(succ_free))) {
        LOG_WARN("fail to try free phy_blk", KR(ret), KPC(phy_blk));
      } else if (succ_free) {
        free_bitmap_->set(blk_idx, true);
        if (OB_FAIL(blk_cnt_info_.decrease_blk_used_cnt(blk_type))) {
          LOG_WARN("fail to decrease blk_used_cnt", KR(ret), K(blk_idx), K(blk_type), KPC(phy_blk));
        } else if (is_sealed) {
          if (OB_FAIL(delete_sparse_block(ObSSPhyBlockIdx(blk_idx)))) {
            LOG_WARN("fail to delete sparse phy_blk", KR(ret), K(blk_idx), KPC(phy_blk));
          }
          LOG_TRACE("ss_micro_cache: succ to free phy_blk", K(blk_idx), K(blk_type), K_(blk_cnt_info), KPC(phy_blk));
        } else {
          LOG_INFO("ss_micro_cache: succ to free unsealed phy_blk", K(blk_idx), K(blk_type), K_(blk_cnt_info), KPC(phy_blk));
        }
      }

      if (OB_SUCC(ret) && succ_free) {
        if (OB_FAIL(delete_reusable_block(blk_idx))) {
          LOG_WARN("fail to delete block from reusable set", KR(ret), K(blk_idx));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::try_free_batch_blocks(const ObIArray<int64_t> &blk_idx_arr, int64_t &free_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  free_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_LIKELY(!blk_idx_arr.empty())) {
    ObSEArray<int64_t, 64> unfree_blk_arr;
    const int64_t total_cnt = blk_idx_arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < total_cnt); ++i) {
      bool succ_free = false;
      const int64_t blk_idx = blk_idx_arr.at(i);
      if (OB_FAIL(free_block(blk_idx, succ_free))) {
        LOG_WARN("fail to free block", KR(ret), K(blk_idx));
      } else if (succ_free) {
        ++free_cnt;
      } 
      
      if (!succ_free && OB_TMP_FAIL(unfree_blk_arr.push_back(blk_idx))) {
        LOG_WARN("fail to push back", KR(tmp_ret), K(i), K(total_cnt), K(blk_idx));
        tmp_ret = OB_SUCCESS;
      }
    }

    const int64_t unfree_blk_cnt = unfree_blk_arr.count();
    if (OB_SUCC(ret) && (unfree_blk_cnt > 0)) {
      if (OB_FAIL(add_batch_reusable_blocks(unfree_blk_arr))) {
        LOG_WARN("fail to add reusable phy_blocks", KR(ret), K(unfree_blk_arr), K(unfree_blk_cnt));
      }
      FLOG_INFO("exist unfree blocks", KR(ret), K(unfree_blk_cnt), K(unfree_blk_arr));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::rebuild_phy_blk_valid_len(const common::ObIArray<int64_t> &expected_valid_len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(expected_valid_len.count() != blk_cnt_info_.total_blk_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("phy_blk cnt mismatch", KR(ret), "expected_cnt", expected_valid_len.count(),
        "total_blk_cnt", blk_cnt_info_.total_blk_cnt_);
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);
    reusable_blks_.reuse();
    sparse_blk_map_.clear();
    const int64_t total_blk_cnt = blk_cnt_info_.total_blk_cnt_;
    for (int64_t i = SS_SUPER_BLK_COUNT; i < total_blk_cnt; ++i) {
      ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(i);
      if (OB_ISNULL(phy_blk)) {
        LOG_WARN("phy_block should not be null", K(i), K(total_blk_cnt)); // ignore ret
      } else if ((ObSSPhyBlockType::SS_MICRO_DATA_BLK == phy_blk->get_block_type())
              || (ObSSPhyBlockType::SS_REORGAN_BLK == phy_blk->get_block_type())) {
        // only update valid_len for data_blk and reorgan_blk
        const int64_t new_valid_len = expected_valid_len.at(i);
        if (OB_UNLIKELY((new_valid_len < 0) || (new_valid_len > block_size_))) {
          LOG_WARN("invalid valid_len", K(i), K(new_valid_len), "block_size", block_size_); // ignore ret
        } else if (FALSE_IT(phy_blk->set_valid_len(new_valid_len))) {
        } else if (!phy_blk->is_free()) {
          // seal in-use blocks so reuse/reorganize/dec_valid_len can process it.
          phy_blk->mark_sealed(new_valid_len);
          if (0 == new_valid_len) {
            if (OB_TMP_FAIL(add_reusable_block(i))) {
              LOG_WARN("fail to add reusable block", KR(tmp_ret), K(i), KPC(phy_blk));
              tmp_ret = OB_SUCCESS;
            }
          } else if (phy_blk->can_reorganize()) {
            if (OB_TMP_FAIL(add_sparse_block(ObSSPhyBlockIdx(i)))) {
              LOG_WARN("fail to add sparse block", KR(tmp_ret), K(i), KPC(phy_blk));
              tmp_ret = OB_SUCCESS;
            }
          }
        }
      } else {
        // Keep checkpoint blocks (e.g. SS_PHY_BLK_CKPT_BLK) unchanged here.
        // They will be released after new checkpoint finishes.
        // Preserving them allows replaying part of data if crash happens.
      }
    }
    const int64_t reusable_cnt = reusable_blks_.size();
    const int64_t sparse_cnt = sparse_blk_map_.count();
    cache_stat_.phy_blk_stat().set_reusable_block_cnt(reusable_cnt);
    cache_stat_.phy_blk_stat().set_sparse_block_cnt(sparse_cnt);
    FLOG_INFO("ss_micro_cache: finish rebuild phy_blk valid_len", K_(tenant_id), K(total_blk_cnt),
        K(reusable_cnt), K(sparse_cnt), K_(blk_cnt_info));
  }
  return ret;
}

int ObSSPhysicalBlockManager::add_reusable_block(const int64_t blk_idx)
{
  int ret = OB_SUCCESS;
  // don't check blk_idx valid or not, cuz 'is_valid_block_idx' needs resize_lock
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(reusable_blks_.set_refactored(blk_idx, 1/*overwrite*/))) {
    LOG_ERROR("fail to add block into reusable set, may leak!", KR(ret), K(blk_idx));
  } else {
    LOG_TRACE("ss_micro_cache: add reusable phy_blk", K(blk_idx));
  }
  return ret;
}

int ObSSPhysicalBlockManager::add_batch_reusable_blocks(const ObIArray<int64_t> &blk_idx_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const int64_t array_cnt = blk_idx_arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < array_cnt); i++) {
      const int64_t blk_idx = blk_idx_arr.at(i);
      if (OB_FAIL(add_reusable_block(blk_idx))) {
        LOG_WARN("fail to add reusable block", KR(ret), K(i), K(array_cnt), K(blk_idx));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::scan_reusable_ckpt_blocks_to_free(bool &exist_reusable_ckpt_blks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<int64_t> ckpt_blk_idx_arr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    exist_reusable_ckpt_blks = false;
    SpinRLockGuard guard_for_resize(resize_lock_);
    SpinRLockGuard info_guard(info_lock_);

    ObHashSet<int64_t>::iterator iter = reusable_blks_.begin();
    for (; OB_SUCC(ret) && iter != reusable_blks_.end(); iter++) {
      const int64_t blk_idx = iter->first;
      if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
        // no need to return err, just print log and delete this blk_idx from reusable_set
        LOG_ERROR("invalid block_idx", KR(ret), K(blk_idx), K_(blk_cnt_info));
        if (OB_TMP_FAIL(delete_reusable_block(blk_idx))) {
          LOG_WARN("fail to delete block idx from resuable_set", KR(tmp_ret), K(blk_idx));
        }
      } else {
        ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(blk_idx);
        if (OB_ISNULL(phy_blk)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx), K_(blk_cnt_info));
        } else if (phy_blk->can_reuse() && is_ss_ckpt_block(phy_blk->get_block_type())) {
          LOG_INFO("exist reusable ckpt block which need to be freed", K(blk_idx), KPC(phy_blk));
          if (OB_FAIL(ckpt_blk_idx_arr.push_back(blk_idx))) {
            LOG_WARN("fail to push back", KR(ret), K(blk_idx));
          }
        }
      }
    }

    // forbid all blk alloc, except phy_blk_ckpt blk. Cuz we will free these ckpt blocks, and these blocks' reuse_version now persisted.
    if (OB_SUCC(ret) && (ckpt_blk_idx_arr.count() > 0)) {
      inner_update_alloc_all_blk(false);
      inner_update_alloc_blk(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK, true);
      exist_reusable_ckpt_blks = true;
    }
  }

  const int64_t ckpt_blk_cnt = ckpt_blk_idx_arr.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < ckpt_blk_cnt); ++i) {
    const int64_t blk_idx = ckpt_blk_idx_arr.at(i);
    bool succ_free = false;
    if (OB_FAIL(free_block(blk_idx, succ_free))) {
      LOG_ERROR("fail to free block", KR(ret), K(i), K(blk_idx), K(ckpt_blk_idx_arr));
    } else if (!succ_free) {
      LOG_INFO("not free ckpt block", K(i), K(blk_idx), K(ckpt_blk_idx_arr));
    } else {
      FLOG_INFO("succ to free reusable ckpt block", K(i), K(blk_idx), K(ckpt_blk_cnt));
      if (OB_TMP_FAIL(delete_reusable_block(blk_idx))) {
        LOG_WARN("fail to delete reusable block", KR(tmp_ret), K(blk_idx), K(i), K(ckpt_blk_cnt));
        tmp_ret = OB_SUCCESS;
      }
    }
    ret = OB_SUCCESS; // continue to handle next one
  }

  if (exist_reusable_ckpt_blks) {
    FLOG_INFO("forbid all blk alloc, except phy_blk_ckpt blk", K(ckpt_blk_cnt), K(ckpt_blk_idx_arr.at(0)));
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_batch_reusable_blocks(ObIArray<int64_t> &blk_idx_arr, const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(max_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(max_cnt));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);
    int64_t blk_idx = -1;
    const int64_t count = MIN(max_cnt, reusable_blks_.size());
    ObHashSet<int64_t>::iterator iter = reusable_blks_.begin();
    for (int64_t i = 0; OB_SUCC(ret) && i < count && iter != reusable_blks_.end(); i++, iter++) {
      blk_idx = iter->first;
      if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
        // no need to return err, just print log and delete this blk_idx from reusable_set
        LOG_ERROR("invalid block_idx", KR(ret), K(blk_idx), K_(blk_cnt_info));
        if (OB_TMP_FAIL(delete_reusable_block(blk_idx))) {
          LOG_WARN("fail to delete block idx from resuable_set", KR(tmp_ret), K(blk_idx));
        }
      }
      if (OB_FAIL(blk_idx_arr.push_back(blk_idx))) {
        LOG_WARN("fail to push block idx", KR(ret), K(blk_idx), K(blk_idx_arr));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::delete_reusable_block(const int64_t blk_idx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(blk_idx < SS_SUPER_BLK_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_idx));
  } else if (OB_FAIL(reusable_blks_.erase_refactored(blk_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from reusable_set", KR(ret), K(blk_idx));
    }
  }
  return ret;
}

void ObSSPhysicalBlockManager::clear()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  SpinWLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);

  // clear phy_blocks state
  reusable_blks_.reuse();
  sparse_blk_map_.clear();
  for (int64_t i = SS_SUPER_BLK_COUNT; i < blk_cnt_info_.total_blk_cnt_; ++i) {
    bool succ_free = false;
    ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(i);
    if (OB_NOT_NULL(phy_blk) && !phy_blk->is_free()) {
      const ObSSPhyBlockType blk_type = phy_blk->get_block_type();
      phy_blk->set_valid_len(0); // all its data was already been cleared
      phy_blk->try_free(succ_free); // ignore ret
      if (succ_free) {
        free_bitmap_->set(i, true);
        if (OB_TMP_FAIL(blk_cnt_info_.decrease_blk_used_cnt(blk_type))) {
          LOG_ERROR("fail to decrease blk_used_cnt", KR(tmp_ret), K(i), K(blk_type), KPC(phy_blk));
        }
      } else if (OB_TMP_FAIL(add_reusable_block(i))) {
        LOG_ERROR("fail to add into reusable_set, phy_blk leak!!!", KR(tmp_ret), K(i), KPC(phy_blk));
      }
    }
  }
  cache_stat_.phy_blk_stat().reset_dynamic_info();
  cache_stat_.phy_blk_stat().set_reusable_block_cnt(reusable_blks_.size());

  FLOG_INFO("ss_micro_cache: clear phy_block manager", KR(ret), K_(blk_cnt_info), K_(super_blk));
}

int ObSSPhysicalBlockManager::scan_blocks_to_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);
    SpinRLockGuard info_guard(info_lock_);
    ObSSPhysicalBlock *phy_blk = nullptr;
    int64_t can_reuse_cnt = 0;
    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); i++) {
      if (OB_ISNULL(phy_blk = inner_get_phy_block_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else if (phy_blk->can_reuse()) {
        if (OB_FAIL(add_reusable_block(i))) {
          LOG_WARN("fail to add block into reusable_set", KR(ret), K(i), K_(blk_cnt_info), KPC(phy_blk));
        } else {
          ++can_reuse_cnt;
        }
      }
    }

    LOG_INFO("ss_micro_cache: scan blocks to reuse", KR(ret), K(can_reuse_cnt));
  }
  return ret;
}

int ObSSPhysicalBlockManager::scan_blocks_to_reorgan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);
    SpinRLockGuard info_guard(info_lock_);
    ObSSPhysicalBlock *phy_blk = nullptr;
    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); i++) {
      phy_blk = inner_get_phy_block_by_idx(i);

      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        const int32_t blk_valid_len = phy_blk->get_valid_len();
        if (OB_UNLIKELY(blk_valid_len > block_size_ || block_size_ <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("abnormal phy_block valid_len or block_size", KR(ret), K_(block_size), KPC(phy_blk));
        } else if (phy_blk->can_reorganize() && OB_FAIL(add_sparse_block(ObSSPhyBlockIdx(i)))) {
          LOG_WARN("fail to add sparse_block", KR(ret), K(i), KPC(phy_blk));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::add_sparse_block(const ObSSPhyBlockIdx &blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(blk_idx.blk_idx_ < SS_SUPER_BLK_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_idx));
  } else if (OB_FAIL(sparse_blk_map_.insert(blk_idx, true))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to add sparse_block", KR(ret), K(blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::delete_sparse_block(const ObSSPhyBlockIdx &blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(blk_idx.blk_idx_ < SS_SUPER_BLK_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_idx));
  } else if (OB_FAIL(sparse_blk_map_.erase(blk_idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from sparse_blk_map", KR(ret), K(blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_batch_sparse_blocks(
    ObIArray<ObSSPhyBlkReorganEntry> &sparse_blks, 
    const int64_t max_reorgan_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!sparse_blks.empty() || max_reorgan_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sparse_blks), K(max_reorgan_cnt));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);

    // in case first randomly iter is 'iter_end', thus we plus 1.
    const int64_t max_scan_cnt = MIN(SS_MAX_SCAN_SPARSE_BLK_CNT, sparse_blk_map_.count() + 1);
    ObArray<SparsePhyBlockInfo> sparse_info_arr;
    if (OB_FAIL(sparse_info_arr.reserve(max_scan_cnt))) {
      LOG_WARN("fail to reserve array", KR(ret), K(max_scan_cnt));
    }

    // 1. Scan some phy_blocks randomly.
    ObSSPhysicalBlock *phy_blk = nullptr;
    SparsePhyBlockMap::BlurredIterator iter(sparse_blk_map_);
    iter.rewind_randomly();
    int64_t scan_cnt = 0;
    while (OB_SUCC(ret) && (++scan_cnt <= max_scan_cnt)) {
      ObSSPhyBlockIdx blk_idx;
      bool is_exist = false;
      bool is_invalid_blk = false;
      if (OB_FAIL(iter.next(blk_idx, is_exist))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          iter.rewind();
        } else {
          LOG_WARN("fail to next phy_block", KR(ret), K(scan_cnt), K(max_scan_cnt));
        }
      } else if (OB_UNLIKELY(!is_valid_block_idx(blk_idx.blk_idx_))) {
        LOG_WARN("block idx is invalid", KR(ret), K(blk_idx), K_(blk_cnt_info));
        is_invalid_blk = true;
      } else if (OB_ISNULL(phy_blk = inner_get_phy_block_by_idx(blk_idx.blk_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx), K_(blk_cnt_info));
      } else if (OB_UNLIKELY(phy_blk->is_empty())) {
        LOG_INFO("phy_block is already empty", K(blk_idx), K_(blk_cnt_info), KPC(phy_blk));
        is_invalid_blk = true;
      } else if (OB_UNLIKELY(!phy_blk->can_reorganize())) {
        LOG_ERROR("phy_block should be reorganizable", KR(ret), K(blk_idx), K_(blk_cnt_info), KPC(phy_blk));
        is_invalid_blk = true;
      } else {
        SparsePhyBlockInfo sparse_blk_info(blk_idx.blk_idx_, phy_blk->get_valid_len());
        if (OB_FAIL(sparse_info_arr.push_back(sparse_blk_info))) {
          LOG_WARN("fail to push sparse_blk_info", KR(ret), K(sparse_blk_info));
        }
      }

      if (OB_SUCC(ret) && is_invalid_blk) {
        if (OB_TMP_FAIL(delete_sparse_block(blk_idx))) {
          LOG_WARN("fail to delete sparse_block", KR(tmp_ret), K(blk_idx));
        }
      }
    } 

    // 2. Sort and select some phy_blocks with the smallest valid_len to reorganize.
    if (OB_SUCC(ret) && sparse_info_arr.count() > 0) {
      lib::ob_sort(sparse_info_arr.begin(), sparse_info_arr.end(), SparsePhyBlockInfoCmp());
      
      for (int64_t i = 0; OB_SUCC(ret) && sparse_blks.count() < max_reorgan_cnt && i < sparse_info_arr.count(); ++i) {
        const int64_t cur_blk_idx = sparse_info_arr[i].blk_idx_;
        // avoid pushing duplicate phy_block_idx
        bool repeated = false;
        for (int64_t j = 0; j < sparse_blks.count(); ++j) {
          if (sparse_blks.at(j).phy_blk_idx_ == cur_blk_idx) {
            repeated = true;
            break;
          }
        }

        if (!repeated) {
          phy_blk = inner_get_phy_block_by_idx(cur_blk_idx);
          if (OB_ISNULL(phy_blk)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block should not be null", KR(ret), K(cur_blk_idx), K_(blk_cnt_info));
          } else {
            const uint64_t reuse_version = phy_blk->get_reuse_version();
            ObSSPhyBlkReorganEntry reorgan_entry(cur_blk_idx, phy_blk, reuse_version);  // will gen phy_blk_handle, inc ref_cnt
            if (OB_FAIL(sparse_blks.push_back(reorgan_entry))) {
              LOG_WARN("fail to push back", KR(ret), K(reorgan_entry));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const uint64_t phy_blk_min_len = sparse_info_arr[0].valid_len_;
        cache_stat_.task_stat().set_phy_blk_min_len(phy_blk_min_len);
      }
    }
  }

  return ret;
}

int ObSSPhysicalBlockManager::update_compressor_type(const ObCompressorType new_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCacheSuperBlk new_super_blk;
  ObCompressorType old_type = ObCompressorType::INVALID_COMPRESSOR;
  SpinWLockGuard resize_guard(resize_lock_);
  if (OB_FAIL(inner_get_ss_super_block(new_super_blk))) {
    LOG_WARN("fail to inner get ss_super_block", KR(ret), K_(super_blk));
  } else if (FALSE_IT(old_type = new_super_blk.admin_info_.get_micro_meta_compressor_type())) {
  } else if (FALSE_IT(new_super_blk.set_compressor_type(new_type))) {
  } else if (OB_FAIL(inner_write_ss_super_block(new_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
  } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
    FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(new_super_blk), K_(super_blk));
  } 
  
  if (OB_SUCC(ret)) {
    FLOG_INFO("ss_micro_cache: succ to update compressor_type", KR(tmp_ret), K(old_type), K(new_type));
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_blk_ckpt_info(ObSSBlkCkptInfo &blk_ckpt_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(blk_ckpt_info.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("blk_ckpt_info is not inited", KR(ret), K(blk_ckpt_info));
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    if (OB_FAIL(inner_check_ss_super_block())) {
      LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
    } else if (OB_FAIL(blk_ckpt_info.assign(super_blk_.blk_ckpt_info_))) {
      LOG_WARN("fail to assign blk_ckpt primary info", KR(ret), K_(super_blk), K(blk_ckpt_info));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_micro_ckpt_info(ObSSMicroCkptInfo &micro_ckpt_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(micro_ckpt_info.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_ckpt_info is not inited", KR(ret), K(micro_ckpt_info));
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    if (OB_FAIL(inner_check_ss_super_block())) {
      LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
    } else if (OB_FAIL(micro_ckpt_info.assign(super_blk_.micro_ckpt_info_, false/*need_reset*/))) {
      LOG_WARN("fail to assign micro_ckpt primary info", KR(ret), K_(super_blk), K(micro_ckpt_info));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::format_ss_super_block(const uint32_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // invoked when first start(micro_cache file not exist), no need to add resize_lock
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(super_blk_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_super_block is already valid", KR(ret), K_(super_blk));
  } else {
    ObSSMicroCacheSuperBlk tmp_super_blk(tenant_id_, cache_file_size_, micro_ckpt_split_cnt);
    if (OB_FAIL(inner_write_ss_super_block(tmp_super_blk, true/*is_format*/))) {
      LOG_WARN("fail to do write ss_super_block", KR(ret), K(tmp_super_blk));
    } else if (OB_TMP_FAIL(super_blk_.assign(tmp_super_blk))) {
      FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(tmp_super_blk), K_(super_blk));
    } 
    
    if (OB_SUCC(ret)) {
      FLOG_INFO("ss_micro_cache: succ to format ss_super_block", K(tmp_ret), K(micro_ckpt_split_cnt), K_(super_blk));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    if (OB_FAIL(inner_get_ss_super_block(ss_super_blk))) {
      LOG_WARN("fail to inner get ss_super_block", KR(ret));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::reformat_ss_super_block(const int64_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);

  ObSSMicroCacheSuperBlk tmp_super_blk;
  if (OB_FAIL(inner_check_ss_super_block())) {
    LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
  } else if (OB_LIKELY(micro_ckpt_split_cnt == super_blk_.get_ckpt_split_cnt())) {
    if (OB_FAIL(tmp_super_blk.assign(super_blk_))) {
      FLOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk), K_(super_blk));
    } else {
      tmp_super_blk.reuse();
    }
  } else {
    FLOG_INFO("micro_ckpt split cnt is changed", K(micro_ckpt_split_cnt), K(super_blk_.get_ckpt_split_cnt()));
    tmp_super_blk.assign_basic_info(super_blk_);
    if (OB_FAIL(tmp_super_blk.init_non_basic_info(tenant_id_, micro_ckpt_split_cnt))) {
      LOG_WARN("fail to init non basic info", KR(ret), K_(tenant_id), K(micro_ckpt_split_cnt), K(tmp_super_blk));
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_write_ss_super_block(tmp_super_blk, false/*is_format*/))) {
    FLOG_WARN("fail to do write ss_super_block", KR(ret), K(tmp_super_blk));
  } else if (OB_TMP_FAIL(super_blk_.assign(tmp_super_blk))) {
    LOG_WARN("fail to assign ss_super_blk", KR(tmp_ret), K(tmp_super_blk), K_(super_blk));
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block(
    ObSSMicroCacheSuperBlk &new_super_blk,
    const bool do_check,
    const int64_t exp_modify_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  if (OB_UNLIKELY(!new_super_blk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_super_blk));
  } else if (do_check && OB_UNLIKELY(exp_modify_time != super_blk_.modify_time_us_)) {
    ret = OB_EAGAIN;
    LOG_WARN("modify_time mismatch", KR(ret), K(exp_modify_time), K_(super_blk));
  } else if (OB_FAIL(inner_write_ss_super_block(new_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
  } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
    FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(new_super_blk), K_(super_blk));
  } 
  
  if (OB_SUCC(ret)) {
    FLOG_INFO("ss_micro_cache: succ to update ss_super_block", KR(tmp_ret), K_(super_blk), K(do_check),
      K(exp_modify_time));
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block_for_blk_ckpt(
    const ObSSMicroCacheSuperBlk &ckpt_super_blk)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  ObSSMicroCacheSuperBlk new_super_blk;
  if (OB_FAIL(inner_get_ss_super_block(new_super_blk))) {
    LOG_WARN("fail to inner get ss_super_block", KR(ret), K_(super_blk));
  } else if (OB_FAIL(new_super_blk.assign_for_blk_ckpt(ckpt_super_blk))) {
    LOG_WARN("fail to assign for blk_ckpt", KR(ret), K(new_super_blk), K(ckpt_super_blk));
  } else if (OB_FAIL(inner_write_ss_super_block(new_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
  } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
    FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(new_super_blk), K_(super_blk));
  }
  
  if (OB_SUCC(ret)) {
    FLOG_INFO("ss_micro_cache: succ to update ss_super_block for blk_ckpt");
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block_for_micro_ckpt(
    const int64_t idx, 
    const ObSSMicroCacheSuperBlk &ckpt_super_blk)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  ObSSMicroCacheSuperBlk new_super_blk;
  if (OB_FAIL(inner_get_ss_super_block(new_super_blk))) {
    LOG_WARN("fail to inner get ss_super_block", KR(ret), K_(super_blk));
  } else if (OB_FAIL(new_super_blk.assign_for_micro_ckpt(idx, ckpt_super_blk))) {
    LOG_WARN("fail to assign for micro_ckpt", KR(ret), K(idx), K(new_super_blk), K(ckpt_super_blk));
  } else if (OB_FAIL(inner_write_ss_super_block(new_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
  } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
    FLOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(new_super_blk), K_(super_blk));
  } 
  
  if (OB_SUCC(ret)) {
    FLOG_INFO("ss_micro_cache: succ to update ss_super_block for micro_ckpt", K(idx));
  }
  return ret;
}

int ObSSPhysicalBlockManager::exist_in_ss_super_block_ckpt_list(const int64_t blk_idx, bool &is_exist)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  is_exist = false;
  if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid phy_block idx", KR(ret), K(blk_idx), K_(blk_cnt_info));
  } else if (OB_FAIL(inner_check_ss_super_block())) {
    LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
  } else {
    is_exist = super_blk_.exist_in_ckpt_list(blk_idx);
  }
  return ret;
}

int ObSSPhysicalBlockManager::read_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // invoked when restart(micro_cache file already exist), no need to add resize_lock
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    if (OB_FAIL(inner_read_ss_super_block(ss_super_blk))) {
      LOG_WARN("fail to read ss_super_block", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(!ss_super_blk.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block is invalid", KR(ret), K(ss_super_blk));
    } else if (OB_TMP_FAIL(super_blk_.assign(ss_super_blk))) {
      LOG_WARN("fail to assign ss_super_block", KR(tmp_ret), K(ss_super_blk), K_(super_blk));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::inner_write_ss_super_block(
    ObSSMicroCacheSuperBlk &ss_super_blk,
    const bool is_format,
    const bool allow_failed)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  ObMemAttr attr(tenant_id_, "SSSuperBlk");
  const int64_t alignment = SS_MEM_BUF_ALIGNMENT;
  ss_super_blk.update_modify_time_us();
  const int64_t super_blk_size = ss_super_blk.get_serialize_size();

  ObSSPhyBlockCommonHeader common_header;
  if (OB_UNLIKELY(!ss_super_blk.is_valid() || block_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ss_super_blk), K_(block_size));
  } else if (OB_UNLIKELY(super_blk_size + common_header.header_size_ > block_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough", KR(ret), K_(block_size), K(super_blk_size), K(common_header));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc_align(alignment, block_size_, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(ss_super_blk), K(alignment), K(attr), K_(block_size));
  } else {
    int64_t pos = common_header.header_size_;
    if (OB_FAIL(ss_super_blk.serialize(buf, block_size_, pos))) {
      LOG_WARN("fail to serialize ss_super_block", KR(ret), K(ss_super_blk), K_(block_size), K(pos), KP(buf));
    } else {
      common_header.set_payload_size(pos - common_header.header_size_);
      common_header.set_block_type(ObSSPhyBlockType::SS_SUPER_BLK);
      common_header.calc_payload_checksum(buf + common_header.header_size_, common_header.payload_size_);
      pos = 0;
      if (OB_FAIL(common_header.serialize(buf, block_size_, pos))) {
        LOG_WARN("fail to serialize common header", KR(ret), K_(block_size), KP(buf));
      } else {
        if (OB_UNLIKELY(pos != common_header.header_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pos is unexpected", KR(ret), K(pos), K(common_header));
        } else {
          const int64_t total_size = pos + common_header.payload_size_;
          ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
          const int64_t align_size = lib::align_up(total_size, alignment);
          int64_t write_size = 0;
          const int32_t first_blk_write_index = (newer_super_blk_index_ + 1) % SS_SUPER_BLK_COUNT;
          const int64_t first_blk_offset = first_blk_write_index * block_size_;
          
          if (OB_ISNULL(tnt_file_mgr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant file manager should not be null", KR(ret));
          } else if (OB_FAIL(inner_do_write_ss_super_block(first_blk_offset, align_size, buf, write_size))) {
            // write 1st super_blk
            LOG_WARN("fail to write 1st ss_super_blk multi times", KR(ret), K(ss_super_blk), K(first_blk_write_index), K(first_blk_offset), K(is_format));
          } else {
            // update write_index if write successfully
            newer_super_blk_index_ = first_blk_write_index;
          }
          
          const int32_t second_blk_write_index = (newer_super_blk_index_ + 1) % SS_SUPER_BLK_COUNT;
          const int64_t second_blk_offset = second_blk_write_index * block_size_;
          // write 2nd super_blk only if 1st super_blk is written successfully
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(second_blk_write_index == first_blk_write_index)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("super_blk_write_index not change after write", KR(ret), K(first_blk_write_index), K(second_blk_write_index), K(SS_SUPER_BLK_COUNT));
          } else if (OB_FAIL(inner_do_write_ss_super_block(second_blk_offset, align_size, buf, write_size))) {
            LOG_WARN("fail to write 2nd ss_super_blk multi times", KR(ret), K(ss_super_blk), K(second_blk_write_index), K(second_blk_offset), K(is_format));
            // success to write 1st super_blk, but fail to write 2nd super_blk
            // we regard it is successful as long as write one blk
            ret = OB_SUCCESS;
          } else {
            // update write_index if write successfully
            newer_super_blk_index_ = second_blk_write_index;
          }
          
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tnt_file_mgr->fsync_cache_file())) {
            LOG_WARN("fail to fsync micro_cache data file", KR(ret));
          } else {
            FLOG_INFO("succ to persist super block", "ori_super_blk", super_blk_, "new_super_blk", ss_super_blk, K(pos),
              K(align_size), K(is_format));
          }
        }
      }
    }
  }

  // if we fail to persist when we want to update super_block's cache_file_size, we still return OB_SUCCESS.
  if (OB_FAIL(ret) && allow_failed) {
    ret = OB_SUCCESS;
    FLOG_INFO("although fail to write ss_super_blk, we will ignore ret", K(ss_super_blk), K(is_format));
  }

  if (nullptr != buf) {
    ob_free_align(buf);
    buf = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::inner_do_write_ss_super_block(
    const int64_t blk_offset,
    const int64_t align_size,
    const char *buf,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  const int32_t MAX_RETRY_TIME = 3;

  if (OB_UNLIKELY((blk_offset < 0) || (blk_offset >= (SS_SUPER_BLK_COUNT * block_size_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("blk_offset is invalid", KR(ret), K(blk_offset), K(SS_SUPER_BLK_COUNT));
  } else if (OB_ISNULL(tnt_file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager should not be null", KR(ret));
  } else {
    int32_t retry_cnt = 0;
    ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_SUPER_BLK_ERR;

    if (OB_FAIL(ret)) {
      LOG_INFO("sim persist super_blk write error", KR(ret), K(blk_offset));
    } else {
      do {
        ret = OB_SUCCESS; // set ret to OB_SUCCESS, and it will be set to error when fail persisting
        if (OB_FAIL(tnt_file_mgr->pwrite_cache_block(blk_offset, align_size, buf, write_size))) {
          LOG_WARN("fail to persist ss_super_block", KR(ret), K(retry_cnt), K(MAX_RETRY_TIME), K(align_size), K(write_size));
          ob_usleep(SS_LOOP_SLEEP_INTERVAL_US);
          if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
            FLOG_WARN("still wait for persist ss_super_block", KR(ret));
          }
        }
        retry_cnt++;
      } while (OB_FAIL(ret) && retry_cnt < MAX_RETRY_TIME);
    }
  }

  return ret;
}

int ObSSPhysicalBlockManager::inner_read_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "SSSuperBlk");
  const int64_t align_size = lib::align_up(ss_super_blk.get_serialize_size(), SS_MEM_BUF_ALIGNMENT);
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, align_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(align_size), K(attr));
  } else {
    ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
    int64_t read_size = 0;
    bool is_blk_updated = false;
    bool is_read_succ = false;
    int32_t newer_super_blk_index = ATOMIC_LOAD(&newer_super_blk_index_);
    if (OB_UNLIKELY(newer_super_blk_index < 0 || newer_super_blk_index >= SS_SUPER_BLK_COUNT)) {
      LOG_ERROR("newer_super_blk_index is invalid", K(newer_super_blk_index), K(SS_SUPER_BLK_COUNT));
      ATOMIC_STORE(&newer_super_blk_index_, 0); // reset newer_super_blk_index to 0;
      newer_super_blk_index = 0;
    }

    // read 1st super_blk
    const int32_t first_blk_read_index = newer_super_blk_index;
    const int64_t first_blk_offset = first_blk_read_index * block_size_;
    if (OB_ISNULL(tnt_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager should not be null", KR(ret));
    } else if (OB_TMP_FAIL(tnt_file_mgr->pread_cache_block(first_blk_offset, align_size, buf, read_size))) {
      LOG_WARN("fail to read ss_super_block", KR(tmp_ret), K(first_blk_read_index), K(first_blk_offset), K(align_size));
      tmp_ret = OB_SUCCESS;
    } else if (OB_TMP_FAIL(inner_build_ss_super_block(ss_super_blk, buf, read_size, is_blk_updated))) {
      LOG_WARN("fail to build ss_super_block", KR(tmp_ret), K(first_blk_read_index), K(first_blk_offset), K(read_size));
      tmp_ret = OB_SUCCESS;
    } else if (OB_LIKELY(is_blk_updated)) {
      ATOMIC_STORE(&newer_super_blk_index_, first_blk_read_index); // means super_blk is newer or valid
      is_read_succ = true;
    }
    
    // read 2nd super_blk
    const int32_t second_blk_read_index = (first_blk_read_index + 1) % SS_SUPER_BLK_COUNT;
    const int64_t second_blk_offset = second_blk_read_index * block_size_;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(second_blk_read_index == first_blk_read_index)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("super_blk_read_index not change after write", KR(tmp_ret), K(first_blk_read_index), K(second_blk_read_index), K(SS_SUPER_BLK_COUNT));
    } else if (OB_TMP_FAIL(tnt_file_mgr->pread_cache_block(second_blk_offset, align_size, buf, read_size))) {
      LOG_WARN("fail to read backup ss_super_block", KR(tmp_ret), K(second_blk_read_index), K(second_blk_offset), K(align_size), K_(block_size));
    } else if (OB_TMP_FAIL(inner_build_ss_super_block(ss_super_blk, buf, read_size, is_blk_updated))) {
      LOG_WARN("fail to try check backup ss_super_block", KR(tmp_ret), K(second_blk_read_index), K(second_blk_offset), K(read_size), K_(block_size));
    } else if (OB_LIKELY(is_blk_updated)) {
      ATOMIC_STORE(&newer_super_blk_index_, second_blk_read_index); // means 2nd super_blk is newer or valid
      is_read_succ = true;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(is_read_succ)) {
      // the reading process is considered successful as long as one super_blk is read successfully
      ret = OB_SUCCESS;
    } else {
      // fail to read both super_blk, return the error code of 2nd super_blk, and ignore the error code of 1st super_blk
      ret = tmp_ret;
    } 

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!ss_super_blk.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get valid ss_super_block", KR(ret), K(ss_super_blk));
    } 
  }
  
  if (nullptr != buf) {
    ob_free_align(buf);
    buf = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::inner_build_ss_super_block(
    ObSSMicroCacheSuperBlk &ss_super_blk,
    char *read_buf, 
    const int64_t read_size,
    bool& is_blk_updated)
{
  int ret = OB_SUCCESS;
  is_blk_updated = false;
  int64_t pos = 0;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroCacheSuperBlk tmp_super_blk;
  if (OB_FAIL(common_header.deserialize(read_buf, read_size, pos))) {
    LOG_WARN("fail to deserialize common header", KR(ret), KP(read_buf), K(read_size), K(pos));
  } else if (OB_UNLIKELY(common_header.header_size_ != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos is unexpected", KR(ret), K(common_header), K(pos));
  } else if (OB_UNLIKELY(!common_header.is_valid() || (!common_header.is_super_blk()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized common header is invalid or wrong type", KR(ret), K(common_header));
  } else if (OB_FAIL(common_header.check_payload_checksum(read_buf + pos, common_header.payload_size_))) {
    LOG_WARN("fail to check common header payload checksum", KR(ret), K(common_header), KP(read_buf), K(pos));
  } else if (OB_FAIL(tmp_super_blk.deserialize(read_buf, read_size, pos))) {
    LOG_WARN("fail to deserialize super_block", KR(ret), KP(read_buf), K(read_size), K(pos));
  } else if (OB_UNLIKELY(tmp_super_blk.tenant_id_ != tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant_id in deserialized super_block", KR(ret), K_(tenant_id), K(tmp_super_blk), K(pos));
  } else if (OB_FAIL(tmp_super_blk.init_unpersisted_info())) {
    LOG_WARN("fail to init unpersisted info", KR(ret), K(tmp_super_blk));
  } else if (OB_UNLIKELY(!tmp_super_blk.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized super_block is invalid", KR(ret), K_(tenant_id), K(tmp_super_blk), K(pos));
  } else {
    ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_READ_SUPER_BLK_ERR;
  }

  if (OB_SUCC(ret)) {
    if (!ss_super_blk.is_valid()) {
      if (OB_FAIL(ss_super_blk.assign(tmp_super_blk))) {
        LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
      } else {
        is_blk_updated = true;
      }
    } else if (ss_super_blk.modify_time_us() < tmp_super_blk.modify_time_us()) {
      if (OB_FAIL(ss_super_blk.assign(tmp_super_blk))) {
        LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
      } else {
        is_blk_updated = true;
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::inner_get_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_check_ss_super_block())) {
    LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
  } else if (OB_FAIL(ss_super_blk.assign(super_blk_))) {
    FLOG_WARN("fail to assign ss_super_block", KR(ret), K_(super_blk));
  }
  return ret;
}

// need w_resize_lock to protect
int ObSSPhysicalBlockManager::inner_check_ss_super_block()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(super_blk_.is_valid())) {
    // do nothing
  } else {
    ObSSMicroCacheSuperBlk new_super_blk;
    if (OB_FAIL(inner_read_ss_super_block(new_super_blk))) {
      LOG_WARN("fail to read ss_super_block", KR(ret), K_(tenant_id), K_(super_blk));
    } else if (OB_FAIL(super_blk_.assign(new_super_blk))) {
      FLOG_WARN("fail to assign ss_super_block", KR(ret), K(new_super_blk), K_(super_blk));
    } else if (OB_UNLIKELY(!super_blk_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block should be valid", KR(ret), K_(super_blk));
    }
  }
  return ret;
}

// Reset super_blk's meta_ckpt_info to reuse all meta_blks
// 1. Save old meta_ckpt_info and reset super_blk's meta_ckpt_info (with lock)
// 2. Release lock and reuse meta_blks using old meta_ckpt_info
int ObSSPhysicalBlockManager::reuse_meta_blks_on_meta_ckpt_reset(const uint32_t micro_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCkptInfo old_micro_ckpt_info;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    // Step 1: Save old meta_ckpt_info and reset super_blk (with write lock)
    {
      SpinWLockGuard resize_guard(resize_lock_);
      if (OB_FAIL(inner_check_ss_super_block())) {
        LOG_WARN("fail to check ss_super_block", KR(ret), K_(super_blk));
      } else if (OB_FAIL(old_micro_ckpt_info.assign(super_blk_.micro_ckpt_info_, false/*need_reset*/))) {
        LOG_WARN("fail to save old meta_ckpt_info", KR(ret));
      } else {
        // Reset super_blk's meta_ckpt_info
        ObSSMicroCacheSuperBlk new_super_blk;
        if (OB_FAIL(new_super_blk.assign(super_blk_))) {
          LOG_WARN("fail to assign ss_super_block for reset", KR(ret), K(new_super_blk), K_(super_blk));
        } else if (OB_FAIL(new_super_blk.reinit_meta_ckpt_info(tenant_id_, micro_ckpt_split_cnt))) {
          LOG_WARN("fail to reinit meta_ckpt_info", KR(ret), K(tenant_id_), K(micro_ckpt_split_cnt));
        } else if (OB_FAIL(inner_write_ss_super_block(new_super_blk, false/*is_format*/))) {
          LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
        } else if (OB_TMP_FAIL(super_blk_.assign(new_super_blk))) {
          LOG_WARN("fail to assign ss_super_blk", KR(tmp_ret), K(new_super_blk), K_(super_blk));
        } else {
          FLOG_INFO("succ to reset ss_super_block about meta_ckpt_info", K(micro_ckpt_split_cnt), K_(super_blk));
        }
      }
    }
    
    // Step 2: Reuse meta_blks using old meta_ckpt_info
    if (OB_FAIL(ret)) {
    } else {
      const int64_t meta_ckpt_used_blks_cnt = old_micro_ckpt_info.micro_ckpt_used_blks_.count();
      for (int64_t idx = 0; OB_SUCC(ret) && (idx < meta_ckpt_used_blks_cnt); ++idx) {
        if (OB_FAIL(add_batch_reusable_blocks(old_micro_ckpt_info.micro_ckpt_used_blks_.at(idx)))) {
          LOG_ERROR("fail to add batch reusable phy_blocks", KR(ret), K(idx), K(old_micro_ckpt_info));
        }
      }
    }
  }
  return ret;
}

// Invoked when replay checkpoint, thus no need to add lock
int ObSSPhysicalBlockManager::update_blk_ckpt_used_block_list(const ObIArray<int64_t> &blk_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!blk_list.empty())) {
    if (OB_FAIL(super_blk_.set_blk_ckpt_blk_list(blk_list))) {
      LOG_WARN("fail to set blk_ckpt used_blk list", KR(ret), "blk_cnt", blk_list.count(), K(blk_list));
    }
  }
  return ret;
}

// Invoked when replay checkpoint, thus no need to add lock
int ObSSPhysicalBlockManager::update_micro_ckpt_used_block_list(const int64_t idx, const ObIArray<int64_t> &blk_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!blk_list.empty())) {
    if (OB_FAIL(super_blk_.set_micro_ckpt_blk_list(idx, blk_list))) {
      LOG_WARN("fail to set micro_ckpt used_blk list", KR(ret), K(idx), "blk_cnt", blk_list.count(), K(blk_list));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::read_persisted_sub_range(
    char *buf,
    const int64_t phy_blk_idx,
    const int64_t offset, 
    const int64_t size,
    const uint64_t reuse_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(phy_blk_idx < SS_SUPER_BLK_COUNT || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx), K(offset), K(size), KP(buf));
  } else {
    ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(phy_blk_idx);
    if (OB_ISNULL(phy_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should be valid", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
    } else if (OB_UNLIKELY(reuse_version != phy_blk->reuse_version_)) {
      ret = OB_S2_REUSE_VERSION_MISMATCH;
      LOG_WARN("unexpected reuse_version", KR(ret), K(reuse_version), K(phy_blk_idx), K(offset), K(size), KPC(phy_blk));
    } else {
      ObSSPhyBlockHandle phy_blk_handle;
      phy_blk_handle.set_ptr(phy_blk);
      const int64_t file_offset = phy_blk_idx * block_size_ + offset;
      if (OB_FAIL(ObSSMicroCacheIOHelper::read_block(file_offset, size, buf, phy_blk_handle))) {
        LOG_WARN("fail to read micro_cache block", KR(ret), K(file_offset), K(size), K(phy_blk_idx), KP(buf));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::divide_cache_data_block_range(
    const ObLSID &ls_id,
    const int64_t split_count,
    ObIArray<ObSSPhyBlockIdxRange> &blk_idx_ranges)
{
  // physical block is not organized by ls_id now, hence cannot filter by ls_id here now
  UNUSED(ls_id);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(split_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(split_count));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);
    SpinRLockGuard info_guard(info_lock_);

    const int64_t data_blk_used_cnt = blk_cnt_info_.data_blk_.used_cnt_;
    const int64_t block_cnt_per_range = (data_blk_used_cnt + split_count - 1) / split_count;
    int64_t cur_split_count = 0;
    int64_t block_cnt = 0;
    int64_t start_blk_idx = -1;

    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_)
        && (cur_split_count < split_count); ++i) {
      ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(i);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else if (phy_blk->is_valid_micro_data_block()) {
        ++block_cnt;
        if (-1 == start_blk_idx) {
          // left-open and right-closed intervals, thus i plus 1 here
          start_blk_idx = i - 1;
        }
      }

      // if reach block_cnt_per_range or iterate end, then push back block range
      if (OB_FAIL(ret)) {
      } else if (((block_cnt == block_cnt_per_range) || (i == (blk_cnt_info_.total_blk_cnt_ - 1))) &&
                (-1 != start_blk_idx)) {
        ObSSPhyBlockIdxRange blk_idx_range(start_blk_idx, i);
        if (OB_FAIL(blk_idx_ranges.push_back(blk_idx_range))) {
          LOG_WARN("fail to push back", KR(ret), K(blk_idx_range));
        } else {
          block_cnt = 0;
          start_blk_idx = -1;
          ++cur_split_count;
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_block_valid_length(
    const uint64_t data_loc, 
    const uint64_t reuse_version,
    const int32_t delta_len,
    int64_t &blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(data_loc < 1 || delta_len == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_loc), K(delta_len), K(reuse_version));
  } else {
    ObSSPhysicalBlock *phy_blk = nullptr;
    blk_idx = data_loc / block_size_;
    SpinRLockGuard resize_guard(resize_lock_);

    if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("invalid phy_block idx", KR(ret), K(blk_idx), K_(blk_cnt_info));
    } else if (FALSE_IT(phy_blk = inner_get_phy_block_by_idx(blk_idx))) {
    } else if (OB_NOT_NULL(phy_blk)) {
      const uint64_t cur_reuse_version = phy_blk->get_reuse_version();
      if (OB_UNLIKELY(reuse_version != cur_reuse_version)) {
        ret = OB_S2_REUSE_VERSION_MISMATCH;
        LOG_TRACE("phy_block maybe reused", KR(ret), K(reuse_version), K(cur_reuse_version), 
          K(blk_idx), KPC(phy_blk));
      } else if (delta_len < 0 && OB_FAIL(phy_blk->dec_valid_len(blk_idx, delta_len))) {
        LOG_WARN("fail to dec valid_len", KR(ret), K(blk_idx), K(data_loc), K(reuse_version), K(delta_len));
      } else if (delta_len > 0 && OB_FAIL(phy_blk->inc_valid_len(blk_idx, delta_len))) {
        LOG_WARN("fail to inc valid_len", KR(ret), K(blk_idx), K(data_loc), K(reuse_version), K(delta_len));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_handle(
    const uint64_t data_loc, 
    const uint64_t exp_reuse_version, 
    ObSSPhyBlockHandle &phy_blk_handle,
    uint64_t &real_reuse_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(data_loc <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_loc));
  } else {
    ObSSPhysicalBlock *phy_blk = nullptr;
    const int64_t blk_idx = data_loc / block_size_;
    SpinRLockGuard resize_guard(resize_lock_);

    if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("invalid phy_block idx", KR(ret), K(blk_idx), K(data_loc), K_(block_size), K_(blk_cnt_info));
    } else if (FALSE_IT(phy_blk = inner_get_phy_block_by_idx(blk_idx))) {
    } else if (OB_NOT_NULL(phy_blk)) {
      real_reuse_version = phy_blk->get_reuse_version();
      if (OB_UNLIKELY(exp_reuse_version != real_reuse_version)) {
        ret = OB_S2_REUSE_VERSION_MISMATCH;
        LOG_TRACE("phy_block reuse_version mismatch", KR(ret), K(data_loc), K(exp_reuse_version), K(blk_idx),
          K(real_reuse_version), KPC(phy_blk));
      } else {
        phy_blk_handle.set_ptr(phy_blk);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx), K(data_loc), K_(block_size));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_handle(
    const int64_t blk_idx, 
    ObSSPhyBlockHandle &phy_blk_handle)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_block_idx(blk_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_idx));
  } else {
    ObSSPhysicalBlock *phy_blk = inner_get_phy_block_by_idx(blk_idx);
    if (OB_ISNULL(phy_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx));
    } else {
      phy_blk_handle.set_ptr(phy_blk);
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_info(
    const int64_t blk_idx, 
    ObSSPhysicalBlock& phy_blk_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    ObSSPhyBlockHandle phy_blk_handle;
    if (OB_FAIL(get_block_handle(blk_idx, phy_blk_handle))) {
      LOG_WARN("fail to get block_handle", KR(ret), K(blk_idx));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle should be valid", KR(ret), K(blk_idx), K(phy_blk_handle));
    } else {
      phy_blk_info = *(phy_blk_handle.get_ptr());
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::scan_blocks_to_ckpt(ObIArray<ObSSPhyBlockReuseInfo> &reuse_info_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObSSPhysicalBlock *phy_blk = nullptr;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      phy_blk = inner_get_phy_block_by_idx(i);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        ObSSPhyBlockReuseInfo reuse_info(i);
        reuse_info.reuse_version_ = phy_blk->get_reuse_version();
        if (OB_UNLIKELY(!reuse_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_blk's reuse_info is invalid", KR(ret), K(i), K(reuse_info), KPC(phy_blk));
        } else if (OB_FAIL(reuse_info_arr.push_back(reuse_info))) {
          LOG_WARN("fail to push back reuse_info", KR(ret), K(i), K(reuse_info), KPC(phy_blk));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::read_phy_block_checkpoint(
    ObSSCkptPhyBlockItemReader &item_reader,
    ObIArray<int64_t> &ckpt_blk_id_list)
{
  int ret = OB_SUCCESS;
  int64_t replay_blk_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!item_reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next phy_block persist_info item", KR(ret), K_(tenant_id));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ObSSPhysicalBlock *phy_blk = nullptr;
        ObSSPhyBlockInfoCkptItem persist_info;
        int64_t pos = 0;
        if (OB_FAIL(persist_info.deserialize(item_buf, item_buf_len, pos))) {
          LOG_WARN("fail to deserialize", KR(ret), K(item_buf_len), KP(item_buf), K(replay_blk_cnt));
        } else if (OB_UNLIKELY(!persist_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block persist info should be valid", KR(ret), K(persist_info), K(replay_blk_cnt));
        } else if (FALSE_IT(phy_blk = inner_get_phy_block_by_idx(persist_info.blk_idx_))) {
        } else if (OB_ISNULL(phy_blk)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block should not be null", KR(ret), K(persist_info), K_(blk_cnt_info), K(replay_blk_cnt));
        } else {
          phy_blk->set_reuse_version(persist_info.reuse_version_);
          ++replay_blk_cnt;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_block_id_list(ckpt_blk_id_list))) {
        LOG_WARN("fail to get block_id list", KR(ret));
      }
    }
  }
  FLOG_INFO("ss_micro_cache: finish read phy_blk ckpt", KR(ret), "ckpt_blk_cnt", ckpt_blk_id_list.count(), 
    K(replay_blk_cnt), K_(blk_cnt_info));
  return ret;
}


// Only used for reading phy_block's checkpoint
int ObSSPhysicalBlockManager::update_block_state_for_restart()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    // NOTICE: one thread, no need to add lock
    if (OB_FAIL(inner_set_ckpt_blk_type())) {
      LOG_WARN("fail to inner set ckpt blk type", KR(ret), K_(super_blk));
    }

    ObSSPhysicalBlock *phy_blk = nullptr;
    for (int64_t i = SS_SUPER_BLK_COUNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      phy_blk = inner_get_phy_block_by_idx(i);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_INVALID_TYPE;
        if (phy_blk->get_valid_len() > 0) {
          // When execute micro_meta ckpt, there may exists valid micro_meta in reorgan_phy_block.
          // Thus when replay ckpt, the count of micro_data_blk may exceed (data_blk_cnt - reorgan_blk_cnt)
          blk_type = ObSSPhyBlockType::SS_REORGAN_BLK;
        } else {
          blk_type = phy_blk->get_block_type();
        }

        if (ObSSPhyBlockType::SS_INVALID_TYPE != blk_type) {
          // Cuz we didn't record ckpt_blk's valid_len, so make it greated than 0 to prevent it from being reused.
          if (is_ss_ckpt_block(blk_type)) {
            phy_blk->set_valid_len(1);
          }
          
          if (OB_FAIL(blk_cnt_info_.increase_blk_used_cnt(blk_type))) {
            LOG_WARN("fail to increase blk_used_cnt", KR(ret), K(i), K(blk_type));
          } else {
            free_bitmap_->set(i, false);
            phy_blk->force_mark_used(blk_type, true/*is_sealed*/);
          }
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::inner_set_ckpt_blk_type()
{
  int ret = OB_SUCCESS;
  ObSSPhysicalBlock *phy_blk = nullptr;
  // 1. set blk_ckpt type
  const int64_t used_blk_cnt = super_blk_.blk_ckpt_used_blk_list().count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < used_blk_cnt); ++i) {
    phy_blk = nullptr;
    const int64_t blk_idx = super_blk_.blk_ckpt_used_blk_list().at(i);
    phy_blk = inner_get_phy_block_by_idx(blk_idx);
    if (OB_ISNULL(phy_blk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(i), K(blk_idx), K(used_blk_cnt), K_(super_blk));
    } else {
      phy_blk->set_block_type(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK);
    }
  }

  // 2. set micro_ckpt type
  const int64_t micro_ckpt_list_cnt = super_blk_.get_micro_ckpt_used_blk_list_cnt();
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_ckpt_list_cnt); ++i) {
    const int64_t cur_used_blk_cnt = super_blk_.micro_ckpt_used_blk_list_by_idx(i).count();
    for (int64_t j = 0; OB_SUCC(ret) && (j < cur_used_blk_cnt); ++j) {
      phy_blk = nullptr;
      const int64_t blk_idx = super_blk_.micro_ckpt_used_blk_list_by_idx(i).at(j);
      phy_blk = inner_get_phy_block_by_idx(blk_idx);
      if (OB_ISNULL(phy_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K(j), K(blk_idx), K(cur_used_blk_cnt), K_(super_blk));
      } else {
        phy_blk->set_block_type(ObSSPhyBlockType::SS_MICRO_META_BLK);
      }
    }
  }
  return ret;
}

void ObSSPhysicalBlockManager::set_phy_blk_stat(const int64_t cache_file_size)
{
  cache_stat_.phy_blk_stat().set_cache_file_size(cache_file_size);
  cache_stat_.phy_blk_stat().set_block_size(block_size_);
  cache_stat_.phy_blk_stat().set_super_block_cnt(blk_cnt_info_.super_blk_cnt_);
  cache_stat_.phy_blk_stat().set_total_block_cnt(blk_cnt_info_.total_blk_cnt_);
  cache_stat_.phy_blk_stat().set_shared_block_cnt(blk_cnt_info_.shared_blk_cnt_);
  cache_stat_.phy_blk_stat().set_phy_ckpt_block_cnt(blk_cnt_info_.phy_ckpt_blk_cnt_);

  cache_stat_.phy_blk_stat().set_data_block_cnt(blk_cnt_info_.data_blk_.hold_cnt_);
  cache_stat_.phy_blk_stat().set_meta_block_cnt(blk_cnt_info_.meta_blk_.hold_cnt_);
  cache_stat_.phy_blk_stat().set_shared_block_used_cnt(blk_cnt_info_.shared_blk_used_cnt_);
}

int64_t ObSSPhysicalBlockManager::get_data_block_used_cnt() const
{
  SpinRLockGuard info_guard(info_lock_);
  return blk_cnt_info_.data_blk_.used_cnt_;
}

int64_t ObSSPhysicalBlockManager::get_data_block_max_cnt() const
{
  // resize_lock is to protect 'shared_blk_cnt_'
  // info_lock is to protect 'data_blk_.hold_cnt_' and 'shared_blk_used_cnt_'
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  return blk_cnt_info_.micro_data_blk_max_cnt();
}

int64_t ObSSPhysicalBlockManager::get_meta_block_min_cnt() const
{
  SpinRLockGuard resize_guard(resize_lock_);
  return blk_cnt_info_.meta_blk_.min_cnt_;
}

int64_t ObSSPhysicalBlockManager::get_cache_limit_size() const
{
  // resize_lock is to protect 'shared_blk_cnt_'
  // info_lock is to protect 'data_blk_.hold_cnt_' and 'shared_blk_used_cnt_'
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  const int64_t data_blk_max_cnt = blk_cnt_info_.micro_data_blk_max_cnt();
  return data_blk_max_cnt * block_size_;
}

int64_t ObSSPhysicalBlockManager::get_total_blk_cnt() const
{
  // resize_lock is to protect 'total_blk_cnt_'
  SpinRLockGuard resize_guard(resize_lock_);
  return blk_cnt_info_.total_blk_cnt_;
}

int ObSSPhysicalBlockManager::get_and_update_cache_limit_size(const bool need_update_reorgan_pct)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  if (need_update_reorgan_pct) {
    double new_reorgan_pct = static_cast<double>(SSMicroMetaMgr.get_arc_limit_pct()) / 100 + 0.02;
    if (new_reorgan_pct > 0.85) {
      LOG_WARN("blk_reorgan_pct is too high, please check MIN_REOEGAN_BLK_CNT", K(new_reorgan_pct));
    }
    phy_blk_reorgan_pct_ = new_reorgan_pct;
  }

  const int64_t cache_limit_size = blk_cnt_info_.micro_data_blk_max_cnt() * block_size_;
  if (OB_FAIL(SSMicroMetaMgr.update_arc_limit(cache_limit_size))) {
    LOG_WARN("fail to update arc limit", KR(ret), K(cache_limit_size));
  }
  return ret;
}

void ObSSPhysicalBlockManager::get_data_block_info(int64_t &data_block_max_cnt, int64_t &data_blk_usage_pct) const
{
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  data_block_max_cnt = blk_cnt_info_.micro_data_blk_max_cnt();
  data_blk_usage_pct = blk_cnt_info_.data_blk_.used_cnt_ * 100 / data_block_max_cnt;
}

void ObSSPhysicalBlockManager::get_blk_ckpt_block_info(int64_t &blk_ckpt_cnt, int64_t &blk_cnpt_used_cnt) const
{
  SpinRLockGuard resize_guard(resize_lock_);
  blk_ckpt_cnt = blk_cnt_info_.phy_ckpt_blk_cnt_;
  blk_cnpt_used_cnt = blk_cnt_info_.phy_ckpt_blk_used_cnt_;
}

bool ObSSPhysicalBlockManager::exist_free_block(const ObSSPhyBlockType blk_type) const
{
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  return blk_cnt_info_.has_free_blk(blk_type);
}

} // namespace storage
} // namespace oceanbase
