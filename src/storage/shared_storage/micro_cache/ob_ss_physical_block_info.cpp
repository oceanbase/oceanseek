/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
using namespace oceanbase::share;

bool is_ss_ckpt_block(const ObSSPhyBlockType blk_type)
{
  return (ObSSPhyBlockType::SS_MICRO_META_BLK == blk_type) || (ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK == blk_type);
}

ObSSPhyBlockType get_ss_block_persist_type(const ObSSPhyBlockType blk_type)
{
  return ((ObSSPhyBlockType::SS_REORGAN_BLK == blk_type) ? ObSSPhyBlockType::SS_MICRO_DATA_BLK : blk_type);
}

/*-----------------------------------------ObSSPhyBlockCountInfo::DynamicCntInfo------------------------------------------*/
void ObSSPhyBlockCountInfo::DynamicCntInfo::reset()
{
  hold_cnt_ = 0;
  used_cnt_ = 0;
  min_cnt_ = 0;
  max_cnt_ = 0;
}

void ObSSPhyBlockCountInfo::DynamicCntInfo::reuse()
{
  used_cnt_ = 0;
}

/*-----------------------------------------ObSSPhyBlockCountInfo------------------------------------------*/
ObSSPhyBlockCountInfo::ObSSPhyBlockCountInfo(ObSSMicroCacheStat &cache_stat) 
  : cache_stat_(cache_stat)
{
  reset();
}

void ObSSPhyBlockCountInfo::reset()
{
  total_blk_cnt_ = 0;
  super_blk_cnt_ = 0;
  shared_blk_cnt_ = 0;
  shared_blk_used_cnt_ = 0;
  phy_ckpt_blk_cnt_ = 0;
  phy_ckpt_blk_used_cnt_ = 0;
  reorgan_blk_cnt_ = 0;
  meta_blk_.reset();
  data_blk_.reset();
}

void ObSSPhyBlockCountInfo::reuse()
{
  phy_ckpt_blk_used_cnt_ = 0;
  data_blk_.reuse();
  meta_blk_.reuse();
}

bool ObSSPhyBlockCountInfo::is_valid() const
{
  return (total_blk_cnt_ > 0) && (super_blk_cnt_ > 0) && (shared_blk_cnt_ >= shared_blk_used_cnt_) &&
         (shared_blk_used_cnt_ == meta_blk_.hold_cnt_ + data_blk_.hold_cnt_) && (phy_ckpt_blk_cnt_ > 0) &&
         (phy_ckpt_blk_used_cnt_ >= 0) && meta_blk_.is_valid() && data_blk_.is_valid();
}

int64_t ObSSPhyBlockCountInfo::calc_phy_ckpt_blk_cnt(const int32_t block_size) const
{
  int64_t blk_cnt = 0;
  const int32_t phy_ckpt_item_size = ObSSCkptPhyBlockItemHeader::get_max_serialize_size() + 
                                     ObSSPhyBlockInfoCkptItem::get_max_serialize_size();
  if (phy_ckpt_item_size > 0) {
    ObSSPhyBlockCommonHeader common_header;
    ObSSCkptBlockHeader ckpt_blk_header;
    const int32_t header_size = common_header.get_serialize_size() + ckpt_blk_header.get_serialize_size();
    const int32_t each_blk_item_cnt = (block_size - header_size) / phy_ckpt_item_size;
    if (each_blk_item_cnt > 0) {
      const int64_t phy_ckpt_blk_cnt = ((total_blk_cnt_ + each_blk_item_cnt - 1) / each_blk_item_cnt);
      blk_cnt = MAX(phy_ckpt_blk_cnt * 2, 2);
    }
  }
  return blk_cnt;
}

void ObSSPhyBlockCountInfo::init_blk_cnt_info(
    const int64_t total_blk_cnt,
    const int64_t block_size,
    const bool is_mini_cache)
{
  total_blk_cnt_ = total_blk_cnt;
  super_blk_cnt_ = SS_SUPER_BLK_COUNT;
  phy_ckpt_blk_cnt_ = calc_phy_ckpt_blk_cnt(block_size);
  shared_blk_cnt_ = total_blk_cnt_ - super_blk_cnt_ - phy_ckpt_blk_cnt_;

  const int64_t new_reorgan_blk_cnt = (is_mini_cache ? SS_MIN_RESERVE_REORGAN_BLK_CNT : SS_MAX_RESERVE_REORGAN_BLK_CNT);
  reorgan_blk_cnt_ = MAX(reorgan_blk_cnt_, new_reorgan_blk_cnt);
  data_blk_.min_cnt_ = MAX(reorgan_blk_cnt_, shared_blk_cnt_ * SS_MIN_MICRO_DATA_BLK_CNT_PCT / 100);
  meta_blk_.min_cnt_ = MAX(2, shared_blk_cnt_ * SS_MIN_MICRO_META_BLK_CNT_PCT / 100);
  
  data_blk_.max_cnt_ = shared_blk_cnt_ - meta_blk_.min_cnt_;
  meta_blk_.max_cnt_ = shared_blk_cnt_ - data_blk_.min_cnt_;

  // if resize cache_file_size, must ensure hold_cnt_ >= min_cnt_
  meta_blk_.hold_cnt_ = MAX(meta_blk_.hold_cnt_, meta_blk_.min_cnt_);
  data_blk_.hold_cnt_ = MAX(data_blk_.hold_cnt_, data_blk_.min_cnt_);
  shared_blk_used_cnt_ = meta_blk_.hold_cnt_ + data_blk_.hold_cnt_;
}

void ObSSPhyBlockCountInfo::try_reserve_dynamic_blk(
    const ObSSPhyBlockType type, 
    const int64_t expect_cnt, 
    int64_t &available_cnt)
{
  available_cnt = 0;
  if (is_dynamic_blk_type(type)) {
    DynamicCntInfo &dynamic_blk = (type == ObSSPhyBlockType::SS_MICRO_DATA_BLK ? data_blk_ : meta_blk_);
    if (expect_cnt <= dynamic_blk.free_blk_cnt()) {
      // don't need to reserve extra block from shared_block
      available_cnt = expect_cnt;
    } else {
      const int64_t delta_cnt = expect_cnt - dynamic_blk.free_blk_cnt();
      const int64_t limit_cnt = dynamic_blk.max_cnt_ - dynamic_blk.hold_cnt_;
      const int64_t shared_free_cnt = shared_blk_free_cnt();
      const int64_t extra_reserve_cnt = MIN(delta_cnt, MIN(limit_cnt, shared_free_cnt));

      shared_blk_used_cnt_ += extra_reserve_cnt;
      dynamic_blk.hold_cnt_ += extra_reserve_cnt;

      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(extra_reserve_cnt);
      if (type == ObSSPhyBlockType::SS_MICRO_DATA_BLK) {
        cache_stat_.phy_blk_stat().update_data_block_cnt(extra_reserve_cnt);
      } else if (type == ObSSPhyBlockType::SS_MICRO_META_BLK) {
        cache_stat_.phy_blk_stat().update_meta_block_cnt(extra_reserve_cnt);
      }
      available_cnt = dynamic_blk.free_blk_cnt();
    }
  }
}

void ObSSPhyBlockCountInfo::update_blk_used_cnt(
    const ObSSPhyBlockType type, 
    const int64_t delta_cnt, 
    const bool update_shared_blk)
{
  if (ObSSPhyBlockType::SS_MICRO_DATA_BLK == type) {
    if (update_shared_blk) {
      shared_blk_used_cnt_ += delta_cnt;
      data_blk_.hold_cnt_ += delta_cnt;
      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(delta_cnt);
      cache_stat_.phy_blk_stat().update_data_block_cnt(delta_cnt);
    }
    data_blk_.used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_data_block_used_cnt(delta_cnt);
  } else if (ObSSPhyBlockType::SS_MICRO_META_BLK == type) {
    if (update_shared_blk) {
      shared_blk_used_cnt_ += delta_cnt;
      meta_blk_.hold_cnt_ += delta_cnt;
      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(delta_cnt);
      cache_stat_.phy_blk_stat().update_meta_block_cnt(delta_cnt);
    }
    meta_blk_.used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_meta_block_used_cnt(delta_cnt);
  } else if (ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK == type) {
    phy_ckpt_blk_used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_phy_ckpt_block_used_cnt(delta_cnt);
  }
}

int ObSSPhyBlockCountInfo::increase_blk_used_cnt(const ObSSPhyBlockType blk_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_TYPE == blk_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(blk_type));
  } else if (ObSSPhyBlockType::SS_MICRO_DATA_BLK == blk_type || ObSSPhyBlockType::SS_REORGAN_BLK == blk_type) {
    if ((ObSSPhyBlockType::SS_MICRO_DATA_BLK == blk_type && data_blk_.free_blk_cnt() > reorgan_blk_cnt_) ||
        (ObSSPhyBlockType::SS_REORGAN_BLK == blk_type && data_blk_.free_blk_cnt() > 0)) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_DATA_BLK, 1, /*update_shared_blk*/false);
    } else if (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_DATA_BLK, 1, /*update_shared_blk*/true);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(blk_type), KPC(this));
    }

    const int64_t delta_cnt = (OB_SUCC(ret) && ObSSPhyBlockType::SS_REORGAN_BLK == blk_type) ? 1 : 0;
    cache_stat_.phy_blk_stat().update_reorgan_block_used_cnt(delta_cnt);
  } else if (ObSSPhyBlockType::SS_MICRO_META_BLK == blk_type) {
    if (meta_blk_.free_blk_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_BLK, 1, /*update_shared_blk*/false);
    } else if (!meta_blk_.reach_limit() && shared_blk_free_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_BLK, 1, /*update_shared_blk*/true);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(blk_type), KPC(this));
    }
  } else if (ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK == blk_type) {
    if (phy_ckpt_blk_used_cnt_ < phy_ckpt_blk_cnt_) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK, 1, /* update_shared_blk */false);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(blk_type), KPC(this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block_type is wrong", KR(ret), K(blk_type), KPC(this));
  }

  if (FAILEDx(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("blk_cnt_info is invalid", KR(ret), K(blk_type), KPC(this));
  }
  return ret;
}

int ObSSPhyBlockCountInfo::decrease_blk_used_cnt(const ObSSPhyBlockType blk_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_TYPE == blk_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(blk_type));
  } else if (ObSSPhyBlockType::SS_MICRO_DATA_BLK == blk_type) {
    if ((data_blk_.free_blk_cnt() < reorgan_blk_cnt_) || (data_blk_.hold_cnt_ == data_blk_.min_cnt_)) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_DATA_BLK, -1, /*update_shared_blk*/false);
    } else {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_DATA_BLK, -1, /*update_shared_blk*/true);
    }
    cache_stat_.phy_blk_stat().try_dec_reorgan_block_used_cnt();
  } else if (ObSSPhyBlockType::SS_MICRO_META_BLK == blk_type) {
    if (meta_blk_.hold_cnt_ == meta_blk_.min_cnt_) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_BLK, -1, /*update_shared_blk*/false);
    } else {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_BLK, -1, /*update_shared_blk*/true);
    }
  } else if (ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK == blk_type) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK, -1, /*update_shared_blk*/false);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block_type is wrong", KR(ret), K(blk_type), KPC(this));
  }

  if (FAILEDx(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("blk_cnt_info is invalid", KR(ret), K(blk_type), KPC(this));
  }
  return ret;
}

int64_t ObSSPhyBlockCountInfo::micro_data_blk_max_cnt() const
{
  return MIN(data_blk_.hold_cnt_ + shared_blk_free_cnt(), data_blk_.max_cnt_) - reorgan_blk_cnt_;
}

bool ObSSPhyBlockCountInfo::has_free_blk(const ObSSPhyBlockType type) const
{
  bool b_ret = false;
  if (ObSSPhyBlockType::SS_REORGAN_BLK == type) {
    b_ret = (data_blk_.free_blk_cnt() > 0) || (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_MICRO_DATA_BLK == type) {
    b_ret = (data_blk_.free_blk_cnt() > reorgan_blk_cnt_) || (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_MICRO_META_BLK == type) {
    b_ret = (meta_blk_.free_blk_cnt() > 0) || (!meta_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK == type) {
    b_ret = (phy_ckpt_blk_used_cnt_ < phy_ckpt_blk_cnt_);
  }
  return b_ret;
}

/*-----------------------------------------ObSSPhysicalBlock-----------------------------------------*/
ObSSPhysicalBlock::ObSSPhysicalBlock()
  : lock_(common::ObLatchIds::OB_SS_PHYSICAL_BLOCK_LOCK), ref_cnt_(1), alloc_time_s_(0), valid_len_(0), is_free_(1), is_sealed_(0), 
    blk_type_(static_cast<uint64_t>(ObSSPhyBlockType::SS_INVALID_TYPE)), reserved_(0), 
    reuse_version_(INITIAL_REUSE_VERSION)
{}

ObSSPhysicalBlock::ObSSPhysicalBlock(const ObSSPhysicalBlock &other) 
    : lock_(common::ObLatchIds::OB_SS_PHYSICAL_BLOCK_LOCK), ref_cnt_(other.ref_cnt_ + 1), first_val_(other.first_val_), reuse_version_(other.reuse_version_)
{}

void ObSSPhysicalBlock::reset()
{
  SpinWLockGuard guard(lock_);
  inner_reset();
}

void ObSSPhysicalBlock::inner_reuse()
{
  ref_cnt_ = 1;
  alloc_time_s_ = 0;
  reuse_version_ += 1;
  valid_len_ = 0;
  is_free_ = 1;
  is_sealed_ = 0;
  blk_type_ = static_cast<uint64_t>(ObSSPhyBlockType::SS_INVALID_TYPE);
  reserved_ = 0;
}

void ObSSPhysicalBlock::inner_reset()
{
  inner_reuse();
  reuse_version_ = INITIAL_REUSE_VERSION;
}

bool ObSSPhysicalBlock::is_empty() const
{
  SpinRLockGuard guard(lock_);
  return (valid_len_ == 0);
}

bool ObSSPhysicalBlock::is_free() const
{
  SpinRLockGuard guard(lock_);
  return is_free_;
}

bool ObSSPhysicalBlock::is_sealed() const
{
  SpinRLockGuard guard(lock_);
  return is_sealed_;
}

bool ObSSPhysicalBlock::is_valid_micro_data_block() const
{
  SpinRLockGuard guard(lock_);
  return (!is_free_) && (valid_len_ > 0) &&
         (ObSSPhyBlockType::SS_MICRO_DATA_BLK == static_cast<ObSSPhyBlockType>(blk_type_));
}

bool ObSSPhysicalBlock::can_reuse() const
{
  SpinRLockGuard guard(lock_);
  return (!is_free_) && (valid_len_ == 0) && (is_sealed_ || 
         (ObTimeUtility::current_time_s() - alloc_time_s_ >= SS_FREE_BLK_MIN_REUSE_TIME_S)); 
}

bool ObSSPhysicalBlock::can_reorganize() const
{
  bool b_ret = false;
  const int32_t block_size = SSPhyBlockMgr.get_block_size();
  const double blk_reorgan_pct = SSPhyBlockMgr.get_blk_reorgan_pct();
  SpinRLockGuard guard(lock_);
  if (block_size > 0) {
    const double blk_usage = static_cast<double>(valid_len_) / block_size;
    b_ret = (!is_free_) && (valid_len_ > 0) && is_sealed_ && (blk_usage < blk_reorgan_pct) &&
            (ObSSPhyBlockType::SS_MICRO_DATA_BLK == static_cast<ObSSPhyBlockType>(blk_type_));
  }
  return b_ret;
}

uint64_t ObSSPhysicalBlock::get_reuse_version() const
{
  SpinRLockGuard guard(lock_);
  return reuse_version_;
}

uint64_t ObSSPhysicalBlock::get_valid_len() const
{
  SpinRLockGuard guard(lock_);
  return valid_len_;
}

ObSSPhyBlockType ObSSPhysicalBlock::get_block_type() const
{
  SpinRLockGuard guard(lock_);
  return static_cast<ObSSPhyBlockType>(blk_type_);
}

uint32_t ObSSPhysicalBlock::get_ref_count() const
{
  SpinRLockGuard guard(lock_);
  return ref_cnt_;
}

void ObSSPhysicalBlock::inc_ref_count()
{
  SpinWLockGuard guard(lock_);
  ++ref_cnt_;
}

void ObSSPhysicalBlock::dec_ref_count()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    LOG_ERROR("phy_block's ref_cnt is invalid", K(*this));
  } else {
    --ref_cnt_;
  }
}

void ObSSPhysicalBlock::set_sealed(const uint64_t valid_len)
{
  SpinWLockGuard guard(lock_);
  is_sealed_ = 1;
  valid_len_ = valid_len;
}

void ObSSPhysicalBlock::set_valid_len(const uint64_t valid_len)
{
  SpinWLockGuard guard(lock_);
  valid_len_ = valid_len;
}

void ObSSPhysicalBlock::set_reuse_version(const uint64_t reuse_version)
{
  SpinWLockGuard guard(lock_);
  reuse_version_ = reuse_version;
}

void ObSSPhysicalBlock::set_block_type(const ObSSPhyBlockType &blk_type)
{
  SpinWLockGuard guard(lock_);
  blk_type_ = static_cast<uint64_t>(blk_type);
}

int ObSSPhysicalBlock::inc_valid_len(const int64_t blk_idx, const int64_t delta_len)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const int32_t block_size = SSPhyBlockMgr.get_block_size();
  if (OB_UNLIKELY((delta_len <= 0) || (block_size <= 0) || (valid_len_ + delta_len > block_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(block_size), K(blk_idx), K(delta_len), KPC(this));
  } else {
    valid_len_ += delta_len;
  }
  return ret;
}

int ObSSPhysicalBlock::dec_valid_len(const int64_t blk_idx, const int64_t delta_len)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  bool is_sparse = false;
  {
    SpinWLockGuard guard(lock_);
    const int32_t block_size = SSPhyBlockMgr.get_block_size();
    const double blk_reorgan_pct = SSPhyBlockMgr.get_blk_reorgan_pct();
    if (OB_UNLIKELY((delta_len >= 0) || (block_size <= 0) || (valid_len_ + delta_len < 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid argument", KR(ret), K(block_size), K(blk_idx), K(delta_len), KPC(this));
    } else {
      const double prev_usage = static_cast<double>(valid_len_) / block_size;
      const double cur_usage = static_cast<double>(valid_len_ + delta_len) / block_size;
      valid_len_ += delta_len;
      if (0 == valid_len_) {
        if (OB_UNLIKELY(!is_sealed_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected phy_blk state", KR(ret), K(blk_idx), K(delta_len), K(*this));
        } else {
          is_empty = true;
        }
      } else if ((prev_usage >= blk_reorgan_pct && cur_usage < blk_reorgan_pct) && is_sealed_ &&
                 (ObSSPhyBlockType::SS_MICRO_DATA_BLK == static_cast<ObSSPhyBlockType>(blk_type_))) {
        is_sparse = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_empty && OB_FAIL(SSPhyBlockMgr.add_reusable_block(blk_idx))) {
      LOG_WARN("fail to add phy_block into reusable_list", KR(ret), K(blk_idx));
    } else if (is_sparse && OB_FAIL(SSPhyBlockMgr.add_sparse_block(ObSSPhyBlockIdx(blk_idx)))) {
      LOG_WARN("fail to add sparse_block", KR(ret), K(blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlock::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  SpinWLockGuard guard(lock_);
  // Notice: phy_block's ref_cnt is inited as 1
  if (OB_LIKELY((0 == valid_len_) && (1 == ref_cnt_))) {
    inner_reuse();
    succ_free = true;
  } else if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("phy_blk's ref_cnt is invalid", KR(ret), KPC(this));
  }
  return ret;
}

int ObSSPhysicalBlock::mark_used(const ObSSPhyBlockType &blk_type, const bool is_sealed)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY((!is_free_) || (valid_len_ > 0) || is_sealed_ || 
      (ObSSPhyBlockType::SS_INVALID_TYPE == blk_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(*this), K(blk_type));
  } else {
    force_mark_used(blk_type, is_sealed);
  }
  return ret;
}

void ObSSPhysicalBlock::force_mark_used(const ObSSPhyBlockType &blk_type, const bool is_sealed)
{
  is_free_ = 0;
  ObSSPhyBlockType real_blk_type = get_ss_block_persist_type(blk_type);
  blk_type_ = static_cast<uint64_t>(real_blk_type);
  is_sealed_ = static_cast<uint64_t>(is_sealed);
  alloc_time_s_ = ObTimeUtility::current_time_s();
}

void ObSSPhysicalBlock::mark_sealed(const uint64_t valid_len)
{
  SpinWLockGuard guard(lock_);
  is_sealed_ = 1;
  valid_len_ = valid_len;
}

void ObSSPhysicalBlock::mark_reusable(const ObSSPhyBlockType &blk_type)
{
  SpinWLockGuard guard(lock_);
  is_free_ = 0;
  valid_len_ = 0;
  is_sealed_ = 1;
  blk_type_ = static_cast<uint64_t>(get_ss_block_persist_type(blk_type));
}

ObSSPhysicalBlock &ObSSPhysicalBlock::operator=(const ObSSPhysicalBlock &other)
{
  SpinRLockGuard r_guard(other.lock_);
  SpinWLockGuard w_guard(lock_);
  ref_cnt_ = other.ref_cnt_;
  alloc_time_s_ = other.alloc_time_s_;
  valid_len_ = other.valid_len_;
  is_free_ = other.is_free_;
  is_sealed_ = other.is_sealed_;
  blk_type_ = other.blk_type_;
  reuse_version_ = other.reuse_version_;
  return *this;
}

OB_SERIALIZE_MEMBER(ObSSPhysicalBlock, ref_cnt_, first_val_, reuse_version_);

/*-----------------------------------------ObSSPhyBlockIdxRange------------------------------------------*/
ObSSPhyBlockIdxRange::ObSSPhyBlockIdxRange() 
  : start_blk_idx_(-1), end_blk_idx_(-1)
{}

ObSSPhyBlockIdxRange::ObSSPhyBlockIdxRange(const int64_t start_blk_idx, const int64_t end_blk_idx)
  : start_blk_idx_(start_blk_idx), end_blk_idx_(end_blk_idx)
{}

bool ObSSPhyBlockIdxRange::is_valid() const
{
  // left-open and right-closed interval, e.g., (1, 100] represents 2~100 blocks.
  // 0 and 1 blocks are super blocks, which should not be accessed by other upper-level modules
  return (start_blk_idx_ < end_blk_idx_) && (start_blk_idx_ > 0);
}

void ObSSPhyBlockIdxRange::reset()
{
  start_blk_idx_ = -1;
  end_blk_idx_ = -1;
}

OB_SERIALIZE_MEMBER(ObSSPhyBlockIdxRange, start_blk_idx_, end_blk_idx_);

/*-----------------------------------------ObSSPhyBlockInfoCkptItem-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSPhyBlockInfoCkptItem, blk_idx_, reuse_version_);

/*-----------------------------------------ObSSPhyBlockCommonHeader-----------------------------------------*/
void ObSSPhyBlockCommonHeader::reset()
{
  header_size_ = get_serialize_size();
  version_ = SS_PHY_BLK_COMMON_HEADER_VERSION;
  magic_ = SS_PHY_BLK_COMMON_HEADER_MAGIC;
  payload_size_ = 0;
  payload_checksum_ = 0;
  blk_type_ = static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_TYPE);
  reserved_ = 0;
}

bool ObSSPhyBlockCommonHeader::is_valid() const
{
  return (header_size_ > 0 && version_ == SS_PHY_BLK_COMMON_HEADER_VERSION && 
          magic_ == SS_PHY_BLK_COMMON_HEADER_MAGIC && 
          blk_type_ != static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_TYPE));
}

void ObSSPhyBlockCommonHeader::calc_payload_checksum(const char *buf, const int32_t buf_size)
{
  if (OB_NOT_NULL(buf) && buf_size > 0) {
    payload_checksum_ = static_cast<int32_t>(ob_crc64(buf, buf_size));
  }
}

int ObSSPhyBlockCommonHeader::check_payload_checksum(const char *buf, const int32_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size), KP(buf));
  } else {
    const uint32_t cur_checksum = static_cast<uint32_t>(ob_crc64(buf, buf_size));
    if (cur_checksum != payload_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("phy_block header checksum error!", KR(ret), K(cur_checksum), K(buf_size), KP(buf),
        K(*this));
    }
  }
  return ret;
}

int ObSSPhyBlockCommonHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("common header is invalid", KR(ret), K(*this));
  } else {
    ObSSPhyBlockCommonHeader *header = reinterpret_cast<ObSSPhyBlockCommonHeader *>(buf + pos);
    header->header_size_ = header_size_;
    header->version_ = version_;
    header->magic_ = magic_;
    header->payload_size_ = payload_size_;
    header->payload_checksum_ = payload_checksum_;
    header->attr_ = attr_;
    pos += header->header_size_;
  }
  return ret;
}

int ObSSPhyBlockCommonHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObSSPhyBlockCommonHeader *header_ptr = reinterpret_cast<const ObSSPhyBlockCommonHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    magic_ = header_ptr->magic_;
    payload_size_ = header_ptr->payload_size_;
    payload_checksum_ = header_ptr->payload_checksum_;
    attr_ = header_ptr->attr_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized phy_blk common header is invalid", KR(ret), K(*this));
    } else {
      pos += header_size_;
    }
  }
  return ret;
}

int64_t ObSSPhyBlockCommonHeader::get_serialize_size() const
{
  return sizeof(ObSSPhyBlockCommonHeader);
}

/*-----------------------------------------ObSSMicroDataBlockHeader-----------------------------------------*/
void ObSSMicroDataBlockHeader::reset()
{
  header_size_ = get_serialize_size();
  version_ = SS_MICRO_DATA_BLK_HEADER_VERSION;
  magic_ = SS_MICRO_DATA_BLK_HEADER_MAGIC;
  payload_offset_ = -1;
  payload_size_ = 0;
  micro_count_ = 0;
  micro_index_offset_ = -1;
  micro_index_size_ = 0;
}

bool ObSSMicroDataBlockHeader::is_valid() const
{
  return (header_size_ > 0) && (payload_size_ > 0) && (magic_ == SS_MICRO_DATA_BLK_HEADER_MAGIC);
}

int ObSSMicroDataBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_data phy_blk header is invalid", KR(ret), K(*this));
  } else {
    ObSSMicroDataBlockHeader *header = reinterpret_cast<ObSSMicroDataBlockHeader *>(buf + pos);
    header->header_size_ = get_serialize_size();
    header->version_ = version_;
    header->magic_ = magic_;
    header->payload_offset_ = payload_offset_;
    header->payload_size_ = payload_size_;
    header->micro_count_ = micro_count_;
    header->micro_index_offset_ = micro_index_offset_;
    header->micro_index_size_ = micro_index_size_;
    pos += header->header_size_;
  }
  return ret;
}

int ObSSMicroDataBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObSSMicroDataBlockHeader *header_ptr = reinterpret_cast<const ObSSMicroDataBlockHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    magic_ = header_ptr->magic_;
    payload_offset_ = header_ptr->payload_offset_;
    payload_size_ = header_ptr->payload_size_;
    micro_count_ = header_ptr->micro_count_;
    micro_index_offset_ = header_ptr->micro_index_offset_;
    micro_index_size_ = header_ptr->micro_index_size_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized micro_data phy_blk header is invalid", KR(ret), K(*this));
    } else {
      pos += header_size_;
    }
  }
  return ret;
}

int64_t ObSSMicroDataBlockHeader::get_serialize_size() const
{
  return sizeof(ObSSMicroDataBlockHeader);
}

/*-----------------------------------------ObSSCkptBlockHeader-----------------------------------------*/
bool ObSSCkptBlockHeader::is_valid() const
{
  return (header_size_ > 0) && (SS_CKPT_BLOCK_HEADER_MAGIC == magic_) && (payload_size_ > 0);
}

void ObSSCkptBlockHeader::reset()
{
  header_size_ = get_serialize_size();
  version_ = SS_CKPT_BLK_HEADER_VERSION;
  magic_ = SS_CKPT_BLOCK_HEADER_MAGIC;
  item_count_ = 0;
  payload_size_ = 0;
  payload_checksum_ = 0;
  prev_phy_blk_id_ = -1;
}

int ObSSCkptBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ckpt phy_blk header is invalid", KR(ret), K(*this));
  } else {
    ObSSCkptBlockHeader *header = reinterpret_cast<ObSSCkptBlockHeader *>(buf + pos);
    header->header_size_ = get_serialize_size();
    header->version_ = version_;
    header->magic_ = magic_;
    header->item_count_ = item_count_;
    header->payload_size_ = payload_size_;
    header->payload_checksum_ = payload_checksum_;
    header->prev_phy_blk_id_ = prev_phy_blk_id_;
    pos += header->header_size_;
  }
  return ret;
}

int ObSSCkptBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObSSCkptBlockHeader *header_ptr = reinterpret_cast<const ObSSCkptBlockHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    magic_ = header_ptr->magic_;
    item_count_ = header_ptr->item_count_;
    payload_size_ = header_ptr->payload_size_;
    payload_checksum_ = header_ptr->payload_checksum_;
    prev_phy_blk_id_ = header_ptr->prev_phy_blk_id_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized ckpt phy_blk header is invalid", KR(ret), K(*this));
    } else {
      pos += header_size_;
    }
  }
  return ret;
}

int64_t ObSSCkptBlockHeader::get_serialize_size() const
{
  return sizeof(ObSSCkptBlockHeader);
}

} /* namespace storage */
} /* namespace oceanbase */
