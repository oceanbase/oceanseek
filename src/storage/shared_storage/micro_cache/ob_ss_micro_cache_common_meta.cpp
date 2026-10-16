/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

bool is_prewarm_access_type(const ObSSMicroCacheAccessType access_type)
{
  return (access_type >= ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE &&
          access_type <= ObSSMicroCacheAccessType::TRY_WRITE_LCACHE_PREWARM_TYPE);
}

/*-----------------------------------------ObSSMicroCacheAccessInfo-----------------------------------------*/
ObSSMicroCacheAccessInfo::ObSSMicroCacheAccessInfo(
    const ObSSMicroCacheAccessType access_type, 
    const int64_t delta_cnt, 
    const int64_t delta_size)
  : access_type_(access_type), delta_cnt_(delta_cnt), delta_size_(delta_size)
{}

void ObSSMicroCacheAccessInfo::reset()
{
  access_type_ = ObSSMicroCacheAccessType::INVALID_TYPE;
  delta_cnt_ = 0;
  delta_size_ = 0;
}

bool ObSSMicroCacheAccessInfo::is_valid() const
{
  return ((access_type_ != ObSSMicroCacheAccessType::INVALID_TYPE) && 
          (delta_cnt_ > 0) && (delta_size_ > 0));
}

/*-----------------------------------------ObSSMicroCacheTabletInfo-----------------------------------------*/
void ObSSMicroCacheTabletInfo::add_micro_size(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size)
{
  if (is_in_l1 && !is_in_ghost) {
    t1_size_ += delta_size;
  } else if (!is_in_l1 && !is_in_ghost) {
    t2_size_ += delta_size;
  }
}

ObSSMicroCacheTabletInfo& ObSSMicroCacheTabletInfo::operator=(const ObSSMicroCacheTabletInfo &other)
{
  if (this != &other) {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    t1_size_ = other.t1_size_;
    t2_size_ = other.t2_size_;
  }
  return *this;
}

/*-----------------------------------------ObSSMicroCacheLSInfo-----------------------------------------*/
ObSSMicroCacheLSInfo& ObSSMicroCacheLSInfo::operator=(const ObSSMicroCacheLSInfo &other)
{
  if (this != &other) {
    ls_id_ = other.ls_id_;
    tablet_cnt_ = other.tablet_cnt_;
    t1_size_ = other.t1_size_;
    t2_size_ = other.t2_size_;
    t1_hot_size_ = other.t1_hot_size_;
    t2_hot_size_ = other.t2_hot_size_;
  }
  return *this;
}

int64_t ObSSMicroCacheLSInfo::get_hot_size() const
{
  int64_t hot_size = t1_hot_size_ + t2_hot_size_;
  const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (min_cluster_version < CLUSTER_VERSION_4_5_1_0) {
    // During cluster upgrade, t1_hot_size_ and t2_hot_size_ deserialized from old nodes default to 0.
    // Use total size as a fallback to preserve hot data as much as possible.
    hot_size = get_valid_size();
  }
  return hot_size;
}

OB_SERIALIZE_MEMBER(ObSSMicroCacheLSInfo, ls_id_, tablet_cnt_, t1_size_, t2_size_, t1_hot_size_, t2_hot_size_);

/*-----------------------------------------ObSSMicroCacheAdminInfo-----------------------------------------*/
void ObSSMicroCacheAdminInfo::reset()
{
  micro_comp_type_ = ObCompressorType::INVALID_COMPRESSOR;
}

ObSSMicroCacheAdminInfo& ObSSMicroCacheAdminInfo::operator=(const ObSSMicroCacheAdminInfo &other)
{
  if (this != &other) {
    micro_comp_type_ = other.micro_comp_type_;
  }
  return *this;
}

OB_SERIALIZE_MEMBER(ObSSMicroCacheAdminInfo, micro_comp_type_);

/*-----------------------------------------ObSSBlkCkptInfo-----------------------------------------*/
void ObSSBlkCkptInfo::reset()
{
  blk_ckpt_used_blks_.reset();
  blk_ckpt_entry_ = SS_INVALID_PHY_BLK_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObSSBlkCkptInfo::reuse()
{
  blk_ckpt_used_blks_.reuse();
  blk_ckpt_entry_ = SS_INVALID_PHY_BLK_ID;
}

int ObSSBlkCkptInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_unpersisted_info(tenant_id))) {
    LOG_WARN("fail to init unpersisted info", KR(ret), K(tenant_id));
  } else {
    blk_ckpt_entry_ = SS_INVALID_PHY_BLK_ID;
  }
  return ret;
}

int ObSSBlkCkptInfo::init_unpersisted_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid_tenant_id(tenant_id_))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    blk_ckpt_used_blks_.set_attr(ObMemAttr(tenant_id, "SSBCkptList"));
  }
  return ret;
}

bool ObSSBlkCkptInfo::is_valid() const
{
  return (is_valid_tenant_id(tenant_id_));
}

bool ObSSBlkCkptInfo::exist_valid_ckpt() const
{
  return (blk_ckpt_entry_ != SS_INVALID_PHY_BLK_ID);
}

bool ObSSBlkCkptInfo::exist_in_used_blks(const int64_t blk_idx) const
{
  bool is_exist = false;
  for (int64_t i = 0; i < blk_ckpt_used_blks_.count(); ++i) {
    is_exist = (blk_idx == blk_ckpt_used_blks_.at(i));
    if (is_exist) {
      break;
    }
  }
  return is_exist;
}

int ObSSBlkCkptInfo::assign(const ObSSBlkCkptInfo &other, const bool need_reset)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (need_reset) {
      reset();
      if (OB_FAIL(init(other.tenant_id_))) {
        LOG_WARN("fail to init", KR(ret), K(other), K(*this));
      }
    } else {
      reuse();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(blk_ckpt_used_blks_.assign(other.blk_ckpt_used_blks_))) {
      LOG_WARN("fail to assign", KR(ret), K(other), K(*this));
    } else {
      tenant_id_ = other.tenant_id_;
      blk_ckpt_entry_ = other.blk_ckpt_entry_;
    }
  }
  return ret;
}

int ObSSBlkCkptInfo::update_used_blks(const ObIArray<int64_t> &used_blks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ckpt primary info", KR(ret), K(*this));
  } else if (OB_UNLIKELY(used_blks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(used_blks));
  } else if (OB_FAIL(blk_ckpt_used_blks_.assign(used_blks))) {
    LOG_WARN("fail to assign", KR(ret), K(*this), K(used_blks));
  }
  return ret;
}

bool ObSSBlkCkptInfo::operator==(const ObSSBlkCkptInfo &other) const
{
  bool b_ret = ((tenant_id_ == other.tenant_id_) && (blk_ckpt_entry_ == other.blk_ckpt_entry_) &&
                (blk_ckpt_used_blks_.count() == other.blk_ckpt_used_blks_.count()));
  if (b_ret) {
    for (int64_t i = 0; b_ret && (i < blk_ckpt_used_blks_.count()); ++i) {
      b_ret = (blk_ckpt_used_blks_.at(i) == other.blk_ckpt_used_blks_.at(i));
    }
  }
  return b_ret;
}

bool ObSSBlkCkptInfo::operator!=(const ObSSBlkCkptInfo &other) const
{
  return (!((*this) == other));
}

OB_SERIALIZE_MEMBER(ObSSBlkCkptInfo, blk_ckpt_entry_);

/*-----------------------------------------ObSSMicroCkptEntryItem-----------------------------------------*/
ObSSMicroCkptEntryItem::ObSSMicroCkptEntryItem(const bool exist_ckpt, const int64_t entry_blk_idx)
  : exist_ckpt_(exist_ckpt), entry_blk_idx_(entry_blk_idx)
{}

void ObSSMicroCkptEntryItem::reset()
{
  exist_ckpt_ = false;
  entry_blk_idx_ = SS_INVALID_PHY_BLK_ID;
}

bool ObSSMicroCkptEntryItem::is_valid() const
{
  bool b_ret = true;
  if (exist_ckpt_) {
    b_ret = (entry_blk_idx_ != SS_INVALID_PHY_BLK_ID);
  }
  return b_ret;
}

int ObSSMicroCkptEntryItem::assign(const ObSSMicroCkptEntryItem &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    exist_ckpt_ = other.exist_ckpt_;
    entry_blk_idx_ = other.entry_blk_idx_;
  }
  return ret;
}

void ObSSMicroCkptEntryItem::set_info(const bool exist_ckpt, const int64_t entry_blk_idx)
{
  exist_ckpt_ = exist_ckpt;
  entry_blk_idx_ = entry_blk_idx;
}

bool ObSSMicroCkptEntryItem::operator==(const ObSSMicroCkptEntryItem &other) const
{
  return ((exist_ckpt_ == other.exist_ckpt_) && (entry_blk_idx_ == other.entry_blk_idx_));
}

OB_SERIALIZE_MEMBER(ObSSMicroCkptEntryItem, exist_ckpt_, entry_blk_idx_);

/*-----------------------------------------ObSSMicroCkptInfo-----------------------------------------*/
void ObSSMicroCkptInfo::reset()
{
  for (int64_t i = 0; i < micro_ckpt_used_blks_.count(); ++i) {
    micro_ckpt_used_blks_.at(i).reset();
  }
  micro_ckpt_used_blks_.reset();
  micro_ckpt_entries_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

void ObSSMicroCkptInfo::reuse()
{
  for (int64_t i = 0; i < micro_ckpt_used_blks_.count(); ++i) {
    micro_ckpt_used_blks_.at(i).reuse();
  }
  for (int64_t i = 0; i < micro_ckpt_entries_.count(); ++i) {
    micro_ckpt_entries_.at(i).reset();
  }
}

int ObSSMicroCkptInfo::init(const uint64_t tenant_id, const int64_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid_tenant_id(tenant_id_) || micro_ckpt_entries_.count() > 0 ||
      micro_ckpt_used_blks_.count() > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ckpt_split_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < ckpt_split_cnt); ++i) {
      ObSEArray<int64_t, SS_DEF_ARRAY_CNT> rngs_blk_list;
      ObSSMicroCkptEntryItem entry_item;
      if (OB_FAIL(micro_ckpt_used_blks_.push_back(rngs_blk_list))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      } else if (OB_FAIL(micro_ckpt_entries_.push_back(entry_item))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      } else {
        micro_ckpt_used_blks_.at(micro_ckpt_used_blks_.count() - 1).set_attr(ObMemAttr(tenant_id, "SSMCkptSList"));
      }
    }

    if (OB_SUCC(ret)) {
      tenant_id_ = tenant_id;
      micro_ckpt_entries_.set_attr(ObMemAttr(tenant_id, "SSMCkptEntries"));
      micro_ckpt_used_blks_.set_attr(ObMemAttr(tenant_id, "SSMCkptList"));
    }
  }
  return ret;
}

int ObSSMicroCkptInfo::init_unpersisted_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid_tenant_id(tenant_id_) || micro_ckpt_used_blks_.count() > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(tenant_id), "used_blk_list_cnt", micro_ckpt_used_blks_.count());
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  }

  const int64_t ckpt_entry_cnt = micro_ckpt_entries_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < ckpt_entry_cnt); ++i) {
    ObSEArray<int64_t, SS_DEF_ARRAY_CNT> rngs_blk_list;
    if (OB_FAIL(micro_ckpt_used_blks_.push_back(rngs_blk_list))) {
      LOG_WARN("fail to push back", KR(ret), K(i));
    } else {
      micro_ckpt_used_blks_.at(micro_ckpt_used_blks_.count() - 1).set_attr(ObMemAttr(tenant_id, "SSMCkptSList"));
    }
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    micro_ckpt_entries_.set_attr(ObMemAttr(tenant_id, "SSMCkptEntries"));
    micro_ckpt_used_blks_.set_attr(ObMemAttr(tenant_id, "SSMCkptList"));
  }
  return ret;
}

bool ObSSMicroCkptInfo::is_valid() const
{
  return (is_valid_tenant_id(tenant_id_) && (micro_ckpt_entries_.count() > 0) && (micro_ckpt_used_blks_.count() > 0));
}

bool ObSSMicroCkptInfo::check_init(const int64_t exp_entry_cnt) const
{
  bool b_ret = is_valid_tenant_id(tenant_id_);
  if (b_ret) {
    b_ret = ((micro_ckpt_entries_.count() == exp_entry_cnt) && (micro_ckpt_used_blks_.count() == exp_entry_cnt));
  }
  return b_ret;
}

bool ObSSMicroCkptInfo::exist_valid_ckpt() const
{
  int ret = OB_SUCCESS;
  bool exist_ckpt = false;
  for (int64_t i = 0; (!exist_ckpt) && (i < micro_ckpt_entries_.count()); ++i) {
    const ObSSMicroCkptEntryItem &entry_item = micro_ckpt_entries_.at(i);
    if (OB_UNLIKELY(!entry_item.is_valid())) {
      LOG_ERROR("entry_item should be valid", KR(ret), K(i), K(entry_item), K(*this));
    }
    exist_ckpt = (entry_item.exist_ckpt_ && (entry_item.entry_blk_idx_ != SS_INVALID_PHY_BLK_ID));
  }
  return exist_ckpt;
}

bool ObSSMicroCkptInfo::exist_in_used_blks(const int64_t blk_idx) const
{
  bool is_exist = false;
  for (int64_t i = 0; i < micro_ckpt_used_blks_.count(); ++i) {
    for (int64_t j = 0; j < micro_ckpt_used_blks_.at(i).count(); ++j) {
      is_exist = (blk_idx == micro_ckpt_used_blks_.at(i).at(j));
      if (is_exist) {
        break;
      }
    }
    if (is_exist) {
      break;
    }
  }
  return is_exist;
}

int64_t ObSSMicroCkptInfo::get_total_used_blk_cnt() const
{
  int64_t total_cnt = 0;
  for (int64_t i = 0; i < micro_ckpt_used_blks_.count(); ++i) {
    total_cnt += micro_ckpt_used_blks_.at(i).count();
  }
  return total_cnt;
}

int ObSSMicroCkptInfo::assign(const ObSSMicroCkptInfo &other, const bool need_reset)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const int64_t exp_ckpt_split_cnt = other.micro_ckpt_used_blks_.count();
    if (need_reset || (exp_ckpt_split_cnt != micro_ckpt_used_blks_.count())) {
      reset();
      if (OB_FAIL(init(other.tenant_id_, exp_ckpt_split_cnt))) {
        LOG_WARN("fail to init", KR(ret), K(other), K(*this));
      }
    } else {
      reuse();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(micro_ckpt_entries_.assign(other.micro_ckpt_entries_))) {
      LOG_WARN("fail to assign", KR(ret), K(other));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < exp_ckpt_split_cnt); ++i) {
        if (OB_FAIL(micro_ckpt_used_blks_.at(i).assign(other.micro_ckpt_used_blks_.at(i)))) {
          LOG_WARN("fail to assign", KR(ret), K(i), K(other.micro_ckpt_used_blks_.at(i)));
        }
      }

      if (OB_SUCC(ret)) {
        tenant_id_ = other.tenant_id_;
      }
    }
  }
  return ret;
}

int ObSSMicroCkptInfo::assign_by_idx(
    const int64_t idx, 
    const ObSSMicroCkptInfo &other,
    const int64_t exp_entry_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_init(exp_entry_cnt))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ckpt primary info", KR(ret), K(exp_entry_cnt), K(*this));
  } else if (OB_UNLIKELY(exp_entry_cnt <= 0 || idx >= exp_entry_cnt || !other.check_init(exp_entry_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K(exp_entry_cnt), K(other));
  } else if (OB_LIKELY(other.micro_ckpt_used_blks_.at(idx).count() > 0)) {
    if (OB_FAIL(micro_ckpt_used_blks_.at(idx).assign(other.micro_ckpt_used_blks_.at(idx)))) {
      LOG_WARN("fail to assign used_blks", KR(ret), K(idx), "other_used_blks", other.micro_ckpt_used_blks_.at(idx),
        K(*this));
    } else if (OB_FAIL(micro_ckpt_entries_.at(idx).assign(other.micro_ckpt_entries_.at(idx)))) {
      LOG_WARN("fail to assign ckpt entry", KR(ret), K(idx), "other_ckpt_entry", other.micro_ckpt_entries_.at(idx),
        K(*this));
    }
  }
  return ret;
}

int ObSSMicroCkptInfo::update_entry_info_by_idx(const int64_t idx)
{
  int ret = OB_SUCCESS;
  const int64_t blk_list_cnt = micro_ckpt_used_blks_.count();
  const int64_t entry_cnt = micro_ckpt_entries_.count();
  if (OB_UNLIKELY(entry_cnt < 1 || blk_list_cnt < 1)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ckpt primary info", KR(ret), K(entry_cnt), K(*this));
  } else if (OB_UNLIKELY(idx >= entry_cnt || idx >= blk_list_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K(entry_cnt), K(blk_list_cnt));
  } else if (micro_ckpt_used_blks_.at(idx).empty()) {
    micro_ckpt_entries_.at(idx).reset();
  } else {
    const int64_t entry_blk_idx = micro_ckpt_used_blks_.at(idx).at(0);
    micro_ckpt_entries_.at(idx).set_info(true, entry_blk_idx);
  }
  return ret;
}

int ObSSMicroCkptInfo::update_used_blks_by_idx(const int64_t idx, const ObIArray<int64_t> &used_blks)
{
  int ret = OB_SUCCESS;
  const int64_t blk_list_cnt = micro_ckpt_used_blks_.count();
  const int64_t entry_cnt = micro_ckpt_entries_.count();
  if (OB_UNLIKELY(entry_cnt < 1 || blk_list_cnt < 1)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init ckpt primary info", KR(ret), K(entry_cnt), K(*this));
  } else if (OB_UNLIKELY(idx >= entry_cnt || idx >= blk_list_cnt || used_blks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K(entry_cnt), K(blk_list_cnt), K(used_blks));
  } else if (OB_FAIL(micro_ckpt_used_blks_.at(idx).assign(used_blks))) {
    LOG_WARN("fail to assign", KR(ret), K(idx), K(used_blks), K(*this));
  }
  return ret;
}

bool ObSSMicroCkptInfo::operator==(const ObSSMicroCkptInfo &other) const
{
  bool b_ret = ((tenant_id_ == other.tenant_id_) && (micro_ckpt_entries_.count() == other.micro_ckpt_entries_.count())
                && (micro_ckpt_used_blks_.count() == other.micro_ckpt_used_blks_.count()));
  if (b_ret) {
    for (int64_t i = 0; b_ret && (i < micro_ckpt_entries_.count()); ++i) {
      b_ret = (micro_ckpt_entries_.at(i) == other.micro_ckpt_entries_.at(i));
    }
    for (int64_t i = 0; b_ret && (i < micro_ckpt_used_blks_.count()); ++i) {
      b_ret = (micro_ckpt_used_blks_.at(i).count() == other.micro_ckpt_used_blks_.at(i).count());
      if (b_ret) {
        for (int64_t j = 0; b_ret && (j < micro_ckpt_used_blks_.at(i).count()); ++j) {
          b_ret = (micro_ckpt_used_blks_.at(i).at(j) == other.micro_ckpt_used_blks_.at(i).at(j));
        }
      }
    }
  }
  return b_ret;
}

bool ObSSMicroCkptInfo::operator!=(const ObSSMicroCkptInfo &other) const
{
  return (!((*this) == other));
}

OB_SERIALIZE_MEMBER(ObSSMicroCkptInfo, micro_ckpt_entries_);

/*-----------------------------------------ObSSMicroCacheSuperBlk-----------------------------------------*/
ObSSMicroCacheSuperBlk::ObSSMicroCacheSuperBlk(const uint64_t tenant_id, const int64_t cache_file_size, 
                                               const int64_t micro_ckpt_split_cnt)
  : magic_(SS_SUPER_BLK_MAGIC), tenant_id_(tenant_id), cache_file_size_(cache_file_size), 
    modify_time_us_(ObTimeUtility::current_time_us()), micro_ckpt_time_us_(0), admin_info_(), 
    blk_ckpt_info_(), micro_ckpt_info_(), ls_info_list_(), tablet_info_list_()
{
  init_non_basic_info(tenant_id_, micro_ckpt_split_cnt);
}

void ObSSMicroCacheSuperBlk::reset()
{
  magic_ = SS_SUPER_BLK_MAGIC;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_file_size_ = 0;
  modify_time_us_ = 0;
  micro_ckpt_time_us_ = 0;
  admin_info_.reset();
  blk_ckpt_info_.reset();
  micro_ckpt_info_.reset();
  ls_info_list_.reset();
  tablet_info_list_.reset();
}

void ObSSMicroCacheSuperBlk::reuse()
{
  modify_time_us_ = ObTimeUtility::current_time_us();
  micro_ckpt_time_us_ = 0;
  micro_ckpt_info_.reuse();
  blk_ckpt_info_.reuse();
  ls_info_list_.reuse();
  tablet_info_list_.reuse();
}

bool ObSSMicroCacheSuperBlk::is_valid() const
{
  return ((is_valid_tenant_id(tenant_id_)) && (cache_file_size_ > 0) && (modify_time_us_ > 0) &&
          (SS_SUPER_BLK_MAGIC == magic_) && is_ckpt_info_valid());
}

bool ObSSMicroCacheSuperBlk::is_ckpt_info_valid() const
{
  return ((blk_ckpt_info_.is_valid()) && (micro_ckpt_info_.is_valid()));
}

bool ObSSMicroCacheSuperBlk::exist_micro_checkpoint() const 
{ 
  return micro_ckpt_info_.exist_valid_ckpt();
}

bool ObSSMicroCacheSuperBlk::exist_blk_checkpoint() const 
{ 
  return blk_ckpt_info_.exist_valid_ckpt();
}

bool ObSSMicroCacheSuperBlk::exist_valid_checkpoint() const 
{ 
  return (exist_micro_checkpoint() && exist_blk_checkpoint());
}

int ObSSMicroCacheSuperBlk::add_ls_cache_info(const ObSSMicroCacheLSInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t ls_idx = -1;
  if (OB_UNLIKELY(!ls_cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_cache_info));
  } else if (OB_FAIL(get_ls_cache_info_idx(ls_cache_info.ls_id_, ls_idx))) {
    LOG_WARN("fail to check ls_info exist", KR(ret), K(ls_cache_info));
  } else if (OB_UNLIKELY(ls_idx != -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already exist the same ls_id", KR(ret), K(ls_cache_info), K(ls_idx), K_(ls_info_list));
  } else if (OB_FAIL(ls_info_list_.push_back(ls_cache_info))) {
    LOG_WARN("fail to push back", KR(ret), K(ls_cache_info), K_(ls_info_list));
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::get_ls_cache_info(
    const ObLSID &ls_id, 
    ObSSMicroCacheLSInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t ls_idx = -1;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_cache_info_idx(ls_id, ls_idx))) {
    LOG_WARN("fail to check ls_info exist", KR(ret), K(ls_id));
  } else if (-1 == ls_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not exist this ls_id", KR(ret), K(ls_id));
  } else {
    ls_cache_info = ls_info_list_.at(ls_idx);
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::get_ls_cache_info_idx(const ObLSID &ls_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    idx = -1;
    const int64_t ls_info_cnt = ls_info_list_.count();
    for (int64_t i = 0; (-1 == idx) && (i < ls_info_cnt); ++i) {
      if (ls_id == ls_info_list_.at(i).ls_id_) {
        idx = i;
      }
    }
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::get_tablet_cache_info(
    const ObTabletID &tablet_id, 
    ObSSMicroCacheTabletInfo &tablet_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(exist_tablet_cache_info(tablet_id, tablet_idx))) {
    LOG_WARN("fail to check tablet_info exist", KR(ret), K(tablet_id));
  } else if (-1 == tablet_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not exist this tablet_id", KR(ret), K(tablet_id));
  } else {
    tablet_cache_info = tablet_info_list_.at(tablet_idx);
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::exist_tablet_cache_info(const ObTabletID &tablet_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    idx = -1;
    const int64_t tablet_info_cnt = tablet_info_list_.count();
    for (int64_t i = 0; (-1 == idx) && (i < tablet_info_cnt); ++i) {
      if (tablet_id == tablet_info_list_.at(i).tablet_id_) {
        idx = i;
      }
    }
  }
  return ret;
}

bool ObSSMicroCacheSuperBlk::exist_in_ckpt_list(const int64_t blk_idx) const
{
  return (blk_ckpt_info_.exist_in_used_blks(blk_idx) || micro_ckpt_info_.exist_in_used_blks(blk_idx));
}

int ObSSMicroCacheSuperBlk::assign(const ObSSMicroCacheSuperBlk &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    bool need_reset = false;
    if ((tenant_id_ != other.tenant_id_) || (get_ckpt_split_cnt() != other.get_ckpt_split_cnt())) {
      reset();
      if (OB_FAIL(init_non_basic_info(other.tenant_id_, other.get_ckpt_split_cnt()))) {
        LOG_WARN("fail to init non_basic info", KR(ret), K(other), K(*this));
      } else {
        need_reset = true;
      }
    } else {
      reuse();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS != (ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_ASSIGN_SUPER_BLK_ERR)) {
      LOG_WARN("assign super_block errsim", KR(ret));
    } else if (OB_FAIL(blk_ckpt_info_.assign(other.blk_ckpt_info_, need_reset))) {
      LOG_WARN("fail to assign", KR(ret), K(need_reset), K(*this), K(other));
    } else if (OB_FAIL(micro_ckpt_info_.assign(other.micro_ckpt_info_, need_reset))) {
      LOG_WARN("fail to assign", KR(ret), K(need_reset), K(*this), K(other));
    } else if (OB_FAIL(ls_info_list_.assign(other.ls_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else if (OB_FAIL(tablet_info_list_.assign(other.tablet_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else {
      assign_basic_info(other);
      micro_ckpt_time_us_ = other.micro_ckpt_time_us_;
      micro_ckpt_info_ = other.micro_ckpt_info_;
      blk_ckpt_info_ = other.blk_ckpt_info_;
    }

    if (OB_FAIL(ret)) {
      reset();
      FLOG_WARN("fail to assign super_block, will reset", KR(ret), K(need_reset), K(other));
    }
  }
  return ret;
}

void ObSSMicroCacheSuperBlk::assign_basic_info(const ObSSMicroCacheSuperBlk &other)
{
  if (this != &other) {
    magic_ = other.magic_;
    tenant_id_ = other.tenant_id_;
    cache_file_size_ = other.cache_file_size_;
    modify_time_us_ = other.modify_time_us_;
    admin_info_ = other.admin_info_;
  }
}

int ObSSMicroCacheSuperBlk::assign_for_blk_ckpt(const ObSSMicroCacheSuperBlk &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(blk_ckpt_info_.assign(other.blk_ckpt_info_))) {
      LOG_WARN("fail to assign", KR(ret), K(other), K(*this));
    }
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::assign_for_micro_ckpt(const int64_t idx, const ObSSMicroCacheSuperBlk &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(micro_ckpt_info_.assign_by_idx(idx, other.micro_ckpt_info_, get_ckpt_split_cnt()))) {
      LOG_WARN("fail to assign by idx", KR(ret), K(idx), K(other), K(*this));
    } else if (OB_FAIL(tablet_info_list_.assign(other.tablet_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(idx), K(other), K(*this));
    } else if (OB_FAIL(ls_info_list_.assign(other.ls_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(idx), K(other), K(*this));
    } else {
      micro_ckpt_time_us_ = other.micro_ckpt_time_us_;
    }
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::set_micro_ckpt_blk_list(const int64_t idx, const ObIArray<int64_t> &blk_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_ckpt_info_.update_used_blks_by_idx(idx, blk_list))) {
    LOG_WARN("fail to update used_blk list by idx", KR(ret), K(idx), K(blk_list));
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::set_blk_ckpt_blk_list(const ObIArray<int64_t> &blk_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blk_ckpt_info_.update_used_blks(blk_list))) {
    LOG_WARN("fail to update used_blk list", KR(ret), K(blk_list));
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::init_non_basic_info(const uint64_t tenant_id, const int64_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blk_ckpt_info_.init(tenant_id))) {
    LOG_WARN("fail to init blk_ckpt_info", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else if (OB_FAIL(micro_ckpt_info_.init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to init micro_ckpt_info", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    inner_set_list_attr(tenant_id);
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::reinit_meta_ckpt_info(const uint64_t tenant_id, const int64_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  micro_ckpt_info_.reset();
  if (OB_FAIL(micro_ckpt_info_.init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to reinit meta_ckpt_info", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  }
  return ret;
}

int ObSSMicroCacheSuperBlk::init_unpersisted_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blk_ckpt_info_.init_unpersisted_info(tenant_id_))) {
    LOG_WARN("fail to init unpersisted info", KR(ret), K_(tenant_id), K(*this));
  } else if (OB_FAIL(micro_ckpt_info_.init_unpersisted_info(tenant_id_))) {
    LOG_WARN("fail to init unpersisted info", KR(ret), K_(tenant_id), K(*this));
  } else {
    inner_set_list_attr(tenant_id_);
  }
  return ret;
}

void ObSSMicroCacheSuperBlk::inner_set_list_attr(const uint64_t tenant_id)
{
  if (is_valid_tenant_id(tenant_id)) {
    ls_info_list_.set_attr(ObMemAttr(tenant_id, "SSLsInfoList"));
    tablet_info_list_.set_attr(ObMemAttr(tenant_id, "SSTabInfoList"));
  }
}

OB_SERIALIZE_MEMBER(ObSSMicroCacheSuperBlk, magic_, tenant_id_, cache_file_size_, modify_time_us_, micro_ckpt_time_us_,
                    admin_info_, blk_ckpt_info_, micro_ckpt_info_);

/*-----------------------------------------ObSSMicroBlockId-----------------------------------------*/
void ObSSMicroBlockId::reset()
{
  macro_id_.reset();
  offset_ = 0;
  size_ = 0;
}

ObSSMicroBlockId& ObSSMicroBlockId::operator=(const ObSSMicroBlockId &other)
{
  if (this != &other) {
    macro_id_ = other.macro_id_;
    offset_ = other.offset_;
    size_ = other.size_;
  }
  return *this;
}

OB_SERIALIZE_MEMBER(ObSSMicroBlockId, macro_id_, offset_, size_);

/*-----------------------------------------ObSSMicroBlockCacheKey-----------------------------------------*/
void ObSSMicroBlockCacheKey::reset()
{
  if (is_logical_key()) {
    logic_micro_id_.reset();
  } else {
    micro_id_.reset();
  }
  micro_crc_ = 0;
  mode_ = ObSSMicroBlockCacheKeyMode::MAX_MODE;
}

bool ObSSMicroBlockCacheKey::is_valid() const
{
  bool b_ret = (mode_ != ObSSMicroBlockCacheKeyMode::MAX_MODE);
  if (b_ret) {
    if (is_logical_key()) {
      b_ret = logic_micro_id_.is_valid();
    } else {
      b_ret = micro_id_.is_valid();
    }
  }
  return b_ret;
}

uint64_t ObSSMicroBlockId::hash() const
{
  uint64_t hash_val = macro_id_.hash();
  hash_val = murmurhash(&offset_, sizeof(offset_), hash_val);
  hash_val = murmurhash(&size_, sizeof(size_), hash_val);
  return hash_val;
}

uint64_t ObSSMicroBlockCacheKey::hash() const
{
  uint64_t hash_val = 0;
  if (is_logical_key()) {
    hash_val = logic_micro_id_.hash();
    hash_val = murmurhash(&mode_, sizeof(mode_), hash_val);
    hash_val = murmurhash(&micro_crc_, sizeof(micro_crc_), hash_val);
  } else {
    hash_val = micro_id_.hash();
    hash_val = murmurhash(&mode_, sizeof(mode_), hash_val);
  }
  return hash_val;
}

ObSSMicroBlockCacheKey& ObSSMicroBlockCacheKey::operator=(const ObSSMicroBlockCacheKey &other)
{
  if (this != &other) {
    mode_ = other.mode_;
    if (is_logical_key()) {
      logic_micro_id_ = other.logic_micro_id_;
      micro_crc_ = other.micro_crc_;
    } else {
      micro_id_ = other.micro_id_;
    }
  }
  return *this;
}

bool ObSSMicroBlockCacheKey::operator==(const ObSSMicroBlockCacheKey &other) const
{
  bool b_ret = (mode_ == other.mode_);
  if (!b_ret) {
  } else if (is_logical_key()) {
    b_ret = ((logic_micro_id_ == other.logic_micro_id_) && (micro_crc_ == other.micro_crc_));
  } else {
    b_ret = (micro_id_ == other.micro_id_);
  }
  return b_ret;
}

int ObSSMicroBlockCacheKey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize_", KR(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(ObSSMicroBlockCacheKey, serial_size);
      ret = serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObSSMicroBlockCacheKey::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode(buf, buf_len, pos, mode_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (is_logical_key() && OB_FAIL(logic_micro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos), K_(logic_micro_id));
  } else if (!is_logical_key() && OB_FAIL(micro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos), K_(micro_id));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, micro_crc_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos), K_(micro_crc));
  }
  return ret;
}

int ObSSMicroBlockCacheKey::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    CHECK_VERSION_LENGTH(ObSSMicroBlockCacheKey, version, len);
  }
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {
      LOG_WARN("fail to deserialize_", KR(ret), K(len), K(pos));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObSSMicroBlockCacheKey::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, mode_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (is_logical_key() && OB_FAIL(logic_micro_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (!is_logical_key() && OB_FAIL(micro_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, micro_crc_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObSSMicroBlockCacheKey::get_serialize_size(void) const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

int64_t ObSSMicroBlockCacheKey::get_serialize_size_(void) const
{
  int64_t len = serialization::encoded_length(mode_);
  if (is_logical_key()) {
    len += logic_micro_id_.get_serialize_size();
  } else {
    len += micro_id_.get_serialize_size();
  }
  len += serialization::encoded_length(micro_crc_);
  return len;
}

bool ObSSMicroBlockCacheKey::is_shared_macro_type() const
{
  bool b_ret = false;
  // judge according to logical and physical mode
  // 1. logical mode represents SHARED_MAJOR_DATA_MACRO
  // 2. physical mode with shared data/meta block storage_object_type.
  //    e.g., SHARED_MAJOR_META_MACRO, SHARED_MINOR_DATA_MACRO...
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    b_ret = true;
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == mode_) {
    b_ret = micro_id_.macro_id_.is_shared_data_block_or_meta_block();
  }
  return b_ret;
}

ObTabletID ObSSMicroBlockCacheKey::get_macro_tablet_id() const
{
  ObTabletID tablet_id;
  // get tablet_id according to logical and physical mode
  // 1. logical mode: ObLogicalMicroBlockId.logic_macro_id_.tablet_id_
  // 2. physical mode: ObMicroBlockId.macro_id_.second_id()
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    tablet_id = logic_micro_id_.logic_macro_id_.tablet_id_;
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == mode_) {
    tablet_id = micro_id_.macro_id_.second_id();
  }
  return tablet_id;
}

int64_t ObSSMicroBlockCacheKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "{");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(mode));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(logic_micro_id));
  } else {
    ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(micro_id));
  }
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(micro_crc));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

/*-----------------------------------------ObSSMicroBlockIndex-----------------------------------------*/
ObSSMicroBlockIndex& ObSSMicroBlockIndex::operator=(const ObSSMicroBlockIndex &other)
{
  if (this != &other) {
    micro_key_ = other.micro_key_;
    size_ = other.size_;
  }
  return *this;
}

bool ObSSMicroBlockIndex::operator==(const ObSSMicroBlockIndex &other) const
{
  return (micro_key_ == other.micro_key_) && (size_ == other.size_);
}

uint64_t ObSSMicroBlockIndex::hash() const
{
  uint64_t hash_val = micro_key_.hash();
  hash_val = common::murmurhash(&size_, sizeof(size_), hash_val);
  return hash_val;
}

OB_SERIALIZE_MEMBER(ObSSMicroBlockIndex, micro_key_, size_);

/*-----------------------------------------ObSSMemBlock-----------------------------------------*/
ObSSMemBlock::ObSSMemBlock(ObSSMemBlockPool &mem_blk_pool)
  : is_fg_(true), ref_cnt_(1), micro_cnt_(0), incomplete_cnt_(0), total_data_size_(0), total_index_size_(0), reserved_size_(0),
    valid_val_(0), valid_cnt_(0), mem_blk_size_(0), mem_blk_buf_(nullptr), micro_loc_map_(), mem_blk_pool_(mem_blk_pool)
{}

void ObSSMemBlock::reset()
{
  is_fg_ = true;
  ref_cnt_ = 1;
  micro_cnt_ = 0;
  incomplete_cnt_ = 0;
  total_data_size_ = 0;
  total_index_size_ = 0;
  reserved_size_ = 0;
  valid_val_ = 0;
  valid_cnt_ = 0;
  mem_blk_size_ = 0;
  mem_blk_buf_ = nullptr;
  micro_loc_map_.destroy();
}

void ObSSMemBlock::destroy()
{
  dec_ref_count();
}

bool ObSSMemBlock::is_valid() const
{
  return (reserved_size_ > 0) && (nullptr != mem_blk_buf_) && (mem_blk_size_ > 0);
}

int ObSSMemBlock::init(
    const uint64_t tenant_id,
    const uint32_t mem_blk_size,
    char *mem_blk_buf)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_NUM = 128;
  if (OB_ISNULL(mem_blk_buf) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || mem_blk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(mem_blk_buf), K(mem_blk_size));
  } else if (OB_FAIL(micro_loc_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSMemBlkMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id));
  } else {
    mem_blk_buf_ = mem_blk_buf;
    mem_blk_size_ = mem_blk_size;
    reserved_size_ = get_reserved_size();
  }
  return ret;
}

int ObSSMemBlock::calc_write_location(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t size, 
    int32_t &data_offset, 
    int32_t &idx_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(micro_key), K(size), K(*this));
  } else {
    ObSSMicroBlockIndex micro_index(micro_key, size);
    const int32_t idx_size = micro_index.get_serialize_size();
    const int32_t delta_size = size + idx_size;
    if (OB_UNLIKELY(is_large_micro(delta_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("this micro_block is too large", KR(ret), K(delta_size), K(micro_key), K(size), K(*this));
    } else if (has_enough_space(delta_size)) {
      data_offset = reserved_size_ + total_data_size_;
      idx_offset = mem_blk_size_ - total_index_size_ - idx_size;
      total_data_size_ += size;
      total_index_size_ += idx_size;
      ++micro_cnt_;
      ATOMIC_INC(&incomplete_cnt_);
    } else {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_TRACE("current mem_block has not enough space", KR(ret), K(micro_key), K(size), K(*this));
    }
  }
  return ret;
}

int ObSSMemBlock::write_micro_data(
    const ObSSMicroBlockCacheKey &micro_key,
    const char *micro_data, 
    const int32_t size,
    const int32_t data_offset, 
    const int32_t idx_offset,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_data) || OB_UNLIKELY(size <= 0 || data_offset < 0 || idx_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_data), K(data_offset), K(idx_offset), K(size), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(*this));
  } else {
    crc = static_cast<uint32_t>(ob_crc64(micro_data, size));
    MEMCPY(mem_blk_buf_ + data_offset, micro_data, size);

    ObSSMicroBlockIndex micro_index(micro_key, size);
    ObSSMemMicroInfo mem_micro_info(data_offset, size);
    const int64_t index_size = micro_index.get_serialize_size();
    int64_t pos = idx_offset;
    if (OB_FAIL(micro_loc_map_.set_refactored(micro_key, mem_micro_info, 1/*overwrite*/))) {
      LOG_WARN("fail to insert mem_micro_info", KR(ret), K(micro_key), K(mem_micro_info));
    } else if (OB_FAIL(micro_index.serialize(mem_blk_buf_, mem_blk_size_, pos))) {
      LOG_WARN("fail to serialize micro_index", KR(ret), K(idx_offset), K(pos), K(micro_index), KPC(this));
    } else if (OB_UNLIKELY(pos - idx_offset != index_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("index_size dismatch", KR(ret), K(idx_offset), K(pos), K(index_size), K(micro_index));
    }
  }
  return ret;
}

int ObSSMemBlock::get_micro_data(
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t size,
    char *&buf) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(*this));
  } else {
    ObSSMemMicroInfo mem_micro_info;
    if (OB_FAIL(micro_loc_map_.get_refactored(micro_key, mem_micro_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    } else if (OB_UNLIKELY(!mem_micro_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_micro_info is invalid", KR(ret), K(micro_key), K(mem_micro_info));
    } else if (OB_UNLIKELY(size != mem_micro_info.size_)) {
      // There only exists reading a whole macro_block, or just read a micro_block, won't read a
      // batch of micro_blocks. 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("request size must be equal to micro_cache size", KR(ret), K(size), K(mem_micro_info));
    } else {
      buf = mem_blk_buf_ + mem_micro_info.offset_;
    }
  }
  return ret;
}

int ObSSMemBlock::memmove_micro_index()
{
  int ret = OB_SUCCESS;
  const int64_t data_end_offset = reserved_size_ + total_data_size_;
  const int64_t index_start_offset = mem_blk_size_ - total_index_size_;
  if (OB_UNLIKELY(OB_ISNULL(mem_blk_buf_) || (data_end_offset > mem_blk_size_) || (index_start_offset < 0) ||
                  (data_end_offset > index_start_offset))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mem_blk is abnormal", KR(ret), KPC(this));
  } else if (OB_LIKELY(data_end_offset < index_start_offset)) {
    MEMMOVE(mem_blk_buf_ + data_end_offset, mem_blk_buf_ + index_start_offset, total_index_size_);
  }
  return ret;
}

int ObSSMemBlock::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  const int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (ref_cnt == 0) {
    if (OB_FAIL(mem_blk_pool_.free(this))) {
      LOG_WARN("fail to free mem_block", KR(ret), KP(this));
    } else {
      succ_free = true;
    }
  } else if (OB_UNLIKELY(ref_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mem_blk's ref_cnt is invalid", KR(ret), KPC(this));
  }
  return ret;
}

void ObSSMemBlock::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMemBlock::dec_ref_count()
{
  bool succ_free = false;
  try_free(succ_free);
}

void ObSSMemBlock::mark_micro_complete()
{
  ATOMIC_DEC(&incomplete_cnt_);
}

void ObSSMemBlock::update_valid_micro_info(const uint32_t delta_size)
{
  ATOMIC_AAF(&valid_val_, delta_size);
  ATOMIC_INC(&valid_cnt_);
}

int64_t ObSSMemBlock::get_payload_size() const
{
  return total_data_size_ + total_index_size_;
}

bool ObSSMemBlock::is_fg_mem_blk() const
{
  return ATOMIC_LOAD(&is_fg_);
}

bool ObSSMemBlock::can_flush() const
{
  return 0 == ATOMIC_LOAD(&incomplete_cnt_);
}

bool ObSSMemBlock::exist_micro(const ObSSMicroBlockCacheKey &micro_key) const
{
  int ret = OB_SUCCESS;
  ObSSMemMicroInfo mem_info;
  bool is_exist = false;
  if (OB_FAIL(micro_loc_map_.get_refactored(micro_key, mem_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    } else {
      LOG_TRACE("not exist in mem_block", K(micro_key));
    }
  } else {
    is_exist = true;
  }
  return is_exist;
}

void ObSSMemBlock::set_is_fg(const bool is_fg)
{
  ATOMIC_STORE(&is_fg_, is_fg);
}

uint32_t ObSSMemBlock::get_reserved_size()
{
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  return common_header.get_serialize_size() + data_blk_header.get_serialize_size();
}

bool ObSSMemBlock::is_large_micro(const int32_t delta_size) const
{
  return reserved_size_ + delta_size > mem_blk_size_;
}

bool ObSSMemBlock::has_enough_space(const int32_t delta_size) const
{
  return (reserved_size_ + total_data_size_ + total_index_size_ + delta_size <= mem_blk_size_);
}

/*-----------------------------------------ObSSMemBlockPool-----------------------------------------*/
ObSSMemBlockPool::ObSSMemBlockPool(ObSSMicroCacheStat &cache_stat)
  : is_inited_(false), mem_blk_size_(0), tenant_id_(OB_INVALID_TENANT_ID), max_fg_count_(0), max_bg_count_(0),
    used_fg_count_(0), used_bg_count_(0), cond_(), cache_stat_(cache_stat)
{}

void ObSSMemBlockPool::destroy()
{
  cond_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  mem_blk_size_ = 0;
  max_fg_count_ = 0;
  max_bg_count_ = 0;
  used_fg_count_ = 0;
  used_bg_count_ = 0;
  is_inited_ = false;
}

int ObSSMemBlockPool::init(
    const uint64_t tenant_id, 
    const uint32_t block_size, 
    const int64_t max_fg_count, 
    const int64_t max_bg_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(block_size <= 0 || max_bg_count <= 0 || max_fg_count <= 0) || 
             OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size), K(max_fg_count), K(max_bg_count));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::TIERED_BLOCK_WRITE_REMOTE))) {
    LOG_WARN("fail to init thread cond", K(ret));
  } else {
    tenant_id_ = tenant_id;
    mem_blk_size_ = block_size;
    max_fg_count_ = max_fg_count;
    max_bg_count_ = max_bg_count;
    used_fg_count_ = 0;
    used_bg_count_ = 0;
    cache_stat_.mem_blk_stat().set_mem_blk_max_cnt(max_fg_count_, max_bg_count_);
    is_inited_ = true;
  }
  return ret;
}

int ObSSMemBlockPool::alloc(ObSSMemBlock *&mem_block, const bool is_fg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_free_mem_blk_enough(is_fg))) {
    LOG_WARN("free mem_blk count is not enough", KR(ret), K(is_fg));
  } else if (OB_FAIL(create_dynamic_mem_block(mem_block))) {
    LOG_WARN("fail to create dynamic mem_block", KR(ret));
    handle_create_failure(is_fg);
  } else if (OB_ISNULL(mem_block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("alloc mem_block should not be null", KR(ret), K(is_fg));
  } else {
    mem_block->set_is_fg(is_fg);
    LOG_TRACE("ss_micro_cache: succ to alloc mem_blk", K(is_fg), K_(used_fg_count), K_(used_bg_count), 
      KP(mem_block), KPC(mem_block));
  }
  return ret;
}

int64_t ObSSMemBlockPool::get_free_mem_blk_cnt(const bool is_fg) const
{
  int64_t free_cnt = 0;
  if (is_fg) {
    free_cnt = MAX(0, max_fg_count_ - ATOMIC_LOAD(&used_fg_count_));
  } else {
    free_cnt = MAX(0, max_bg_count_ - ATOMIC_LOAD(&used_bg_count_));
  }
  return free_cnt;
}

int ObSSMemBlockPool::check_free_mem_blk_enough(const bool is_fg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (is_fg) {
    if (ATOMIC_AAF(&used_fg_count_, 1) > max_fg_count_) {
      ATOMIC_AAF(&used_fg_count_, -1);
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("free fg_mem_blk is not enough", KR(ret), K_(max_fg_count), K_(used_fg_count));
    } else {
      cache_stat_.mem_blk_stat().update_mem_blk_fg_used_cnt(1);
    }
  } else {
    ObThreadCondGuard guard(cond_);
    if (used_bg_count_ >= max_bg_count_) {
      if (OB_SUCCESS != (tmp_ret = cond_.wait(DEF_WAIT_TIMEOUT_MS))) {
        if (OB_TIMEOUT == tmp_ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("free bg_mem_blk is not enough", KR(ret), KR(tmp_ret), K(DEF_WAIT_TIMEOUT_MS), K_(used_bg_count), 
            K_(max_bg_count));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wait thread condition failed", KR(ret), KR(tmp_ret), K(DEF_WAIT_TIMEOUT_MS), K_(used_bg_count), 
            K_(max_bg_count));
        }
      } else {
        ++used_bg_count_;
        cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(1);
      }
    } else {
      ++used_bg_count_;
      cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(1);
    }
  }
  return ret;
}

int ObSSMemBlockPool::create_dynamic_mem_block(ObSSMemBlock *&mem_block)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "SSMemBlk");
  char *mem_blk_buf = nullptr;
  void *mem_blk_ptr = nullptr;
  const int64_t ptr_size = sizeof(ObSSMemBlock);
  if (OB_ISNULL(mem_blk_buf = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, mem_blk_size_, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K_(mem_blk_size), K(attr));
  } else if (OB_ISNULL(mem_blk_ptr = ob_malloc(ptr_size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(ptr_size), K(attr));
  } else if (FALSE_IT(mem_block = new (mem_blk_ptr) ObSSMemBlock(*this))) {
  } else if (OB_FAIL(mem_block->init(tenant_id_, mem_blk_size_, mem_blk_buf))) {
    LOG_WARN("fail to init mem_block", KR(ret), K_(tenant_id), KP(mem_blk_buf), K_(mem_blk_size));
  } else if (OB_UNLIKELY(!mem_block->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block is invalid", KR(ret), KPC(mem_block));
  } else {
    LOG_TRACE("ss_micro_cache: succ to create dynamic mem_blk", KP(mem_block));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != mem_blk_buf) {
      ob_free_align(mem_blk_buf);
      mem_blk_buf = nullptr;
    }
    if (nullptr != mem_blk_ptr) {
      ob_free(mem_blk_ptr);
      mem_blk_ptr = nullptr;
    }
    mem_block = nullptr;
  } else {
    const int64_t delta_size = mem_blk_size_ + sizeof(ObSSMemBlock);
    cache_stat_.mem_stat().update_mem_blk_usage(delta_size);
  }
  return ret;
}

void ObSSMemBlockPool::handle_create_failure(bool is_fg)
{
  if (is_fg) {
    ATOMIC_AAF(&used_fg_count_, -1);
  } else {
    ObThreadCondGuard guard(cond_);
    --used_bg_count_;
  }
}

int ObSSMemBlockPool::free(ObSSMemBlock *mem_block)
{
  int ret = OB_SUCCESS;
  bool is_fg = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(mem_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_block));
  } else if (OB_UNLIKELY(!mem_block->is_valid() || mem_block->ref_cnt_ != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(mem_block));
  } else if (FALSE_IT(is_fg = mem_block->is_fg_mem_blk())) {
  } else if (OB_FAIL(free_dynamic_mem_block(mem_block))) {
    LOG_WARN("fail to free dynamic mem_blk", KR(ret), K(is_fg));
  } else {
    if (is_fg) {
      const int64_t cur_used_fg_cnt = ATOMIC_AAF(&used_fg_count_, -1);
      cache_stat_.mem_blk_stat().update_mem_blk_fg_used_cnt(-1);
      if (cur_used_fg_cnt < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fg_mem_blk used count is abnormal", KR(ret), K(cur_used_fg_cnt));
      }
    } else {
      ObThreadCondGuard guard(cond_);
      --used_bg_count_;
      cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(-1);
      cond_.signal();
      if (used_bg_count_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("bg_mem_blk used count is abnormal", KR(ret), K_(used_bg_count));
      }
    }
  }
  return ret;
}

int ObSSMemBlockPool::free_dynamic_mem_block(ObSSMemBlock *mem_block)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_block));
  } else {
    ob_free_align(mem_block->mem_blk_buf_);
    mem_block->reset();
    ob_free(mem_block);
    const int64_t delta_size = mem_blk_size_ + sizeof(ObSSMemBlock);
    cache_stat_.mem_stat().update_mem_blk_usage(-1 * delta_size);
  }
  return ret;
}

/*-----------------------------------------ObSSMicroBlockMetaInfo-----------------------------------------*/
ObSSMicroBlockMetaInfo::ObSSMicroBlockMetaInfo()
  : first_val_(0), second_val_(0), size_(0), access_type_(0), is_in_l1_(1), is_in_ghost_(0), is_data_persisted_(0), 
    is_meta_persisted_(0), is_meta_dirty_(0), is_meta_partial_(0), is_meta_deleted_(0), is_meta_serialized_(0),
    reserved_(0), reuse_version_(0), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), micro_key_()
{}

void ObSSMicroBlockMetaInfo::reset()
{
  first_val_ = 0;
  second_val_ = 0;
  size_ = 0;
  access_type_ = 0;
  is_in_l1_ = 1;
  is_in_ghost_ = 0;
  is_data_persisted_ = 0;
  is_meta_persisted_ = 0;
  is_meta_dirty_ = 0;
  is_meta_partial_ = 0;
  is_meta_deleted_ = 0;
  is_meta_serialized_ = 0;
  reserved_ = 0;
  reuse_version_ = 0;
  effective_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  micro_key_.reset();
}

ObSSMicroBlockMetaInfo& ObSSMicroBlockMetaInfo::operator=(const ObSSMicroBlockMetaInfo &other)
{
  if (this != &other) {
    first_val_ = other.first_val_;
    second_val_ = other.second_val_;
    third_val_ = other.third_val_;
    reuse_version_ = other.reuse_version_;
    effective_tablet_id_ = other.effective_tablet_id_;
    micro_key_ = other.micro_key_;
  }
  return *this;
}

bool ObSSMicroBlockMetaInfo::operator==(const ObSSMicroBlockMetaInfo &other) const
{
  return (first_val_ == other.first_val_) && (second_val_ == other.second_val_) && 
         (third_val_ == other.third_val_) && (micro_key_ == other.micro_key_) && 
         (effective_tablet_id_ == other.effective_tablet_id_) && (reuse_version_ == other.reuse_version_);
}

bool ObSSMicroBlockMetaInfo::operator!=(const ObSSMicroBlockMetaInfo &other) const
{
  return (!((*this) == other));
}

void ObSSMicroBlockMetaInfo::update_newest_access_time_s()
{
  const uint32_t cur_time_s = ObTimeUtility::current_time_s();
  if (access_time_s_ < cur_time_s) {
    access_time_s_ = cur_time_s;
  }
}

void ObSSMicroBlockMetaInfo::update_access_time_s(const int64_t delta_time_s)
{
  access_time_s_ += delta_time_s;
}

void ObSSMicroBlockMetaInfo::update_effective_tablet_id(const uint64_t effective_tablet_id)
{
  effective_tablet_id_ = effective_tablet_id;
}

void ObSSMicroBlockMetaInfo::update_access_type(const uint64_t access_type)
{
  access_type_ = access_type;
}

void ObSSMicroBlockMetaInfo::mark_invalid()
{
  first_val_ = 0;
  reuse_version_ = 0;
}

void ObSSMicroBlockMetaInfo::mark_deleted()
{
  is_meta_deleted_ = 1;
}

void ObSSMicroBlockMetaInfo::mark_meta_expired()
{
  access_time_s_ = 1001;
}

void ObSSMicroBlockMetaInfo::set_meta_serialized(const bool is_serialized)
{
  is_meta_serialized_ = is_serialized;
}

void ObSSMicroBlockMetaInfo::set_meta_dirty(const bool is_dirty)
{
  is_meta_dirty_ = is_dirty;
}

void ObSSMicroBlockMetaInfo::set_meta_persisted(const bool is_persisted)
{
  is_meta_persisted_ = is_persisted;
}

void ObSSMicroBlockMetaInfo::set_persisted_for_loading()
{
  set_meta_serialized(false);
  set_meta_persisted(true);
  set_meta_dirty(false);
}

bool ObSSMicroBlockMetaInfo::is_valid_field() const 
{ 
  return (0 != data_loc_) && (size_ > 0) && (access_time_s_ > 0) && (micro_key_.is_valid()) && (!is_meta_deleted_);
}

bool ObSSMicroBlockMetaInfo::is_abnormal() const
{
  return (!is_in_ghost_ && !is_valid_field());
}

bool ObSSMicroBlockMetaInfo::is_expired(const int64_t expiration_time) const
{
  // NOTICE: can't use current_time - access_time_s, may overflow
  return (ObTimeUtility::current_time_s() >= (access_time_s_ + expiration_time));
}

bool ObSSMicroBlockMetaInfo::is_prewarm_type() const
{
  return (is_prewarm_access_type(static_cast<ObSSMicroCacheAccessType>(access_type_)));
}

bool ObSSMicroBlockMetaInfo::can_evict() const
{
  return is_valid_field() && is_data_persisted_;
}

bool ObSSMicroBlockMetaInfo::can_delete() const
{
  return is_in_ghost_ && is_data_persisted_ && (!is_meta_deleted_);
}

bool ObSSMicroBlockMetaInfo::compare_check_dirty(const ObSSMicroBlockMetaInfo &other) const
{
  return (!((first_val_ == other.first_val_) && (reuse_version_ == other.reuse_version_) &&
          (effective_tablet_id_ == other.effective_tablet_id_) && (crc_ == other.crc_) &&
          (access_type_ == other.access_type_) && (is_in_l1_ == other.is_in_l1_) &&
          (is_in_ghost_ == other.is_in_ghost_) && (is_data_persisted_ == other.is_data_persisted_)));
}

int64_t ObSSMicroBlockMetaInfo::get_phy_blk_idx(const int32_t block_size) const
{
  int64_t phy_blk_idx = -1;
  if (is_data_persisted_ && (block_size > 0)) {
    phy_blk_idx = data_loc_ / block_size;
  }
  return phy_blk_idx;
}

int64_t ObSSMicroBlockMetaInfo::get_max_serialize_size()
{
  // We can use this func to alloc a buf which is enough to hold the serialized content
  return sizeof(ObSSMicroBlockMetaInfo) + 128;
}

OB_SERIALIZE_MEMBER(ObSSMicroBlockMetaInfo, first_val_, second_val_, third_val_, reuse_version_, 
    effective_tablet_id_, micro_key_);

/*-----------------------------------------ObSSMicroBlockMeta-----------------------------------------*/
ObSSMicroBlockMeta::ObSSMicroBlockMeta()
  : ObSSMicroBlockMetaInfo(), ref_cnt_(0), next_(nullptr)
{}

void ObSSMicroBlockMeta::reset()
{
  ObSSMicroBlockMetaInfo::reset();
  ref_cnt_ = 0;
  next_ = nullptr;
}

int ObSSMicroBlockMeta::init(
    const ObSSMicroBlockCacheKey &micro_key, 
    const ObSSMemBlockHandle &mem_blk_handle, 
    const uint32_t size,
    const uint32_t crc,
    const uint64_t effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid()) || OB_UNLIKELY(!micro_key.is_valid() || size <= 0 ||
      effective_tablet_id == ObTabletID::INVALID_TABLET_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), K(mem_blk_handle), K(effective_tablet_id));
  } else {
    inner_validate_micro_meta(micro_key, mem_blk_handle, size, crc, effective_tablet_id);
  } 
  return ret;
}

void ObSSMicroBlockMeta::transfer_arc_seg(const ObSSMicroArcOpType &op_type)
{
  int ret = OB_SUCCESS;
  if (ObSSMicroArcOpType::SS_NEW_INSERT == op_type) { // New added will be put into T1
    is_in_l1_ = true;
    is_in_ghost_ = false;
  } else if (ObSSMicroArcOpType::SS_ARC_HIT_GHOST == op_type) { // If cache_hit ghost, B1/B2 -> T2
    is_in_l1_ = false;
    is_in_ghost_ = false;
  } else if (ObSSMicroArcOpType::SS_ARC_EVICT == op_type) { // If evict, T1 -> B1 or T2 -> B2
    is_in_ghost_ = true;
  } else if (ObSSMicroArcOpType::SS_ARC_HIT_T1 == op_type) { // If hit T1 again, T1 -> T2
    is_in_l1_ = false;
  } else if (ObSSMicroArcOpType::SS_ARC_HIT_T2 == op_type) { // If hit T2
    // do nothing
  } else {
    LOG_WARN("invalid op_type", K(op_type));
  }
  update_newest_access_time_s();
}

int ObSSMicroBlockMeta::validate_micro_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlockHandle &mem_blk_handle,
    const uint32_t size,
    const uint32_t crc,
    const uint64_t effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid() || size <= 0 || size != size_ || !micro_key.is_valid()
      || !(micro_key == micro_key_) || (effective_tablet_id == ObTabletID::INVALID_TABLET_ID))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), K(mem_blk_handle), K(effective_tablet_id), 
      K(*this));
  } else if (OB_UNLIKELY(crc != crc_)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("micro_meta checksum error", KR(ret), K(micro_key), K(crc), K(*this));
  } else {
    inner_validate_micro_meta(micro_key, mem_blk_handle, size, crc, effective_tablet_id);
  }
  return ret;
}

void ObSSMicroBlockMeta::inner_validate_micro_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlockHandle &mem_blk_handle,
    const uint32_t size,
    const uint32_t crc,
    const uint64_t effective_tablet_id)
{
  inner_set_mem_block(mem_blk_handle);
  size_ = size;
  is_data_persisted_ = 0;
  update_newest_access_time_s();
  crc_ = crc;
  micro_key_ = micro_key;
  effective_tablet_id_ = effective_tablet_id;
  is_meta_deleted_ = 0;
}

int ObSSMicroBlockMeta::update_for_reorganize(const ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else {
    inner_set_mem_block(mem_blk_handle);
    is_data_persisted_ = 0;
  }
  return ret;
}

// NOTICE: this func will be invoked by 'try_load_persisted_micro_meta', thus we need to ensure
//         that, it is a persisted state, means: 'is_meta_serialized=false, is_meta_persisted=true,'
//         'is_meta_dirty=false'
int ObSSMicroBlockMeta::assign_micro_info_for_loading(const ObSSMicroBlockMetaInfo &micro_meta_info)
{
  int ret = OB_SUCCESS;
  first_val_ = micro_meta_info.first_val_;
  second_val_ = micro_meta_info.second_val_;
  third_val_ = micro_meta_info.third_val_;
  reuse_version_ = micro_meta_info.reuse_version_;
  effective_tablet_id_ = micro_meta_info.effective_tablet_id_;
  micro_key_ = micro_meta_info.micro_key_;
  set_persisted_for_loading();
  return ret;
}

void ObSSMicroBlockMeta::inner_set_mem_block(const ObSSMemBlockHandle &mem_blk_handle)
{
  if (mem_blk_handle.is_valid()) {
    data_loc_ = reinterpret_cast<uint64_t>(mem_blk_handle.get_ptr());
  }
}

uint32_t ObSSMicroBlockMeta::get_ref_count() const
{
  return ATOMIC_LOAD(&ref_cnt_);
}

void ObSSMicroBlockMeta::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMicroBlockMeta::dec_ref_count()
{
  try_free();
}

void ObSSMicroBlockMeta::try_free()
{
  int ret = OB_SUCCESS;
  const int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (ref_cnt == 0) {
    free();
  } else if (OB_UNLIKELY(ref_cnt < 0)) {
    LOG_ERROR("micro_meta's ref_cnt is invalid", KPC(this));
  }
}

void ObSSMicroBlockMeta::free()
{
  int ret = OB_SUCCESS;
  uint32_t cur_ref_cnt = 0;
  if (OB_UNLIKELY(0 != (cur_ref_cnt = ATOMIC_LOAD(&ref_cnt_)))) {
    LOG_ERROR("micro_meta's ref_cnt must be 0", K(cur_ref_cnt), KPC(this));
  } else {
    reset();
    SSMicroMetaAlloc.free(this);
    const int64_t micro_meta_usage = SSMicroMetaAlloc.hold();
    SSMicroCacheStat.mem_stat().update_micro_mem_usage(-1, micro_meta_usage);
  }
}

bool ObSSMicroBlockMeta::is_valid_or_in_ghost() const
{
  return (is_in_ghost() || is_valid());
}

bool ObSSMicroBlockMeta::is_valid_for_ckpt(const bool print_log) const
{
  const bool valid_field = is_valid_field();
  uint64_t blk_reuse_version = 0;
  bool b_ret = (is_data_persisted_ && (is_in_ghost_ || (valid_field && is_phy_block_match(blk_reuse_version))));
  if (!b_ret && print_log) {
    int ret = OB_SUCCESS;
    LOG_WARN("invalid for micro_cache ckpt", K(valid_field), K(blk_reuse_version), KPC(this));
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_valid_to_keep() const
{
  bool b_ret = false;
  const int64_t expiration_time = SSMicroMetaMgr.get_cache_expiration_time();
  if (!is_valid_field()) {
    // If meta is invalid, only keep ghost meta
    if (is_in_ghost()) {
      if (is_meta_deleted() || is_expired(expiration_time)) {
        b_ret = false;
      } else {
        b_ret = true;
      }
    } else {
      LOG_WARN_RET(OB_SUCCESS, "micro_meta info is invalid", KPC(this));
    }
  } else if (is_meta_deleted() || is_expired(expiration_time)) {
    // If meta is deleted or expired, do not keep it
  } else if (is_data_persisted()) {
    if (is_phy_block_match()) {
      b_ret = true; // If data is persisted and reuse_version is matched, keep it
    }
  } else if (is_mem_block_match()) {
    b_ret = true; // If data is not persisted, verify mem_block is still valid
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_valid() const
{
  bool b_ret = is_valid_field();
  if (b_ret) {
    if (is_data_persisted_) {
      b_ret = is_phy_block_match();
    } else {
      b_ret = is_mem_block_match();
    }
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_valid(const ObSSMemBlockHandle &mem_blk_handle) const
{
  return is_valid_field() && is_mem_block_match(mem_blk_handle);
}

bool ObSSMicroBlockMeta::is_valid(const int64_t phy_blk_idx) const
{
  const int32_t block_size = SSPhyBlockMgr.get_block_size();
  return is_valid_field() && is_phy_block_match() && 
         (get_phy_blk_idx(block_size) == phy_blk_idx);
}

bool ObSSMicroBlockMeta::is_mem_block_match() const
{
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(get_mem_block());
  return is_mem_block_match(mem_blk_handle);
}

// micro_meta's is_data_persisted_ must be false, @mem_blk should not be null.
// micro_meta's mem_blk_ must be equal to @mem_blk, and this micro_block should 
// be in mem_blk's index map, thus we think this micro_meta's mem_blk is matched.
bool ObSSMicroBlockMeta::is_mem_block_match(const ObSSMemBlockHandle &mem_blk_handle) const
{
  bool b_ret = (!is_data_persisted_) && (mem_blk_handle.is_valid()) && (get_mem_block() == mem_blk_handle.get_ptr());
  if (b_ret) {
    b_ret = mem_blk_handle()->exist_micro(micro_key_);
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_phy_block_match() const
{
  uint64_t reuse_version = 0;
  return is_phy_block_match(reuse_version);
}

bool ObSSMicroBlockMeta::is_phy_block_match(uint64_t &real_reuse_version) const
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (is_data_persisted_) {
    ObSSPhyBlockHandle phy_blk_handle;
    real_reuse_version = 0;
    if (OB_FAIL(SSPhyBlockMgr.get_block_handle(data_loc_, reuse_version_, phy_blk_handle, real_reuse_version))) {
      LOG_WARN("fail to get phy_block handle", KR(ret), KPC(this));
    } else if (OB_LIKELY(phy_blk_handle.is_valid())) {
      b_ret = true;
    }
  }
  return b_ret;
}

ObSSMemBlock* ObSSMicroBlockMeta::get_mem_block() const
{
  ObSSMemBlock* mem_blk = nullptr;
  if (!is_data_persisted_) {
    mem_blk = reinterpret_cast<ObSSMemBlock *>(data_loc_);
  }
  return mem_blk;
}

bool ObSSMicroBlockMeta::can_reorganize() const
{
  return is_valid_field() && is_phy_block_match() && (!is_in_ghost_);
}

void ObSSMicroBlockMeta::get_micro_meta_info(ObSSMicroBlockMetaInfo &micro_meta_info) const
{
  micro_meta_info.first_val_ = first_val_;
  micro_meta_info.second_val_ = second_val_;
  micro_meta_info.third_val_ = third_val_;
  micro_meta_info.effective_tablet_id_ = effective_tablet_id_;
  micro_meta_info.reuse_version_ = reuse_version_;
  micro_meta_info.micro_key_ = micro_key_;
}
/*-----------------------------------------ObSSAggregateMicroInfo-----------------------------------------*/
bool ObSSAggregateMicroInfo::operator< (const ObSSAggregateMicroInfo &other) const
{
  bool b_ret = false;
  if (is_in_l1_ != other.is_in_l1_) {
    b_ret = (is_in_l1_ > other.is_in_l1_);
  } else if (tablet_id_ != other.tablet_id_) {
    b_ret = (tablet_id_ < other.tablet_id_);
  } else {
    b_ret = (ls_id_ < other.ls_id_);
  }
  return b_ret;
}

ObSSAggregateMicroInfo& ObSSAggregateMicroInfo::operator=(const ObSSAggregateMicroInfo &other)
{
  if (this != &other) {
    tablet_id_ = other.tablet_id_;
    ls_id_ = other.ls_id_;
    is_in_l1_ = other.is_in_l1_;
  }
  return *this;
}

/*-----------------------------------------ObSSMicroPrewarmMeta-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMicroPrewarmMeta, micro_key_, effective_tablet_id_, data_crc_,
                    data_size_, is_in_l1_);

ObSSMicroPrewarmMeta::ObSSMicroPrewarmMeta()
  : micro_key_(), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), data_crc_(0),
    data_size_(0), is_in_l1_(true)
{}

ObSSMicroPrewarmMeta::ObSSMicroPrewarmMeta(
  const ObSSMicroBlockCacheKey &micro_key,
  const uint64_t effective_tablet_id,
  const uint32_t data_crc,
  const int32_t data_size,
  const bool is_in_l1)
  : micro_key_(micro_key), effective_tablet_id_(effective_tablet_id), data_crc_(data_crc),
    data_size_(data_size), is_in_l1_(is_in_l1)
{}

ObSSMicroPrewarmMeta::ObSSMicroPrewarmMeta(const ObSSMicroPrewarmMeta &other)
{
  IGNORE_RETURN assign(other);
}

int ObSSMicroPrewarmMeta::assign(const ObSSMicroPrewarmMeta &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    micro_key_ = other.micro_key_;
    effective_tablet_id_ = other.effective_tablet_id_;
    data_crc_ = other.data_crc_;
    data_size_ = other.data_size_;
    is_in_l1_ = other.is_in_l1_;
  }
  return ret;
}

bool ObSSMicroPrewarmMeta::operator==(const ObSSMicroPrewarmMeta &other) const
{
  return (micro_key_ == other.micro_key_) && (data_crc_ == other.data_crc_) && (data_size_ == other.data_size_);
}

uint64_t ObSSMicroPrewarmMeta::hash() const
{
  // no need to calculate hash value for hit_type and effective_tablet_id,
  // because hit_type and effective_tablet_id of the same micro_key may be different.
  uint64_t hash_val = micro_key_.hash();
  hash_val = murmurhash(&data_crc_, sizeof(data_crc_), hash_val);
  hash_val = murmurhash(&data_size_, sizeof(data_size_), hash_val);
  return hash_val;
}

bool ObSSMicroPrewarmMeta::is_valid() const
{
  return micro_key_.is_valid() && (data_size_ > 0);
}

/*-----------------------------------------ObSSAllSegMetaInfo------------------------------------------*/
ObSSAllSegMetaInfo::ObSSSegMetaInfo& ObSSAllSegMetaInfo::ObSSSegMetaInfo::operator=(
    const ObSSAllSegMetaInfo::ObSSSegMetaInfo &other)
{
  if (this != &other) {
    cnt_ = other.cnt_;
    size_ = other.size_;
  }
  return *this;
}

void ObSSAllSegMetaInfo::reset()
{
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    seg_info_arr_[i].reset();
  }
}

int ObSSAllSegMetaInfo::assign(const ObSSAllSegMetaInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
      seg_info_arr_[i] = other.seg_info_arr_[i];
    }
  }
  return ret;
}

int64_t ObSSAllSegMetaInfo::get_cnt_by_idx(const int64_t seg_idx) const
{
  int64_t cnt = 0;
  if (is_valid_arc_seg_idx(seg_idx)) {
    cnt = seg_info_arr_[seg_idx].cnt_;
  }
  return cnt;
}

int64_t ObSSAllSegMetaInfo::get_size_by_idx(const int64_t seg_idx) const
{
  int64_t size = 0;
  if (is_valid_arc_seg_idx(seg_idx)) {
    size = seg_info_arr_[seg_idx].size_;
  }
  return size;
}

int64_t ObSSAllSegMetaInfo::get_total_cnt() const
{
  int64_t total_cnt = 0;
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    total_cnt += seg_info_arr_[i].cnt_;
  }
  return total_cnt;
}

int64_t ObSSAllSegMetaInfo::get_total_size() const
{
  int64_t total_size = 0;
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    total_size += seg_info_arr_[i].size_;
  }
  return total_size;
}

void ObSSAllSegMetaInfo::update_seg_meta_info(
    const bool is_in_l1, 
    const bool is_in_ghost,
    const int64_t cnt, 
    const int64_t size)
{
  const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
  seg_info_arr_[seg_idx].update_meta_info(cnt, size);
}

/*-----------------------------------------PUBLIC INTERFACE-----------------------------------------*/
void check_range_cnt(const uint64_t range_cnt)
{
  int ret = OB_SUCCESS;
  if (range_cnt >= UINT32_MAX) {
    LOG_ERROR("range_list may exist cycle", K(range_cnt));
  }
}

/*-----------------------------------------ObSSMicroSubRangeInfo-----------------------------------------*/
ObSSMicroSubRangeInfo::ObSSMicroSubRangeInfo()
  : offset_(0), length_(0), micro_meta_cnt_(0), reuse_version_(0), t1_pmeta_cnt_(0), t2_pmeta_cnt_(0),
    b1_pmeta_cnt_(0), b2_pmeta_cnt_(0), all_mem_meta_(1), is_loading_meta_(0), reserved_(0),
    b1_pmeta_size_(0), b2_pmeta_size_(0), phy_blk_idx_(SS_INVALID_PHY_BLK_ID), end_hval_(0), 
    ref_cnt_(1), first_micro_(nullptr), next_(nullptr), bl_filter_(nullptr)
{}

void ObSSMicroSubRangeInfo::reset()
{
  offset_ = 0;
  length_ = 0;
  micro_meta_cnt_ = 0;
  reuse_version_ = 0;
  phy_blk_idx_ = SS_INVALID_PHY_BLK_ID;
  end_hval_ = 0;
  ref_cnt_ = 1;
  t1_pmeta_cnt_ = 0;
  t2_pmeta_cnt_ = 0;
  b1_pmeta_cnt_ = 0;
  b2_pmeta_cnt_ = 0;
  all_mem_meta_ = 1;
  is_loading_meta_ = 0;
  reserved_ = 0;
  b1_pmeta_size_ = 0;
  b2_pmeta_size_ = 0;
  first_micro_ = nullptr;
  next_ = nullptr;
  bl_filter_ = nullptr;
}

bool ObSSMicroSubRangeInfo::is_valid() const
{
  return (end_hval_ > 0 && ref_cnt_ >= 1);
}

void ObSSMicroSubRangeInfo::try_free(bool &succ_freed)
{
  int ret = OB_SUCCESS;
  succ_freed = false;
  const uint32_t cur_ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (0 == cur_ref_cnt) {
    SSMicroRangeMgr.destroy_sub_range(this);
    succ_freed = true;
  } else if (OB_UNLIKELY(UINT32_MAX == cur_ref_cnt)) {
    LOG_ERROR("micro_range_info's ref_cnt is invalid", K(*this));
  }
}

void ObSSMicroSubRangeInfo::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMicroSubRangeInfo::dec_ref_count()
{
  bool succ_freed = false;
  try_free(succ_freed);
}

// set_offset_len should be invoked under init_range lock to avoid bitfield race condition
void ObSSMicroSubRangeInfo::set_offset_len(const uint64_t offset, const uint64_t length)
{
  offset_ = offset;
  length_ = length;
}

int64_t ObSSMicroSubRangeInfo::get_pmeta_cnt_by_seg_idx(const int64_t seg_idx) const
{
  uint64_t pmeta_cnt = 0;
  if (SS_ARC_T1 == seg_idx) {
    pmeta_cnt = t1_pmeta_cnt_;
  } else if (SS_ARC_T2 == seg_idx) {
    pmeta_cnt = t2_pmeta_cnt_;
  } else if (SS_ARC_B1 == seg_idx) {
    pmeta_cnt = b1_pmeta_cnt_;
  } else if (SS_ARC_B2 == seg_idx) {
    pmeta_cnt = b2_pmeta_cnt_;
  }
  return pmeta_cnt;
}

int64_t ObSSMicroSubRangeInfo::get_ghost_pmeta_cnt() const
{
  return (b1_pmeta_cnt_ + b2_pmeta_cnt_);
}

int64_t ObSSMicroSubRangeInfo::get_valid_pmeta_cnt() const
{
  return (t1_pmeta_cnt_ + t2_pmeta_cnt_);
}

int64_t ObSSMicroSubRangeInfo::get_all_pmeta_cnt() const
{
  return (t1_pmeta_cnt_ + b1_pmeta_cnt_ + t2_pmeta_cnt_ + b2_pmeta_cnt_);
}

int ObSSMicroSubRangeInfo::update_pmeta_info(
    const int64_t seg_idx, 
    const int64_t delta_cnt, 
    const int64_t delta_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seg_idx", KR(ret), K(seg_idx));
  } else {
    switch (seg_idx) {
      case SS_ARC_T1:
        t1_pmeta_cnt_ += delta_cnt;
        break;
      case SS_ARC_B1:
        b1_pmeta_cnt_ += delta_cnt;
        b1_pmeta_size_ += delta_size;
        break;
      case SS_ARC_T2:
        t2_pmeta_cnt_ += delta_cnt;
        break;
      case SS_ARC_B2:
        b2_pmeta_cnt_ += delta_cnt;
        b2_pmeta_size_ += delta_size;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected seg_idx", KR(ret), K(seg_idx));
        break;
    }
  }
  return ret;
}

bool ObSSMicroSubRangeInfo::reach_micro_limit() const
{
  return micro_meta_cnt_ >= SS_SUB_RNG_PERSIST_META_MAX_CNT;
}

bool ObSSMicroSubRangeInfo::contain_hval(const uint32_t hval) const
{
  return hval <= end_hval_;
}

bool ObSSMicroSubRangeInfo::has_next_range() const
{
  return (nullptr != next_);
}

bool ObSSMicroSubRangeInfo::has_micro_meta() const
{
  return (micro_meta_cnt_ > 0 && nullptr != first_micro_);
}

bool ObSSMicroSubRangeInfo::exist_persisted_meta() const
{
  return ((phy_blk_idx_ != SS_INVALID_PHY_BLK_ID) && (offset_ >= 0) && (length_ > 0));
}

bool ObSSMicroSubRangeInfo::exist_ghost_persisted_meta() const
{
  return (exist_persisted_meta() && (get_ghost_pmeta_cnt() > 0));
}

bool ObSSMicroSubRangeInfo::is_empty() const
{
  return (nullptr == next_ && nullptr == first_micro_);
}

int ObSSMicroSubRangeInfo::assign(const ObSSMicroSubRangeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    first_val_ = other.first_val_;
    second_val_ = other.second_val_;
    third_val_ = other.third_val_;
    b1_pmeta_size_ = other.b1_pmeta_size_;
    b2_pmeta_size_ = other.b2_pmeta_size_;
    phy_blk_idx_ = other.phy_blk_idx_;
    end_hval_ = other.end_hval_;
    ref_cnt_ = other.ref_cnt_;
    first_micro_ = other.first_micro_;
    next_ = other.next_;
    bl_filter_ = other.bl_filter_;
  }
  return ret;
}

int ObSSMicroSubRangeInfo::link_micro_meta(
    ObSSMicroBlockMetaHandle &micro_handle,
    const int64_t micro_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_handle.is_valid() || micro_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_handle), K(micro_cnt));
  } else {
    ObSSMicroBlockMeta *next_micro = first_micro_;
    first_micro_ = micro_handle.get_ptr();
    micro_handle()->set_next_micro(next_micro);
    micro_meta_cnt_ += micro_cnt;
  }
  return ret;
}

int ObSSMicroSubRangeInfo::link_next_range(ObSSMicroSubRangeInfo *next_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(next_range));
  } else if (OB_NOT_NULL(next_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already exists next micro_range", KR(ret), KPC(this), KP(next_range));
  } else {
    next_ = next_range;
  }
  return ret;
}

void ObSSMicroSubRangeInfo::remove_micro_list(ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt)
{
  first_micro_meta = first_micro_;
  first_micro_ = nullptr;
  phy_blk_idx_ = SS_INVALID_PHY_BLK_ID;
  offset_ = 0;
  length_ = 0;
  micro_cnt = micro_meta_cnt_;
  micro_meta_cnt_ = 0;
  if (nullptr != bl_filter_) {
    SSMicroRangeMgr.destroy_sub_range_bloom_filter(this);
  }
}

void ObSSMicroSubRangeInfo::remove_micro_list_only(ObSSMicroBlockMeta *&first_micro_meta, int64_t &micro_cnt)
{
  first_micro_meta = first_micro_;
  first_micro_ = nullptr;
  micro_cnt = micro_meta_cnt_;
  micro_meta_cnt_ = 0;
}

void ObSSMicroSubRangeInfo::get_sub_range_by_hval(const uint32_t hval, ObSSMicroSubRangeInfo *&sub_range)
{
  uint64_t range_cnt = 1;
  if (contain_hval(hval)) {
    sub_range = this;
  } else {
    ObSSMicroSubRangeInfo *cur_sub_range = next_;
    while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
      ++range_cnt;
      if (cur_sub_range->contain_hval(hval)) {
        sub_range = cur_sub_range;
        break;
      }
      cur_sub_range = cur_sub_range->get_next_range();
    }
    check_range_cnt(range_cnt);
  }
}

void ObSSMicroSubRangeInfo::get_sub_range_by_seg_idx(const int64_t seg_idx, ObSSMicroSubRangeInfo *&sub_range)
{
  uint64_t range_cnt = 1;
  if (!all_mem_meta_ && get_pmeta_cnt_by_seg_idx(seg_idx) > 0) {
    sub_range = this;
  } else {
    ObSSMicroSubRangeInfo *cur_sub_range = next_;
    while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
      ++range_cnt;
      if (!all_mem_meta_ && cur_sub_range->get_pmeta_cnt_by_seg_idx(seg_idx) > 0) {
        sub_range = cur_sub_range;
        break;
      }
      cur_sub_range = cur_sub_range->get_next_range();
    }
    check_range_cnt(range_cnt);
  }
}

int ObSSMicroSubRangeInfo::add_hval_into_bloom_filter(const uint32_t hval)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bl_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bloom_filter should not be null", KR(ret), KPC(this));
  } else if (OB_FAIL(bl_filter_->insert(hval))) {
    LOG_WARN("fail to insert into bloom_filter", KR(ret), K(hval), KPC(this));
  }
  return ret;
}

int ObSSMicroSubRangeInfo::exist_persisted_micro_meta(
    const uint32_t hval, 
    bool &may_exist)
{
  int ret = OB_SUCCESS;
  may_exist = false;
  if (OB_UNLIKELY(hval > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hval), KPC(this));
  } else if (nullptr != bl_filter_ && !all_mem_meta_) {
    if (OB_LIKELY(exist_ghost_persisted_meta())) {
      if (OB_FAIL(bl_filter_->may_contain(hval, may_exist))) {
        LOG_WARN("fail to execute may_contain", KR(ret), K(hval), KPC(this));
      }
    }
  }
  return ret;
}

void ObSSMicroSubRangeInfo::clear_persisted_meta_info(ObSSAllSegMetaInfo &all_seg_meta_info)
{
  if (!is_loading_meta_ && !all_mem_meta_) {
    FLOG_INFO("will clear this sub_range persisted info", KPC(this));
    // NOTICE: now, only record ghost info
    if ((b1_pmeta_cnt_ > 0) && (b1_pmeta_size_ > 0)) {
      all_seg_meta_info.update_seg_meta_info(true/*is_in_l1*/, true/*is_in_ghost*/, b1_pmeta_cnt_, b1_pmeta_size_);
    }

    if ((b2_pmeta_cnt_ > 0) && (b2_pmeta_size_ > 0)) {
      all_seg_meta_info.update_seg_meta_info(false/*is_in_l1*/, true/*is_in_ghost*/, b2_pmeta_cnt_, b2_pmeta_size_);
    }

    // Set all_mem_meta_ to true, means all micro_meta in this sub_range has been loaded into memory.
    // Thus, we don't need to reset pmeta_cnt and pmeta_size
    all_mem_meta_ = true;
  }
}

/*-----------------------------------------ObSSMicroInitRangeInfo-----------------------------------------*/
ObSSMicroInitRangeInfo::ObSSMicroInitRangeInfo()
  : lock_(common::ObLatchIds::OB_SS_MICRO_INIT_RANGE_INFO_LOCK), end_hval_(0), next_(nullptr)
{}

void ObSSMicroInitRangeInfo::reset()
{
  SpinWLockGuard guard(lock_);
  end_hval_ = 0;
  next_ = nullptr;
}

bool ObSSMicroInitRangeInfo::has_sub_ranges() const
{
  SpinRLockGuard guard(lock_);
  return (nullptr != next_);
}

uint32_t ObSSMicroInitRangeInfo::get_end_hval() const
{
  SpinRLockGuard guard(lock_);
  return end_hval_;
}

ObSSMicroSubRangeInfo *ObSSMicroInitRangeInfo::get_first_sub_range() const
{
  SpinRLockGuard guard(lock_);
  return next_;
}

int64_t ObSSMicroInitRangeInfo::get_total_micro_cnt(const bool include_pmeta) const
{
  int64_t total_micro_cnt = 0;
  SpinRLockGuard guard(lock_);
  ObSSMicroSubRangeInfo *cur_sub_range = next_;
  uint64_t range_cnt = 0;
  while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
    ++range_cnt;
    total_micro_cnt += cur_sub_range->get_micro_meta_cnt();
    if (include_pmeta) {
      // Now only need to include persisted ghost micro_meta
      total_micro_cnt += cur_sub_range->get_ghost_pmeta_cnt();
    }
    cur_sub_range = cur_sub_range->get_next_range();
  }
  check_range_cnt(range_cnt);
  return total_micro_cnt;
}

int64_t ObSSMicroInitRangeInfo::get_sub_range_cnt() const
{
  uint64_t range_cnt = 0;
  SpinRLockGuard guard(lock_);
  ObSSMicroSubRangeInfo *cur_sub_range = next_;
  while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
    ++range_cnt;
    cur_sub_range = cur_sub_range->get_next_range();
  }
  check_range_cnt(range_cnt);
  return range_cnt;
}

int ObSSMicroInitRangeInfo::link_micro_meta_directly(
    ObSSMicroSubRangeInfo *target_sub_range, 
    ObSSMicroBlockMetaHandle &micro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_sub_range) || OB_UNLIKELY(!micro_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(target_sub_range), K(micro_handle));
  } else if (OB_UNLIKELY(target_sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(target_sub_range), KPC(this));
  } else {
    SpinWLockGuard guard(lock_);
    const ObSSMicroBlockCacheKey &micro_key = micro_handle()->get_micro_key();
    if (OB_LIKELY(target_sub_range->contain_hval(micro_key.micro_hash()))) {
      if (OB_FAIL(target_sub_range->link_micro_meta(micro_handle, 1))) {
        LOG_ERROR("fail to link micro_meta into sub_range", KR(ret), K(micro_key), KPC(target_sub_range), KPC(this));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("this micro not belong to target_sub_range", KR(ret), K(micro_key), KPC(target_sub_range), KPC(this));
    }
  }
  return ret;
}

int ObSSMicroInitRangeInfo::link_micro_meta_by_hval(
    const uint32_t hval, 
    ObSSMicroBlockMetaHandle &micro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_handle));
  } else {
    SpinWLockGuard guard(lock_);
    if (nullptr == next_) {
      ObSSMicroSubRangeInfo *sub_range = nullptr;
      if (OB_FAIL(SSMicroRangeMgr.create_sub_range(sub_range, false/*retry_alloc*/))) {
        LOG_WARN("fail to create sub_range", KR(ret), KPC(this));
      } else if (OB_ISNULL(sub_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new sub_range should not be null", KR(ret), KP(sub_range));
      } else {
        sub_range->set_end_hval(end_hval_);
        next_ = sub_range;
      }
    }
    
    if (OB_SUCC(ret)) {
      ObSSMicroSubRangeInfo *cur_sub_rng = next_;
      bool find_range = false;
      uint64_t range_cnt = 0;
      while (OB_SUCC(ret) && (nullptr != cur_sub_rng) && (!find_range) && (range_cnt < UINT32_MAX)) {
        ++range_cnt;
        if (cur_sub_rng->contain_hval(hval)) {
          if (OB_FAIL(cur_sub_rng->link_micro_meta(micro_handle, 1))) {
            LOG_ERROR("fail to link micro_meta into sub_range", KR(ret), KPC(cur_sub_rng), KPC(this));
          }
          find_range = true;
        }
        cur_sub_rng = cur_sub_rng->get_next_range();
      }

      if (OB_SUCC(ret) && !find_range) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find a sub_range in init_range", KR(ret), K(hval), KPC(this));
      }
      check_range_cnt(range_cnt);
    }
  }
  return ret;
}

void ObSSMicroInitRangeInfo::get_sub_range_by_hval(
    const uint32_t hval,
    ObSSMicroSubRangeHandle &range_handle)
{
  SpinRLockGuard guard(lock_);
  if (nullptr != next_) {
    ObSSMicroSubRangeInfo *target_sub_range = nullptr;
    next_->get_sub_range_by_hval(hval, target_sub_range);
    if (nullptr != target_sub_range) {
      range_handle.set_ptr(target_sub_range);
    }
  }
}

int ObSSMicroInitRangeInfo::get_persisted_sub_range(
    const int64_t seg_idx, 
    ObSSMicroSubRangeHandle &range_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(seg_idx >= SS_ARC_SEG_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx));
  } else {
    SpinRLockGuard guard(lock_);
    if (nullptr != next_) {
      ObSSMicroSubRangeInfo *target_sub_range = nullptr;
      next_->get_sub_range_by_seg_idx(seg_idx, target_sub_range);
      if (nullptr != target_sub_range) {
        range_handle.set_ptr(target_sub_range);
      }
    }
  }
  return ret;
}

int ObSSMicroInitRangeInfo::link_sub_ranges(
    ObSSMicroSubRangeInfo *first_sub_range, 
    ObSSMicroSubRangeInfo *&inc_first_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(first_sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(first_sub_range));
  } else {
    SpinWLockGuard guard(lock_);
    inc_first_range = next_;
    next_ = first_sub_range;

    ObSSMicroSubRangeInfo *cur_sub_range = first_sub_range;
    uint64_t range_cnt = 0; 
    while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
      ++range_cnt;
      if (nullptr == cur_sub_range->get_next_range()) { // update the last sub_range end_hval
        cur_sub_range->set_end_hval(end_hval_);
      }
      cur_sub_range = cur_sub_range->next_;
    }
    check_range_cnt(range_cnt);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::try_free_sub_ranges()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObSSMicroSubRangeInfo *cur_sub_range = next_;
  next_ = nullptr;
  uint64_t range_cnt = 0;
  while (nullptr != cur_sub_range && range_cnt < UINT32_MAX) {
    ++range_cnt;
    ObSSMicroSubRangeInfo *next_sub_range = cur_sub_range->get_next_range();
    cur_sub_range->set_next_range(nullptr);
    bool succ_freed = false;
    cur_sub_range->try_free(succ_freed);
    cur_sub_range = next_sub_range;
  }
  check_range_cnt(range_cnt);
  return ret;
}

void ObSSMicroInitRangeInfo::remove_sub_ranges(ObSSMicroSubRangeInfo *&first_sub_range)
{
  SpinWLockGuard guard(lock_);
  if (nullptr != next_) {
    first_sub_range = next_;
    next_ = nullptr;
  }
}

int ObSSMicroInitRangeInfo::remove_sub_range_micro_info(
    ObSSMicroSubRangeInfo *sub_range, 
    ObSSMicroBlockMeta *&first_micro_meta,
    int64_t &micro_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(sub_range), KPC(this));
  } else {
    SpinWLockGuard guard(lock_);
    sub_range->remove_micro_list(first_micro_meta, micro_cnt);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::remove_sub_range_micro_list(
    ObSSMicroSubRangeInfo *sub_range, 
    ObSSMicroBlockMeta *&first_micro_meta,
    int64_t &micro_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(sub_range), KPC(this));
  } else {
    SpinWLockGuard guard(lock_);
    sub_range->remove_micro_list_only(first_micro_meta, micro_cnt);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::check_exist_persisted_micro_meta(
    const uint32_t hval, 
    bool &may_exist,
    ObSSMicroSubRangeHandle &range_handle)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(hval > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hval), KPC(this));
  }

  ObSSMicroSubRangeInfo *cur_sub_rng = next_;
  bool find_range = false;
  uint64_t range_cnt = 0;
  while (OB_SUCC(ret) && (nullptr != cur_sub_rng) && (!find_range) && (range_cnt < UINT32_MAX)) {
    ++range_cnt;
    if (cur_sub_rng->contain_hval(hval)) {
      if (OB_FAIL(cur_sub_rng->exist_persisted_micro_meta(hval, may_exist))) {
        LOG_WARN("fail to check whether hval exist in bloom filter", KR(ret), K(hval), KPC(cur_sub_rng), KPC(this));
      } else {
        range_handle.set_ptr(cur_sub_rng);
      }
      find_range = true;
    }
    cur_sub_rng = cur_sub_rng->get_next_range();
  }

  if (OB_SUCC(ret) && !find_range && (nullptr != next_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find a sub_range in init_range", KR(ret), K(hval), KPC(this));
  }
  return ret;
}

int ObSSMicroInitRangeInfo::start_load_persisted_micro_meta(
    ObSSMicroSubRangeInfo *sub_range,
    bool &all_mem_meta, 
    bool &is_loading_meta)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range), KPC(this));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(sub_range), KPC(this));
  } else {
    all_mem_meta = sub_range->is_all_mem_meta();
    is_loading_meta = sub_range->is_loading_meta();
    if (all_mem_meta || is_loading_meta) { // skip it
    } else {
      sub_range->set_loading_meta(true);
    }
  }
  return ret;
}

int ObSSMicroInitRangeInfo::finish_load_persisted_micro_meta(
    ObSSMicroSubRangeInfo *sub_range, 
    const bool all_mem_meta)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range), KPC(this));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(sub_range), KPC(this));
  } else {
    sub_range->set_loading_meta(false);
    sub_range->set_all_mem_meta(all_mem_meta);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::update_sub_range_all_mem_meta(
    ObSSMicroSubRangeInfo *sub_range, 
    const bool all_mem_meta)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range), KPC(this));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(sub_range), KPC(this));
  } else {
    sub_range->set_all_mem_meta(all_mem_meta);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::set_sub_range_offset_len(
    ObSSMicroSubRangeInfo *sub_range,
    const uint64_t offset,
    const uint64_t length)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_range is null", KR(ret), KP(sub_range));
  } else if (OB_UNLIKELY(sub_range->get_end_hval() > end_hval_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_range end_hval is greater than init_range end_hval", KR(ret), KPC(sub_range), KPC(this));
  } else {
    sub_range->set_offset_len(offset, length);
  }
  return ret;
}

int ObSSMicroInitRangeInfo::clear_sub_range_pmeta_info(ObSSAllSegMetaInfo &all_seg_meta_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObSSMicroSubRangeInfo *sub_range = next_;
  uint64_t range_cnt = 0;
  // save all sub_ranges' persisted micro_meta info into @all_seg_meta_info before clearing them
  while ((nullptr != sub_range) && (range_cnt < UINT32_MAX)) {
    ++range_cnt;
    if ((!sub_range->is_loading_meta()) && (!sub_range->is_all_mem_meta())) {
      sub_range->clear_persisted_meta_info(all_seg_meta_info);
    }
    sub_range = sub_range->get_next_range();
  }
  check_range_cnt(range_cnt);
  return ret;
}

int ObSSMicroInitRangeInfo::get_sub_range_snapshot_info(
    ObSSMicroSubRangeInfo *sub_range, 
    ObSSMicroSubRangeInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_range is null", KR(ret), KP(sub_range));
  } else if (OB_FAIL(snapshot_info.assign(*sub_range))) {
    LOG_WARN("fail to assign sub_range", KR(ret), KP(sub_range), KPC(sub_range));
  }
  return ret;
}

int ObSSMicroInitRangeInfo::update_sub_range_persisted_info(
    ObSSMicroSubRangeInfo *sub_range, 
    const int64_t phy_blk_idx, 
    const uint64_t reuse_version,
    const int64_t offset,
    const int64_t length, 
    const ObSSAllSegMetaInfo &all_seg_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(sub_range) || OB_UNLIKELY(phy_blk_idx <= 0 || offset < 0 || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_range is null", KR(ret), KP(sub_range), K(phy_blk_idx), K(offset), K(length));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < SS_ARC_SEG_COUNT); ++i) {
      const int64_t delta_cnt = all_seg_info.seg_info_arr_[i].cnt_;
      const int64_t delta_size = all_seg_info.seg_info_arr_[i].size_;
      if (OB_FAIL(sub_range->update_pmeta_info(i, delta_cnt, delta_size))) {
        LOG_WARN("fail to update pmeta info", KR(ret), K(i), K(delta_cnt), K(delta_size));
      }
    }

    if (OB_SUCC(ret)) {
      sub_range->set_phy_blk_idx(phy_blk_idx);
      sub_range->set_reuse_version(reuse_version);
      sub_range->set_offset_len(offset, length);
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMicroRangeTabletCacheScanFilter-----------------------------------------*/
bool ObSSMicroRangeTabletCacheScanFilter::should_include(const ObSSMicroBlockMetaInfo &micro_info)
{
  return ((!micro_info.is_meta_deleted()) && (micro_info.is_in_ghost() || (micro_info.is_valid_field())));
}

/*-----------------------------------------ObSSMicroRangePersistScanFilter-----------------------------------------*/
bool ObSSMicroRangePersistScanFilter::should_include(const ObSSMicroBlockMetaInfo &micro_info)
{
  return ((!micro_info.is_meta_deleted()) && (micro_info.is_data_persisted()) && 
          (micro_info.is_in_ghost() || (micro_info.is_valid_field())));
}

/*-----------------------------------------ObSSReplayMicroCkptCtx------------------------------------------*/
void ObSSReplayMicroCkptCtx::reset()
{
  total_replay_cnt_ = 0;
  invalid_t1_micro_cnt_ = 0;
  invalid_t2_micro_cnt_ = 0;
  b1_micro_cnt_ = 0;
  b2_micro_cnt_ = 0;
}

void ObSSReplayMicroCkptCtx::inc_total_replay_cnt()
{
  ATOMIC_INC(&total_replay_cnt_);
}

void ObSSReplayMicroCkptCtx::inc_invalid_micro_cnt(const bool is_in_l1, const bool is_in_ghost)
{
  if (is_in_l1 && !is_in_ghost) {
    ATOMIC_INC(&invalid_t1_micro_cnt_);
  } else if (!is_in_l1 && !is_in_ghost) {
    ATOMIC_INC(&invalid_t2_micro_cnt_);
  }
}

void ObSSReplayMicroCkptCtx::inc_ghost_micro_cnt(const bool is_in_l1, const bool is_in_ghost)
{
  if (is_in_l1 && is_in_ghost) {
    ATOMIC_INC(&b1_micro_cnt_);
  } else if (!is_in_l1 && is_in_ghost) {
    ATOMIC_INC(&b2_micro_cnt_);
  }
}

} /* namespace storage */
} /* namespace oceanbase */
