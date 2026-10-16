/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "common/ob_tablet_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_object_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

const ObSSMacroCacheDesc OB_SS_MACRO_CACHE_DESCS[] =
{
#define DEF_SS_MACRO_CACHE(type, min, max, weight, type_str, class_name) {min, max, weight},
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#undef DEF_SS_MACRO_CACHE
};

bool is_valid_ss_macro_cache_type(const ObSSMacroCacheType type)
{
  return type >= ObSSMacroCacheType::META_FILE && type < ObSSMacroCacheType::MAX_TYPE;
}

bool is_valid_ss_macro_block_type(const ObSSMacroBlockType type)
{
  return type >= ObSSMacroBlockType::PRIVATE_MACRO && type < ObSSMacroBlockType::MAX_TYPE;
}

const char *get_ss_macro_block_type_str(const ObSSMacroBlockType type)
{
  static const char *SS_MACRO_BLOCK_TYPE_STRS[] = {
      "PRIVATE_MACRO",
      "SHARED_MACRO",
      "EXTERNAL_TABLE",
      "MAX_TYPE"
  };
  static_assert(
      sizeof(SS_MACRO_BLOCK_TYPE_STRS) == sizeof(char *) * (SS_MACRO_BLOCK_MAX_TYPE_VAL + 1),
      "element num mismatch");

  const char *type_str = "UNKNOWN";
  if (type >= ObSSMacroBlockType::PRIVATE_MACRO && type <= ObSSMacroBlockType::MAX_TYPE) {
    type_str = SS_MACRO_BLOCK_TYPE_STRS[static_cast<uint8_t>(type)];
  }
  return type_str;
}

/******************************** ObSSMacroCacheDesc **********************************/
ObSSMacroCacheDesc::ObSSMacroCacheDesc() : min_(0), max_(0), weight_(0)
{
}

ObSSMacroCacheDesc::ObSSMacroCacheDesc(const int64_t min, const int64_t max, const int64_t weight)
  : min_(min), max_(max), weight_(weight)
{
}

ObSSMacroCacheDesc::ObSSMacroCacheDesc(const ObSSMacroCacheDesc &other)
{
  *this = other;
}

ObSSMacroCacheDesc &ObSSMacroCacheDesc::operator=(const ObSSMacroCacheDesc &other)
{
  if (this != &other) {
    min_ = other.min_;
    max_ = other.max_;
    weight_ = other.weight_;
  }
  return *this;
}

ObSSMacroCacheDesc::~ObSSMacroCacheDesc()
{
}

void ObSSMacroCacheDesc::reset()
{
  min_ = 0;
  max_ = 0;
  weight_ = 0;
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheDesc, min_, max_, weight_);


/******************************** ObSSMacroCacheStat **********************************/
ObSSMacroCacheStat::ObSSMacroCacheStat()
  : cache_type_(ObSSMacroCacheType::MAX_TYPE), desc_(), used_(0)
{
}

ObSSMacroCacheStat::ObSSMacroCacheStat(const ObSSMacroCacheStat &other)
{
  *this = other;
}

ObSSMacroCacheStat &ObSSMacroCacheStat::operator=(const ObSSMacroCacheStat &other)
{
  if (this != &other) {
    cache_type_ = other.cache_type_;
    desc_ = other.desc_;
    used_ = other.used_;
  }
  return *this;
}

ObSSMacroCacheStat::~ObSSMacroCacheStat()
{
}

int64_t ObSSMacroCacheStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "{");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_SS_MACRO_CACHE_TYPE(cache_type_));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(desc));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "used:% '15ld", used_);
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

void ObSSMacroCacheStat::reset()
{
  cache_type_ = ObSSMacroCacheType::MAX_TYPE;
  desc_.reset();
  used_ = 0;
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheStat, cache_type_, desc_, used_);


/******************************** ObSSMacroCacheInfo **********************************/
ObSSMacroCacheInfo::ObSSMacroCacheInfo()
  : effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), size_(0),
    cache_type_(ObSSMacroCacheType::MAX_TYPE), is_write_cache_(true), is_marked_(false)
{
}

ObSSMacroCacheInfo::ObSSMacroCacheInfo(
    const uint64_t effective_tablet_id,
    const uint32_t size,
    const ObSSMacroCacheType cache_type,
    const bool is_write_cache)
  : effective_tablet_id_(effective_tablet_id), size_(size), cache_type_(cache_type),
    is_write_cache_(is_write_cache), is_marked_(false)
{
}

ObSSMacroCacheInfo::ObSSMacroCacheInfo(const ObSSMacroCacheInfo &other)
{
  *this = other;
}

ObSSMacroCacheInfo::~ObSSMacroCacheInfo()
{
}

ObSSMacroCacheInfo &ObSSMacroCacheInfo::operator=(const ObSSMacroCacheInfo &other)
{
  if (this != &other) {
    effective_tablet_id_ = other.effective_tablet_id_;
    size_ = other.size_;
    cache_type_ = other.cache_type_;
    is_write_cache_ = other.is_write_cache_;
    is_marked_ = other.is_marked_;
  }
  return *this;
}

bool ObSSMacroCacheInfo::is_valid() const
{
  bool b_ret = true;
  if (OB_UNLIKELY((ObSSMacroCacheType::MAX_TYPE == cache_type_) || (size_ <= 0))) {
    b_ret = false;
  }
  return b_ret;
}


/******************************** ObSSMacroCacheMeta **********************************/
ObSSMacroCacheMeta::ObSSMacroCacheMeta()
  : macro_id_(), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), size_(0), ref_cnt_(0),
    last_access_time_us_(0), lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_META_LOCK), cache_type_(ObSSMacroCacheType::MAX_TYPE), access_cnt_(0),
    is_write_cache_(false), is_in_fifo_list_(true), is_marked_(false)
{
}

ObSSMacroCacheMeta::ObSSMacroCacheMeta(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
  : ref_cnt_(0), lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_META_LOCK), access_cnt_(0), is_in_fifo_list_(true), is_marked_(false)
{
  macro_id_ = macro_id;
  effective_tablet_id_ = cache_info.effective_tablet_id_;
  size_ = cache_info.size_;
  last_access_time_us_ = ObTimeUtility::current_time_us();
  cache_type_ = cache_info.cache_type_;
  is_write_cache_ = cache_info.is_write_cache_;
}

ObSSMacroCacheMeta::~ObSSMacroCacheMeta()
{
  reset();
}

bool ObSSMacroCacheMeta::is_valid() const
{
  bool b_ret = true;
  SpinRLockGuard r_guard(lock_);
  if (OB_UNLIKELY(!macro_id_.is_valid() || (size_ <= 0) || (last_access_time_us_ <= 0) ||
                  (ObSSMacroCacheType::MAX_TYPE == cache_type_))) {
    b_ret = false;
  }
  return b_ret;
}

void ObSSMacroCacheMeta::reset()
{
  SpinWLockGuard w_guard(lock_);
  macro_id_.reset();
  effective_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  size_ = 0;
  ref_cnt_ = 0;
  last_access_time_us_ = 0;
  cache_type_ = ObSSMacroCacheType::MAX_TYPE;
  access_cnt_ = 0;
  is_write_cache_ = false;
  is_in_fifo_list_ = true;
  is_marked_ = false;
}

void ObSSMacroCacheMeta::inc_ref_count()
{
  SpinWLockGuard w_guard(lock_);
  ref_cnt_++;
}

void ObSSMacroCacheMeta::dec_ref_count()
{
  int32_t ref_cnt = -1;
  {
    SpinWLockGuard w_guard(lock_);
    ref_cnt_--;
    ref_cnt = ref_cnt_;
  }
  if (OB_UNLIKELY(ref_cnt < 0)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ref count of macro cache meta should not be less than 0",
                  KPC(this));
  } else if (0 == ref_cnt) {
    this->~ObSSMacroCacheMeta();
    MTL(ObSSMacroCacheMgr *)->get_macro_meta_allocator().free(this);
    MTL(ObSSMacroCacheMgr *)->update_macro_meta_alloc_cnt(-1);
  }
}

// just for virtual table display
int32_t ObSSMacroCacheMeta::get_ref_cnt() const
{
  SpinRLockGuard r_guard(lock_);
  return ref_cnt_;
}

void ObSSMacroCacheMeta::set_macro_id(const MacroBlockId &macro_id)
{
  SpinWLockGuard w_guard(lock_);
  macro_id_ = macro_id;
}
const blocksstable::MacroBlockId &ObSSMacroCacheMeta::get_macro_id() const
{
  SpinRLockGuard r_guard(lock_);
  return macro_id_;
}

void ObSSMacroCacheMeta::set_effective_tablet_id(const uint64_t effective_tablet_id)
{
  SpinWLockGuard w_guard(lock_);
  effective_tablet_id_ = effective_tablet_id;
}

uint64_t ObSSMacroCacheMeta::get_effective_tablet_id() const
{
  SpinRLockGuard r_guard(lock_);
  return effective_tablet_id_;
}

void ObSSMacroCacheMeta::set_size(const uint32_t size)
{
  SpinWLockGuard w_guard(lock_);
  size_ = size;
}

uint32_t ObSSMacroCacheMeta::get_size() const
{
  SpinRLockGuard r_guard(lock_);
  return size_;
}

void ObSSMacroCacheMeta::set_last_access_time_us(const int64_t last_access_time_us)
{
  SpinWLockGuard w_guard(lock_);
  last_access_time_us_ = last_access_time_us;
}

int64_t ObSSMacroCacheMeta::get_last_access_time_us() const
{
  SpinRLockGuard r_guard(lock_);
  return last_access_time_us_;
}

void ObSSMacroCacheMeta::set_is_marked(const bool is_marked)
{
  SpinWLockGuard w_guard(lock_);
  is_marked_ = is_marked;
}

bool ObSSMacroCacheMeta::get_is_marked() const
{
  SpinRLockGuard r_guard(lock_);
  return is_marked_;
}

ObSSMacroCacheType ObSSMacroCacheMeta::get_cache_type() const
{
  SpinRLockGuard r_guard(lock_);
  return cache_type_;
}

void ObSSMacroCacheMeta::set_cache_type(const ObSSMacroCacheType &cache_type)
{
  SpinWLockGuard w_guard(lock_);
  cache_type_ = cache_type;
}

bool ObSSMacroCacheMeta::get_is_in_fifo_list() const
{
  SpinRLockGuard r_guard(lock_);
  return is_in_fifo_list_;
}

void ObSSMacroCacheMeta::set_is_in_fifo_list(const bool is_in_fifo_list)
{
  SpinWLockGuard w_guard(lock_);
  is_in_fifo_list_ = is_in_fifo_list;
}

// increase access_cnt_ by one, and return whether need to update lru list
bool ObSSMacroCacheMeta::inc_and_check_access_cnt()
{
  bool b_ret = false;
  SpinWLockGuard w_guard(lock_);
  ++access_cnt_;
  if (access_cnt_ >= UPDATE_LRU_LIST_THRESHOLD) {
    b_ret = true;     // need to update lru list
    access_cnt_ = 0;  // reset access_cnt to zero
  }
  return b_ret;
}

// just for virtual table display
uint8_t ObSSMacroCacheMeta::get_access_cnt() const
{
  SpinRLockGuard r_guard(lock_);
  return access_cnt_;
}

bool ObSSMacroCacheMeta::get_is_write_cache() const
{
  SpinRLockGuard r_guard(lock_);
  return is_write_cache_;
}

void ObSSMacroCacheMeta::set_is_write_cache(const bool is_write_cache)
{
  SpinWLockGuard w_guard(lock_);
  is_write_cache_ = is_write_cache;
}

bool ObSSMacroCacheMeta::is_ckpt_marker() const
{
  SpinRLockGuard r_guard(lock_);
  return ObStorageObjectType::MACRO_CACHE_CKPT_META == macro_id_.storage_object_type();
}

int ObSSMacroCacheMeta::construct_ckpt_marker(ObSSMacroCacheMeta &marker)
{
  int ret = OB_SUCCESS;
  marker.reset();
  marker.unlink();
  MacroBlockId macro_id;
  ObStorageObjectOpt opt;
  opt.set_ss_macro_cache_ckpt_opt(
      true/*is_meta*/, UINT64_MAX - 1 /*version_id*/, UINT64_MAX - 1 /*seq_id*/);
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, macro_id))) {
    LOG_WARN("fail to get object id", KR(ret), K(opt));
  } else {
    marker.set_macro_id(macro_id);
    marker.set_size(1);
    marker.set_last_access_time_us(1);
    marker.set_cache_type(ObSSMacroCacheType::META_FILE);

    if (OB_UNLIKELY(!marker.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to construct ckpt marker", KR(ret), K(opt), K(marker));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheMeta, macro_id_, effective_tablet_id_, size_, last_access_time_us_,
                    cache_type_, is_write_cache_, is_in_fifo_list_);


/******************************** ObSSMacroCacheLRUList **********************************/
ObSSMacroCacheLRUList::ObSSMacroCacheLRUList(): lru_list_(), fifo_list_(), lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_LRU_LIST_LOCK)
{
}

ObSSMacroCacheLRUList::~ObSSMacroCacheLRUList()
{
}

int64_t ObSSMacroCacheLRUList::get_size() const
{
  return (lru_list_.get_size() + fifo_list_.get_size());
}


/*-----------------------------------------ObSSCacheSpaceStat-----------------------------------------*/
void ObSSCacheSpaceStat::reset()
{
  block_count_ = 0;
  required_size_ = 0;
}

bool ObSSCacheSpaceStat::is_valid() const
{
  return block_count_ >= 0 && required_size_ >= 0;
}

void ObSSCacheSpaceStat::add(const int64_t block_count, const int64_t required_size)
{
  ATOMIC_AAF(&block_count_, block_count);
  ATOMIC_AAF(&required_size_, required_size);
}

void ObSSCacheSpaceStat::sub(const int64_t block_count, const int64_t required_size)
{
  add(-block_count, -required_size);
}

void ObSSCacheSpaceStat::merge(const ObSSCacheSpaceStat &other)
{
  add(other.block_count_, other.required_size_);
}

OB_SERIALIZE_MEMBER(ObSSCacheSpaceStat, block_count_, required_size_);


/*-----------------------------------------ObSSMacroCacheQueueSpaceStat-----------------------------------------*/
void ObSSMacroCacheQueueSpaceStat::reset()
{
  fifo_space_.reset();
  lru_space_.reset();
  hot_fifo_space_.reset();
  hot_lru_space_.reset();
}

bool ObSSMacroCacheQueueSpaceStat::is_valid() const
{
  return fifo_space_.is_valid() && lru_space_.is_valid()
      && hot_fifo_space_.is_valid() && hot_lru_space_.is_valid();
}

void ObSSMacroCacheQueueSpaceStat::add(
    const bool is_hot,
    const bool is_in_fifo,
    const int64_t block_count,
    const int64_t required_size)
{
  ObSSCacheSpaceStat &space = (is_in_fifo ? fifo_space_ : lru_space_);
  space.add(block_count, required_size);
  if (is_hot) {
    ObSSCacheSpaceStat &hot_space = (is_in_fifo ? hot_fifo_space_ : hot_lru_space_);
    hot_space.add(block_count, required_size);
  }
}

void ObSSMacroCacheQueueSpaceStat::merge(const ObSSMacroCacheQueueSpaceStat &other)
{
  fifo_space_.merge(other.fifo_space_);
  lru_space_.merge(other.lru_space_);
  hot_fifo_space_.merge(other.hot_fifo_space_);
  hot_lru_space_.merge(other.hot_lru_space_);
}

int64_t ObSSMacroCacheQueueSpaceStat::get_valid_block_count() const
{
  return fifo_space_.get_block_count() + lru_space_.get_block_count();
}

int64_t ObSSMacroCacheQueueSpaceStat::get_valid_required_size() const
{
  return fifo_space_.get_required_size() + lru_space_.get_required_size();
}

int64_t ObSSMacroCacheQueueSpaceStat::get_hot_block_count() const
{
  return hot_fifo_space_.get_block_count() + hot_lru_space_.get_block_count();
}

int64_t ObSSMacroCacheQueueSpaceStat::get_hot_required_size() const
{
  return hot_fifo_space_.get_required_size() + hot_lru_space_.get_required_size();
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheQueueSpaceStat, fifo_space_, lru_space_, hot_fifo_space_, hot_lru_space_);


/*-----------------------------------------ObSSMacroCacheLSInfo-----------------------------------------*/
ObSSMacroCacheLSInfo& ObSSMacroCacheLSInfo::operator=(const ObSSMacroCacheLSInfo &other)
{
  if (this != &other) {
    ls_id_ = other.ls_id_;
    tablet_cnt_ = other.tablet_cnt_;
    queue_space_ = other.queue_space_;
  }
  return *this;
}

int64_t ObSSMacroCacheLSInfo::get_hot_size() const
{
  int64_t hot_size = get_hot_required_size();
  const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (min_cluster_version < CLUSTER_VERSION_4_5_1_0) {
    // During cluster upgrade, hot space deserialized from old nodes defaults to 0.
    // Use total size as a fallback to preserve hot data as much as possible.
    hot_size = get_valid_size();
  }
  return hot_size;
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheLSInfo, ls_id_, tablet_cnt_, queue_space_);


/*-----------------------------------------ObSSMacroPrewarmMeta-----------------------------------------*/
ObSSMacroPrewarmMeta::ObSSMacroPrewarmMeta()
  : macro_id_(), effective_tablet_id_(ObTabletID::INVALID_TABLET_ID), size_(0),
    is_in_fifo_list_(false), cache_type_(ObSSMacroCacheType::MAX_TYPE)
{
}

ObSSMacroPrewarmMeta::~ObSSMacroPrewarmMeta()
{
}

int ObSSMacroPrewarmMeta::assign(const ObSSMacroPrewarmMeta &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    macro_id_ = other.macro_id_;
    effective_tablet_id_ = other.effective_tablet_id_;
    size_ = other.size_;
    is_in_fifo_list_ = other.is_in_fifo_list_;
    cache_type_ = other.cache_type_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSMacroPrewarmMeta, macro_id_, effective_tablet_id_, size_,
                    is_in_fifo_list_, cache_type_);


/*--------------------------------ObSSMacroCacheBucketIdxRange-------------------------------*/
ObSSMacroCacheBucketIdxRange::ObSSMacroCacheBucketIdxRange()
  : start_bkt_idx_(-1), end_bkt_idx_(-1)
{
}

ObSSMacroCacheBucketIdxRange::ObSSMacroCacheBucketIdxRange(
    const int64_t start_bkt_idx,
    const int64_t end_bkt_idx)
  : start_bkt_idx_(start_bkt_idx), end_bkt_idx_(end_bkt_idx)
{
}

bool ObSSMacroCacheBucketIdxRange::is_valid() const
{
  // left-open and right-closed interval, e.g., (-1, 100] represents 0~100 buckets.
  return (start_bkt_idx_ < end_bkt_idx_) && (start_bkt_idx_ >= -1);
}

void ObSSMacroCacheBucketIdxRange::reset()
{
  start_bkt_idx_ = -1;
  end_bkt_idx_ = -1;
}

OB_SERIALIZE_MEMBER(ObSSMacroCacheBucketIdxRange, start_bkt_idx_, end_bkt_idx_);


} // namespace storage
} // namespace oceanbase
