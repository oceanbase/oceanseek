/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * WARNING: Forbid add new macro cache in the middle!!! Only allow add to the tail for compatibility.
 * The format is DEF_SS_MACRO_CACHE(type, min, max, weight, type_str, class_name).
 * Meanwhile, please implement one new macro cache class that extends ObSSMacroCache, whose name
 * should be the same as 'class_name' defined in DEF_SS_MACRO_CACHE.
 */
#ifdef DEF_SS_MACRO_CACHE
DEF_SS_MACRO_CACHE(META_FILE, 10, 100, 5, "META_FILE", ObSSMetaFileCache)
DEF_SS_MACRO_CACHE(TMP_FILE, 2, 100, 10, "TMP_FILE", ObSSTmpFileCache)
DEF_SS_MACRO_CACHE(MACRO_BLOCK, 8, 100, 35, "MACRO_BLOCK", ObSSMacroBlockCache)
DEF_SS_MACRO_CACHE(HOT_TABLET_MACRO_BLOCK, 80, 100, 50, "HOT_TABLET_MACRO_BLOCK", ObSSHotTabletMacroBlockCache)
DEF_SS_MACRO_CACHE(MAX_TYPE, 0, 0, 0, "MAX_TYPE", ObSSMaxTypeCache)
#endif // DEF_SS_MACRO_CACHE

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_COMMON_META_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_COMMON_META_H_

#include <stdint.h>
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "share/ob_define.h"
#include "share/ob_ptr_handle.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSMacroCacheType : uint8_t // FARM COMPAT WHITELIST
{
#define DEF_SS_MACRO_CACHE(type, min, max, weight, type_str, class_name) type,
#include "ob_ss_macro_cache_common_meta.h"
#undef DEF_SS_MACRO_CACHE
};

static constexpr uint8_t SS_MACRO_CACHE_MAX_TYPE_VAL =
    static_cast<uint8_t>(ObSSMacroCacheType::MAX_TYPE);

static const char *get_ss_macro_cache_type_str(const ObSSMacroCacheType type)
{
  static const char *type_str_map_[SS_MACRO_CACHE_MAX_TYPE_VAL + 1] = {
#define DEF_SS_MACRO_CACHE(type, min, max, weight, type_str, class_name) type_str,
#include "ob_ss_macro_cache_common_meta.h"
#undef DEF_SS_MACRO_CACHE
  };
  const char *type_str = type_str_map_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  if (OB_LIKELY(type >= ObSSMacroCacheType::META_FILE && type <= ObSSMacroCacheType::MAX_TYPE)) {
    type_str = type_str_map_[static_cast<uint8_t>(type)];
  } else {
    type_str = "UNKNOWN";
  }
  return type_str;
}

bool is_valid_ss_macro_cache_type(const ObSSMacroCacheType type);

#define K_SS_MACRO_CACHE_TYPE(cache_type) K(cache_type),"cache_type_str",get_ss_macro_cache_type_str(cache_type)

enum class ObSSMacroBlockType : uint8_t
{
  PRIVATE_MACRO    = 0,
  SHARED_MACRO     = 1,
  EXTERNAL_TABLE   = 2,
  EXTERNAL_LOB     = 3,
  MAX_TYPE
};

static constexpr uint8_t SS_MACRO_BLOCK_MAX_TYPE_VAL =
    static_cast<uint8_t>(ObSSMacroBlockType::MAX_TYPE);

bool is_valid_ss_macro_block_type(const ObSSMacroBlockType type);

const char *get_ss_macro_block_type_str(const ObSSMacroBlockType type);

class ObSSMacroCacheDesc final
{
public:
  static const int32_t SS_MACRO_CACHE_DESC_VERSION = 1;
  OB_UNIS_VERSION(SS_MACRO_CACHE_DESC_VERSION);

public:
  ObSSMacroCacheDesc();
  ObSSMacroCacheDesc(const int64_t min, const int64_t max, const int64_t weight);
  ObSSMacroCacheDesc(const ObSSMacroCacheDesc &other);
  ObSSMacroCacheDesc &operator=(const ObSSMacroCacheDesc &other);
  ~ObSSMacroCacheDesc();
  TO_STRING_KV(K_(min), K_(max), K_(weight));

  void reset();

public:
  int64_t min_;
  int64_t max_;
  int64_t weight_;
};

extern const ObSSMacroCacheDesc OB_SS_MACRO_CACHE_DESCS[];


class ObSSMacroCacheStat final
{
public:
  static const int32_t SS_MACRO_CACHE_STAT_VERSION = 1;
  OB_UNIS_VERSION(SS_MACRO_CACHE_STAT_VERSION);

public:
  ObSSMacroCacheStat();
  ObSSMacroCacheStat(const ObSSMacroCacheStat &other);
  ObSSMacroCacheStat &operator=(const ObSSMacroCacheStat &other);
  ~ObSSMacroCacheStat();
  OB_INLINE int64_t get_min() const { return desc_.min_; }
  OB_INLINE int64_t get_max() const { return desc_.max_; }
  OB_INLINE int64_t get_weight() const { return desc_.weight_; }
  OB_INLINE bool is_valid() const { return (used_ >= 0); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  void reset();

public:
  ObSSMacroCacheType cache_type_;
  ObSSMacroCacheDesc desc_;
  int64_t used_;
};

struct ObFileStatInfo
{
public:
  ObFileStatInfo() : dir_num_(0), dir_size_(0), total_size_(0) {}
  virtual ~ObFileStatInfo() {}
  TO_STRING_KV(K_(dir_num), "dir_size(B)", dir_size_, "total_size(MB)", total_size_/1024L/1024L);

public:
  int64_t dir_num_;
  int64_t dir_size_;
  int64_t total_size_;  // dir_size + file_size
};

class ObSSMacroCacheInfo final
{
public:
  ObSSMacroCacheInfo();
  ObSSMacroCacheInfo(const uint64_t effective_tablet_id,
                     const uint32_t size,
                     const ObSSMacroCacheType cache_type,
                     const bool is_write_cache);
  ObSSMacroCacheInfo(const ObSSMacroCacheInfo &other);
  ~ObSSMacroCacheInfo();
  ObSSMacroCacheInfo &operator=(const ObSSMacroCacheInfo &other);
  bool is_valid() const;
  TO_STRING_KV(K_(effective_tablet_id), K_(size), K_SS_MACRO_CACHE_TYPE(cache_type_),
               K_(is_write_cache), K_(is_marked));

public:
  uint64_t effective_tablet_id_;
  uint32_t size_;
  ObSSMacroCacheType cache_type_;
  bool is_write_cache_;
  bool is_marked_;
};


class ObSSMacroCacheMeta final : public ObDLinkBase<ObSSMacroCacheMeta>
{
public:
  ObSSMacroCacheMeta();
  ObSSMacroCacheMeta(const blocksstable::MacroBlockId &macro_id,
                     const ObSSMacroCacheInfo &cache_info);
  ~ObSSMacroCacheMeta();
  bool is_valid() const;
  void reset();
  void inc_ref_count();
  void dec_ref_count();
  int32_t get_ref_cnt() const; // just for virtual table display
  void set_macro_id(const blocksstable::MacroBlockId &macro_id);
  const blocksstable::MacroBlockId &get_macro_id() const;
  void set_effective_tablet_id(const uint64_t effective_tablet_id);
  uint64_t get_effective_tablet_id() const;
  void set_size(const uint32_t size);
  uint32_t get_size() const;
  void set_last_access_time_us(const int64_t last_access_time_us);
  int64_t get_last_access_time_us() const;
  void set_is_marked(const bool is_marked);
  bool get_is_marked() const;
  ObSSMacroCacheType get_cache_type() const;
  void set_cache_type(const ObSSMacroCacheType &cache_type);
  bool inc_and_check_access_cnt();
  uint8_t get_access_cnt() const; // just for virtual table display
  bool get_is_write_cache() const;
  void set_is_write_cache(const bool is_write_cache);
  bool get_is_in_fifo_list() const;
  void set_is_in_fifo_list(const bool is_in_fifo_list);

  bool is_ckpt_marker() const;
  static int construct_ckpt_marker(ObSSMacroCacheMeta &marker);

  TO_STRING_KV(K_(macro_id), K_(effective_tablet_id), K_(size), K_(last_access_time_us),
               K_SS_MACRO_CACHE_TYPE(cache_type_), K_(access_cnt), K_(is_write_cache),
               K_(is_in_fifo_list), K_(is_marked), KP(prev_), KP(next_));

public:
  static const int64_t OB_SS_MACRO_CACHE_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_CACHE_META_VERSION);

private:
  // update lru list every time @access_cnt_ reaches this threshold
  static const int64_t UPDATE_LRU_LIST_THRESHOLD = 5;
  blocksstable::MacroBlockId macro_id_;
  uint64_t effective_tablet_id_;
  uint32_t size_;
  int32_t ref_cnt_; // do not serialize
  uint64_t last_access_time_us_;
  // to ensure visibility. e.g., flush thread modify @is_write_cache_ from true to false, it
  // should be visible to evict thread.
  common::SpinRWLock lock_; // do not serialize
  ObSSMacroCacheType cache_type_;
  // increase @access_cnt_ by one when access this cache. update LRU only when
  // @access_cnt_ reaches UPDATE_LRU_LIST_THRESHOLD, and then reset @access_cnt_ to 0.
  uint8_t access_cnt_; // do not serialize
  bool is_write_cache_;
  bool is_in_fifo_list_;
  bool is_marked_; // do not serialize
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheMeta);
};

typedef ObPtrHandle<ObSSMacroCacheMeta> ObSSMacroCacheMetaHandle;


class ObSSMacroCacheLRUList final
{
public:
  using DListType = common::ObDList<ObSSMacroCacheMeta>;

public:
  ObSSMacroCacheLRUList();
  ~ObSSMacroCacheLRUList();
  int64_t get_size() const;

public:
  DListType lru_list_;
  DListType fifo_list_;
  lib::ObMutex lock_;
};


struct ObSSCacheSpaceStat final
{
public:
  static const int64_t OB_SS_CACHE_SPACE_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_CACHE_SPACE_STAT_VERSION);

public:
  ObSSCacheSpaceStat() { reset(); }
  ~ObSSCacheSpaceStat() {}
  void reset();
  bool is_valid() const;
  void add(const int64_t block_count, const int64_t required_size);
  void sub(const int64_t block_count, const int64_t required_size);
  void merge(const ObSSCacheSpaceStat &other);
  int64_t get_block_count() const { return block_count_; }
  int64_t get_required_size() const { return required_size_; }
  TO_STRING_KV(K(block_count_), K(required_size_));

private:
  int64_t block_count_;
  int64_t required_size_;
};

struct ObSSMacroCacheQueueSpaceStat final
{
public:
  static const int64_t OB_SS_MACRO_CACHE_QUEUE_SPACE_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_CACHE_QUEUE_SPACE_STAT_VERSION);

public:
  ObSSMacroCacheQueueSpaceStat() { reset(); }
  ~ObSSMacroCacheQueueSpaceStat() {}
  void reset();
  bool is_valid() const;
  void add(
      const bool is_hot,
      const bool is_in_fifo,
      const int64_t block_count,
      const int64_t required_size);
  void merge(const ObSSMacroCacheQueueSpaceStat &other);
  int64_t get_valid_block_count() const;
  int64_t get_valid_required_size() const;
  int64_t get_hot_block_count() const;
  int64_t get_hot_required_size() const;

  int64_t get_fifo_block_count() const { return fifo_space_.get_block_count(); }
  int64_t get_fifo_required_size() const { return fifo_space_.get_required_size(); }
  int64_t get_lru_block_count() const { return lru_space_.get_block_count(); }
  int64_t get_lru_required_size() const { return lru_space_.get_required_size(); }
  int64_t get_hot_fifo_block_count() const { return hot_fifo_space_.get_block_count(); }
  int64_t get_hot_fifo_required_size() const { return hot_fifo_space_.get_required_size(); }
  int64_t get_hot_lru_block_count() const { return hot_lru_space_.get_block_count(); }
  int64_t get_hot_lru_required_size() const { return hot_lru_space_.get_required_size(); }

  TO_STRING_KV(K(fifo_space_), K(lru_space_), K(hot_fifo_space_), K(hot_lru_space_));

private:
  ObSSCacheSpaceStat fifo_space_;
  ObSSCacheSpaceStat lru_space_;
  ObSSCacheSpaceStat hot_fifo_space_; // fifo list space of HOT_TABLET_MACRO_BLOCK
  ObSSCacheSpaceStat hot_lru_space_;  // lru list space of HOT_TABLET_MACRO_BLOCK
};

struct ObSSMacroCacheLSInfo final
{
public:
  ObSSMacroCacheLSInfo() { reset(); }
  ~ObSSMacroCacheLSInfo() { reset(); }
  void reset()
  {
    queue_space_.reset();
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    tablet_cnt_ = 0;
  }
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_cnt_ >= 0 && queue_space_.is_valid();
  }
  int64_t get_valid_size() const { return get_valid_required_size(); }
  int64_t get_hot_size() const;
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  int64_t get_tablet_cnt() const { return tablet_cnt_; }
  void set_tablet_cnt(const int64_t tablet_cnt) { tablet_cnt_ = tablet_cnt; }
  void add_tablet_cnt(const int64_t delta_tablet_cnt) { tablet_cnt_ += delta_tablet_cnt; }
  void add_queue_space(
      const bool is_hot,
      const bool is_in_fifo,
      const int64_t block_count,
      const int64_t required_size)
  {
    queue_space_.add(is_hot, is_in_fifo, block_count, required_size);
  }
  void set_queue_space(const ObSSMacroCacheQueueSpaceStat &queue_space) { queue_space_ = queue_space; }
  void merge_queue_space(const ObSSMacroCacheQueueSpaceStat &queue_space) { queue_space_.merge(queue_space); }
  const ObSSMacroCacheQueueSpaceStat &get_queue_space() const { return queue_space_; }
  
  int64_t get_fifo_required_size() const { return queue_space_.get_fifo_required_size(); }
  int64_t get_lru_required_size() const { return queue_space_.get_lru_required_size(); }
  int64_t get_hot_fifo_required_size() const { return queue_space_.get_hot_fifo_required_size(); }
  int64_t get_hot_lru_required_size() const { return queue_space_.get_hot_lru_required_size(); }
  int64_t get_valid_block_count() const { return queue_space_.get_valid_block_count(); }
  int64_t get_valid_required_size() const { return queue_space_.get_valid_required_size(); }
  int64_t get_hot_block_count() const { return queue_space_.get_hot_block_count(); }
  int64_t get_hot_required_size() const { return queue_space_.get_hot_required_size(); }
  
  ObSSMacroCacheLSInfo &operator=(const ObSSMacroCacheLSInfo &other);
  TO_STRING_KV(K(ls_id_), K(tablet_cnt_), K(queue_space_));

public:
  static const int64_t OB_SS_MACRO_CACHE_LS_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_CACHE_LS_INFO_VERSION);

private:
  share::ObLSID ls_id_;
  int64_t tablet_cnt_;
  ObSSMacroCacheQueueSpaceStat queue_space_;
};


class ObSSMacroPrewarmMeta final
{
public:
  ObSSMacroPrewarmMeta();
  ~ObSSMacroPrewarmMeta();
  int assign(const ObSSMacroPrewarmMeta &other);
  // Note: cache_type_ is not checked here, because it may be invalid during cluster upgrade.
  // e.g., v4.5.1(with cache_type_ field) get meta from v4.4.1(without cache_type_ field).
  bool is_valid() const { return macro_id_.is_valid() && (size_ > 0); }
  TO_STRING_KV(K_(macro_id), K_(effective_tablet_id), K_(size), K_(is_in_fifo_list),
               K_SS_MACRO_CACHE_TYPE(cache_type_));

public:
  static const int64_t OB_SS_MACRO_PREWARM_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_PREWARM_META_VERSION);

public:
  blocksstable::MacroBlockId macro_id_;
  uint64_t effective_tablet_id_;
  uint32_t size_;
  bool is_in_fifo_list_;
  ObSSMacroCacheType cache_type_;
};


struct ObSSMacroCacheBucketIdxRange
{
public:
  ObSSMacroCacheBucketIdxRange();
  ObSSMacroCacheBucketIdxRange(const int64_t start_bkt_idx, const int64_t end_bkt_idx);
  virtual ~ObSSMacroCacheBucketIdxRange() {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(start_bkt_idx), K_(end_bkt_idx));

public:
  static const int64_t OB_SS_MACRO_CACHE_BUCKET_IDX_RANGE_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_CACHE_BUCKET_IDX_RANGE_VERSION);

public:
  int64_t start_bkt_idx_;
  int64_t end_bkt_idx_;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_COMMON_META_H_ */
