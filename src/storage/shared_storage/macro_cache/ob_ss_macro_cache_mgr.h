/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_MGR_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_MGR_H_

#include "lib/allocator/ob_small_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_fd_cache_struct.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_flush_map.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_flush_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_evict_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_expire_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_hot_tablet_macro_cache_gc_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_hot_tablet_old_sstable_evict_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_write_cache_ctrl_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_ckpt_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_replay_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_tablet_stat_task.h"
#include "storage/shared_storage/ob_disk_space_meta.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_lock.h"
#include "storage/shared_storage/ob_ss_local_cache_stat.h"

namespace oceanbase
{
namespace storage
{
class ObSSMacroCache;
class ObSSMacroCacheMgr;
class ObSSMacroCacheEvictEntry;
enum class ObDiskSpaceType : uint8_t;

using SSMacroCacheMetaMap = common::hash::ObHashMap<blocksstable::MacroBlockId, ObSSMacroCacheMetaHandle>;
using SSMacroCacheMetaMapPairType = common::hash::HashMapPair<blocksstable::MacroBlockId, ObSSMacroCacheMetaHandle>;

int get_active_tablet_ids(common::hash::ObHashSet<uint64_t> &active_tablet_ids, const ObMemAttr &attr);
class SSMacroCacheReqGuard final
{
public:
  SSMacroCacheReqGuard(int64_t &flying_req_cnt) : flying_req_cnt_(flying_req_cnt)
  {
    ATOMIC_INC(&flying_req_cnt_);
  }

  ~SSMacroCacheReqGuard()
  {
    ATOMIC_DEC(&flying_req_cnt_);
  }

private:
  int64_t &flying_req_cnt_;
};


class ObSSMacroCacheBaseCallback
{
public:
  explicit ObSSMacroCacheBaseCallback(ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheBaseCallback();

protected:
  ObSSMacroCacheMgr &macro_cache_mgr_;
};


class ObSSMacroCacheInsertCallback : public ObSSMacroCacheBaseCallback
{
public:
  explicit ObSSMacroCacheInsertCallback(ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheInsertCallback();
  int operator()(const SSMacroCacheMetaMapPairType &entry);
  TO_STRING_KV(KP(this));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheInsertCallback);
};


class ObSSMacroCacheGetFdCallback : public ObSSMacroCacheBaseCallback
{
public:
  ObSSMacroCacheGetFdCallback(const common::ObTabletID &effective_tablet_id,
                              ObSSMacroCacheMgr &macro_cache_mgr,
                              ObSSFdCacheHandle &fd_handle);
  virtual ~ObSSMacroCacheGetFdCallback();
  int operator()(const SSMacroCacheMetaMapPairType &entry);
  OB_INLINE int get_ret() const { return ret_; }
  OB_INLINE bool is_hit_cache() const { return is_hit_cache_; }
  OB_INLINE ObSSMacroCacheType get_cache_type() const { return cache_type_; }
  OB_INLINE int64_t get_macro_block_size() const { return macro_block_size_; }
  TO_STRING_KV(K_(ret), K_(effective_tablet_id), K_(is_hit_cache), K_(fd_handle));

private:
  int update_lru_list(const SSMacroCacheMetaMapPairType &entry);
  int update_last_access_time(const SSMacroCacheMetaMapPairType &entry);
  int try_update_effective_tablet_id(const SSMacroCacheMetaMapPairType &entry);

private:
  int ret_;
  common::ObTabletID effective_tablet_id_;
  bool is_hit_cache_;
  ObSSFdCacheHandle &fd_handle_;
  ObSSMacroCacheType cache_type_;
  int64_t macro_block_size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheGetFdCallback);
};

class ObSSMacroCacheGetCacheTypeCallback : public ObSSMacroCacheBaseCallback
{
public:
  explicit ObSSMacroCacheGetCacheTypeCallback(ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheGetCacheTypeCallback();
  int operator()(const SSMacroCacheMetaMapPairType &entry);
  ObSSMacroCacheType get_cache_type() const { return cache_type_; }
  int get_ret() const { return ret_; }
  TO_STRING_KV(K_(cache_type), K_(ret));

private:
  ObSSMacroCacheType cache_type_;
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheGetCacheTypeCallback);
};


class ObSSMacroCacheDeletePred : public ObSSMacroCacheBaseCallback
{
public:
  explicit ObSSMacroCacheDeletePred(ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheDeletePred();
  bool operator()(const SSMacroCacheMetaMapPairType &entry);
  bool get_is_write_cache() const { return is_write_cache_; }
  int64_t get_size() const { return size_; }
  int get_ret() const { return ret_; }
  TO_STRING_KV(K_(is_write_cache), K_(size), K_(ret));

private:
  // in order to calc and control write_cache_size_
  bool is_write_cache_;
  int64_t size_;
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheDeletePred);
};


class ObSSMacroCacheUpdateParam
{
public:
  ObSSMacroCacheUpdateParam(const bool is_update_lru,
                            const bool is_update_last_access_time,
                            const bool is_update_effective_tablet_id,
                            const bool is_update_size,
                            const bool is_update_marked,
                            const bool is_update_write_cache);
  ObSSMacroCacheUpdateParam(const ObSSMacroCacheUpdateParam &other);
  virtual ~ObSSMacroCacheUpdateParam();
  ObSSMacroCacheUpdateParam &operator=(const ObSSMacroCacheUpdateParam &other);
  TO_STRING_KV(K_(is_update_lru), K_(is_update_last_access_time), K_(is_update_effective_tablet_id),
               K_(is_update_size), K_(is_update_marked), K_(is_update_write_cache));

public:
  bool is_update_lru_;
  bool is_update_last_access_time_;
  bool is_update_effective_tablet_id_;
  bool is_update_size_;
  bool is_update_marked_;
  bool is_update_write_cache_;
};


class ObSSMacroCacheUpdateCallback : public ObSSMacroCacheBaseCallback
{
public:
  ObSSMacroCacheUpdateCallback(const ObSSMacroCacheInfo &cache_info,
                               const ObSSMacroCacheUpdateParam &update_param,
                               ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheUpdateCallback();
  void operator()(const SSMacroCacheMetaMapPairType &entry);
  int64_t get_ori_size() const { return ori_size_; }
  bool is_size_udpated() const { return is_size_updated_; }
  OB_INLINE int get_ret() { return ret_; }
  TO_STRING_KV(K_(cache_info), K_(update_param), K_(ret));

private:
  ObSSMacroCacheInfo cache_info_;
  ObSSMacroCacheUpdateParam update_param_;
  int64_t ori_size_;     // in order to calc and control write_cache_size_
  bool is_size_updated_; // in order to calc and control write_cache_size_
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheUpdateCallback);
};

class ObSSMacroCacheModifyCacheTypeParam
{
public:
  ObSSMacroCacheModifyCacheTypeParam(
      const bool touch_cache,
      const bool update_effective_tablet_id,
      const uint64_t effective_tablet_id)
    : touch_cache_(touch_cache),
      update_effective_tablet_id_(update_effective_tablet_id),
      effective_tablet_id_(effective_tablet_id)
  {}
  bool is_valid() const
  {
    return update_effective_tablet_id_
        ? common::ObTabletID(effective_tablet_id_).is_valid()
        : common::ObTabletID::INVALID_TABLET_ID == effective_tablet_id_;
  }
  TO_STRING_KV(K(touch_cache_), K(update_effective_tablet_id_), K(effective_tablet_id_));

public:
  bool touch_cache_;
  bool update_effective_tablet_id_;
  uint64_t effective_tablet_id_;
};


class ObSSMacroCacheModifyCacheTypeCallback : public ObSSMacroCacheBaseCallback
{
public:
  ObSSMacroCacheModifyCacheTypeCallback(const ObSSMacroCacheType ori_cache_type,
                                        const ObSSMacroCacheType new_cache_type,
                                        const ObSSMacroCacheModifyCacheTypeParam &modify_param,
                                        ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheModifyCacheTypeCallback();
  void operator()(const SSMacroCacheMetaMapPairType &entry);
  OB_INLINE int get_ret() { return ret_; }
  TO_STRING_KV(K_SS_MACRO_CACHE_TYPE(ori_cache_type_), K_SS_MACRO_CACHE_TYPE(new_cache_type_),
      K(modify_param_), KR(ret_));

private:
  ObSSMacroCacheType ori_cache_type_;
  ObSSMacroCacheType new_cache_type_;
  ObSSMacroCacheModifyCacheTypeParam modify_param_;
  int ret_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheModifyCacheTypeCallback);
};


class ObSSMacroCacheClearMarkedFn
{
public:
  explicit ObSSMacroCacheClearMarkedFn(volatile bool &is_stopped) : is_stopped_(is_stopped) {}
  virtual ~ObSSMacroCacheClearMarkedFn() {}
  int operator()(SSMacroCacheMetaMapPairType &entry);

private:
  volatile bool &is_stopped_;
};

class ObSSMacroCacheBatchModifyCacheTypeFn
{
public:
  ObSSMacroCacheBatchModifyCacheTypeFn(ObSSMacroCacheMgr &macro_cache_mgr,
                                       const ObSSMacroCacheType ori_cache_type,
                                       const ObSSMacroCacheType new_cache_type)
    : macro_cache_mgr_(macro_cache_mgr),
      ori_cache_type_(ori_cache_type),
      new_cache_type_(new_cache_type) {}
  virtual ~ObSSMacroCacheBatchModifyCacheTypeFn() {}
  int operator()(const common::hash::HashSetTypes<blocksstable::MacroBlockId>::pair_type &entry);

private:
  ObSSMacroCacheMgr &macro_cache_mgr_;
  ObSSMacroCacheType ori_cache_type_;
  ObSSMacroCacheType new_cache_type_;
};

class ObSSMacroCacheRestoreInsertCallback : public ObSSMacroCacheBaseCallback
{
public:
  ObSSMacroCacheRestoreInsertCallback(ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheRestoreInsertCallback();
  int operator()(const SSMacroCacheMetaMapPairType &entry);
  
  TO_STRING_KV(KP(this), KP(&macro_cache_mgr_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheRestoreInsertCallback);
};

class ObSSMacroCacheRestoreUpdateCallback : public ObSSMacroCacheBaseCallback
{
public:
  ObSSMacroCacheRestoreUpdateCallback(
      ObSSMacroCacheMgr &macro_cache_mgr, const ObSSMacroCacheMetaHandle &new_meta_handle);
  virtual ~ObSSMacroCacheRestoreUpdateCallback();
  int operator()(SSMacroCacheMetaMapPairType &entry);
  int get_ret() const { return ret_; }
  int64_t get_ori_size() const { return ori_size_; }
  bool is_ori_write_cache() const { return is_ori_write_cache_; }
  bool is_executed() const { return is_executed_; }
  TO_STRING_KV(KP(this), KP(&macro_cache_mgr_), KPC(new_meta_handle_()), K_(ret), K_(ori_size),
               K_(is_ori_write_cache), K_(is_executed));

private:
  ObSSMacroCacheMetaHandle new_meta_handle_;
  int ret_;
  int64_t ori_size_;        // in order to calc and control write_cache_size_
  bool is_ori_write_cache_; // in order to calc and control write_cache_size_
  bool is_executed_;        // in order to calc and control write_cache_size_
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheRestoreUpdateCallback);
};

class ObSSMacroCacheTabletStat : public ObStorageCacheHitStat
{
public:
  ObSSMacroCacheTabletStat();
  virtual ~ObSSMacroCacheTabletStat();
  void reset();
  void update_cached_space(
      const bool is_hot,
      const bool is_in_fifo,
      const int64_t block_count,
      const int64_t required_size);
  void set_cached_space(const ObSSMacroCacheQueueSpaceStat &queue_space) { queue_space_ = queue_space; }
  void merge_cached_space(const ObSSMacroCacheQueueSpaceStat &queue_space);
  const ObSSMacroCacheQueueSpaceStat &get_queue_space() const { return queue_space_; }
  int64_t get_total_cached_size() const { return queue_space_.get_valid_required_size(); }
  int64_t get_total_block_count() const { return queue_space_.get_valid_block_count(); }

  INHERIT_TO_STRING_KV("ObStorageCacheHitStat", ObStorageCacheHitStat, K(queue_space_));

private:
  ObSSMacroCacheQueueSpaceStat queue_space_;
};

struct ObSSMacroCacheClearFlag
{
public:
  ObSSMacroCacheClearFlag() : is_clear_macro_cache_(false), is_clear_hot_macro_cache_(false) {}
  virtual ~ObSSMacroCacheClearFlag() {}
  void reset() { is_clear_macro_cache_ = false; is_clear_hot_macro_cache_ = false; }
  TO_STRING_KV(K_(is_clear_macro_cache), K_(is_clear_hot_macro_cache));

public:
  bool is_clear_macro_cache_;
  bool is_clear_hot_macro_cache_;
};

using SSMacroCacheTabletStatMap = common::hash::ObHashMap<uint64_t, ObSSMacroCacheTabletStat>;
using SSMacroCacheTabletStatMapPairType =
    common::hash::HashMapPair<uint64_t, ObSSMacroCacheTabletStat>;
using SSMacroCacheLSInfoMap = common::hash::ObHashMap<share::ObLSID, ObSSMacroCacheLSInfo>;
using SSMacroCacheLSInfoMapPairType = common::hash::HashMapPair<share::ObLSID, ObSSMacroCacheLSInfo>;

class ObSSMacroCacheMgr
{
  friend class ObSSMacroCacheCkptTask;
  friend class ObSSMacroCacheReplayTask;
  friend class ObSSMacroCacheTabletStatTask;
  friend class ObSSMacroCacheRestoreInsertCallback;
  friend class ObSSMacroCacheRestoreUpdateCallback;
public:
  ObSSMacroCacheMgr();
  virtual ~ObSSMacroCacheMgr();
  static int mtl_init(ObSSMacroCacheMgr *&macro_cache_mgr);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  int wait();
  void destroy();
  bool is_valid() const;
  int put(const blocksstable::MacroBlockId &macro_id,
          const ObSSMacroCacheInfo &cache_info);
  int get(const blocksstable::MacroBlockId &macro_id,
          const common::ObTabletID &effective_tablet_id,
          const int64_t size,
          bool &is_hit_cache,
          ObSSFdCacheHandle &fd_handle);
  int check_if_write_cache_exist(const blocksstable::MacroBlockId &macro_id, bool &is_exist);
  int exist(const blocksstable::MacroBlockId &macro_id, bool &is_exist);
  int erase(const blocksstable::MacroBlockId &macro_id);
  int clear_macro_cache(const int64_t abs_timeout_us, const bool is_hot_tablet);
  // 1. if not exist, put macro cache.
  // 2. if already exist, update macro cache meta (e.g., effective_tablet_id, size, lru).
  int put_or_update(const blocksstable::MacroBlockId &macro_id,
                    const ObSSMacroCacheInfo &cache_info);
  int update_effective_tablet_id(const ObIArray<blocksstable::MacroBlockId> &macro_id_arr,
                                 const common::ObTabletID &effective_tablet_id);
  int clear_macro_cache_meta();
  int update_is_marked(const blocksstable::MacroBlockId &macro_id, const bool is_marked);
  int clear_is_marked();
  int batch_modify_cache_type(const common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_ids, 
      const ObSSMacroCacheType ori_cache_type, const ObSSMacroCacheType new_cache_type);
  int eliminate_macro_scp_blocks(const common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_ids, const common::hash::ObHashSet<int64_t> &tablet_ids, const int64_t elmination_time_us);
  int erase_unmarked_macro_cache_meta(const int64_t start_calc_size_time_us);
  int get_macro_cache_meta(const blocksstable::MacroBlockId &macro_id,
                           bool &is_exist,
                           ObSSMacroCacheMetaHandle &meta_handle);
  // get macro cache type from @meta_map_, which can be used to free_file_size
  int get_macro_cache_type(const blocksstable::MacroBlockId &macro_id,
                           ObSSMacroCacheType &cache_type);
  int add_into_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  int remove_from_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  int update_lru_list(ObSSMacroCacheMeta &macro_cache_meta);
  // for storage cache policy. remove from LRU-List of @ori_cache_type, add into LRU-List of @new_cache_type.
  // e.g., shift from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK, or shift from HOT_TABLET_MACRO_BLOCK to MACRO_BLOCK.
  int shift_lru_list(const ObSSMacroCacheType ori_cache_type,
                     const ObSSMacroCacheType new_cache_type,
                     ObSSMacroCacheMeta &macro_cache_meta);
  // 仅转换 cache type，不 touch cache，不修改 effective_tablet_id。
  int modify_macro_cache_type(const blocksstable::MacroBlockId &macro_id,
                              const ObSSMacroCacheType ori_cache_type,
                              const ObSSMacroCacheType new_cache_type);
  int modify_macro_cache_type(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids,
                              const ObSSMacroCacheType ori_cache_type,
                              const ObSSMacroCacheType new_cache_type);
  // 转换 cache type 并 touch；已不是 ori_cache_type 时仍按 touch 语义刷新热度。
  int modify_macro_cache_type_and_touch(
      const blocksstable::MacroBlockId &macro_id,
      const ObSSMacroCacheType ori_cache_type,
      const ObSSMacroCacheType new_cache_type);
  int modify_macro_cache_type_and_touch(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_ids,
      const ObSSMacroCacheType ori_cache_type,
      const ObSSMacroCacheType new_cache_type);
  // 尝试将 cache type 转化 hot。
  // 只在真实完成 MACRO_BLOCK->HOT_TABLET_MACRO_BLOCK 时更新 effective_tablet_id。
  int promote_macro_cache_to_hot(
      const blocksstable::MacroBlockId &macro_id,
      const common::ObTabletID &effective_tablet_id);
  int alloc_file_size(const ObSSMacroCacheType cache_type,
                      const int64_t alloc_size,
                      const ObDiskSpaceType disk_space_type);
  int alloc_file_size(const blocksstable::ObStorageObjectType object_type,
                      const uint64_t effective_tablet_id,
                      const bool use_effective_tablet_id,
                      const int64_t alloc_size,
                      const ObDiskSpaceType disk_space_type);
  int get_max_free_size_of_cache_type(const ObSSMacroCacheType &macro_cache_type, int64_t &max_free_size) const;
  int alloc_file_size_for_hot_tablet_with_retry(
      const blocksstable::ObStorageObjectType object_type,
      const uint64_t effective_tablet_id,
      const bool use_effective_tablet_id,
      const int64_t alloc_size,
      const int64_t timeout_us);
  static int free_file_size(const blocksstable::ObStorageObjectType object_type,
                            const uint64_t effective_tablet_id,
                            const bool use_effective_tablet_id,
                            const int64_t free_size,
                            const ObDiskSpaceType disk_space_type);
  int is_macro_cache_space_full(bool &is_full);
  int modify_from_write_cache_to_read_cache(const blocksstable::MacroBlockId &macro_id);
  common::ObSmallAllocator &get_macro_meta_allocator() { return macro_meta_allocator_; }
  void update_macro_meta_alloc_cnt(const int64_t delta) { ATOMIC_AAF(&macro_meta_cnt_, delta); }
  int64_t get_macro_meta_alloc_cnt() const { return ATOMIC_LOAD(&macro_meta_cnt_); }
  int push_to_flush_map(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  int seal_and_push_to_flush_map(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  int flush_write_cache(const blocksstable::MacroBlockId &macro_id, ObPromise<int> &promise);
  bool is_ckpt_replayed() const { return ATOMIC_LOAD(&is_ckpt_replayed_); }
  int do_expire_work();
  int64_t get_expiration_time_us();
  int do_write_cache_ctrl_work();
  int check_write_cache_threshold(bool &is_exceed);
  int do_evict_work();
  void set_fg_evict_trigger() { ATOMIC_STORE(&fg_evict_trigger_, true); }
  lib::ObMutex &get_lock_by_macro_id(const blocksstable::MacroBlockId &macro_id)
  {
    return macro_cache_lock_.get_lock_by_macro_id(macro_id);
  }
  ObSSMacroCacheFlushMap &get_flush_map() { return flush_map_; }
  ObSSMacroCacheFlushMap &get_write_cache_flush_map() { return write_cache_flush_map_; }
  int get_evict_entries(const ObSSMacroCacheType evict_macro_cache_type,
                        common::ObIArray<ObSSMacroCacheEvictEntry> &evict_entries);
  int gc_hot_tablet_macro_cache(
      const common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set,
      const int64_t idle_time_us,
      const bool convert_to_auto);
  int push_old_sstable_evict_task(const common::ObTabletID &tablet_id,
                                  const ObSSHotTabletOldSSTableEvictInfo &evict_info);
  // get macro cache type according to params, which is used to insert into @meta_map_ for write io
  static int get_macro_cache_type(const blocksstable::ObStorageObjectType object_type,
                                  const uint64_t effective_tablet_id,
                                  const bool use_effective_tablet_id,
                                  ObSSMacroCacheType &macro_cache_type);
  static uint64_t get_effective_tablet_id(const bool is_write_cache,
                                          const uint64_t io_ctx_tablet_id,
                                          const blocksstable::MacroBlockId &macro_id);
  // interface for statistics
  int get_macro_blocks_stat(const ObSSMacroBlockType block_type, ObSSCacheStatInfo &cache_stat_info);
  int get_macro_cache_hit_stat_by_cache_type(
      const ObSSMacroCacheType cache_type, ObStorageCacheHitStat &hit_stat) const;
  int get_macro_cache_hit_stat_by_block_type(
      const ObSSMacroBlockType block_type, ObStorageCacheHitStat &hit_stat) const;
  int get_tablet_status_from_map(const uint64_t tablet_id, ObSSMacroCacheTabletStat &tablet_stat); 
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSMacroCacheLSInfo &ls_cache_info);
  int get_external_table_cache_stat(ObStorageCacheStat &cache_stat) const;
  int64_t get_write_cache_size() { return ATOMIC_LOAD(&write_cache_size_); }
  // force_evict_by_macro_id: Wait for the write cache to become the read cache and evict it within 10 seconds
  int force_evict_by_macro_id(const blocksstable::MacroBlockId &macro_id);
  // evict_by_macro_id: Do not wait for write cache to become read cache
  int evict_by_macro_id(const blocksstable::MacroBlockId &macro_id, bool &is_write_cache);
  int batch_evict_by_macro_ids(const common::hash::ObHashSet<blocksstable::MacroBlockId> &cold_macro_ids,
                               common::hash::ObHashSet<blocksstable::MacroBlockId> &evicted_macro_ids);
  int evict_by_tablet_id(const ObTabletID &tablet_id, int64_t &macro_read_cache_cnt, int64_t &macro_write_cache_cnt);
  int batch_evict_by_tablet_ids(const common::hash::ObHashSet<uint64_t> &cold_tablet_ids, 
                                common::hash::ObHashSet<uint64_t> &evicted_tablet_ids);
  // interface for prewarm
  int divide_bucket_range(const int64_t split_count,
                          common::ObIArray<ObSSMacroCacheBucketIdxRange> &bucket_ranges);
  int copy_next_meta_map_bucket(const int64_t bucket_pos, ObIArray<ObSSMacroPrewarmMeta> &prewarm_metas);
  int get_fifo_size_from_ls_cache_info(int64_t &fifo_size);
  // Computes how much the macro cache capacity needs to be expanded for LS migration.
  //
  // @param src_ls_macro_cache_info  Macro-cache usage of the source LS (total size and hot size).
  // @param min_expand_size          Minimal extra macro cache space (in bytes) required so that all
  //                                 hot data from the source LS can be cached after migration, assuming
  //                                 cold data on both the source and destination may be discarded and
  //                                 the tenant-level macro cache usage does not exceed the eviction threshold.
  // @param best_expand_size         Extra macro cache space (in bytes) required so that all data (hot and
  //                                 cold) from the source LS can be cached without discarding any source data,
  //                                 while keeping overall macro cache usage under the eviction threshold to
  //                                 preserve the best post-migration cache performance.
  int cal_expand_size_for_ls_migration(
      const ObSSMacroCacheLSInfo &src_ls_macro_cache_info,
      int64_t &min_expand_size, int64_t &best_expand_size);
  // interface for expand or shrink disk size
  int cal_cur_min_shrinkable_size(int64_t &cur_min_shrinkable_size);
  // interface for virtual table
  int64_t get_meta_map_bucket_count() const { return meta_map_.bucket_count(); }
  int get_meta_by_bucket(const int64_t bucket_pos, ObIArray<ObSSMacroCacheMetaHandle> &meta_handles);

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(macro_meta_cnt), "meta_map_size", meta_map_.size(),
               K_(write_cache_size), K_(flying_req_cnt));

private:
  const SSMacroCacheMetaMap &get_meta_map() { return meta_map_; }
  int process_hot_macro_cache_candidates_(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_ids,
      const bool convert_to_auto);
  int modify_macro_cache_type_(
      const blocksstable::MacroBlockId &macro_id,
      const ObSSMacroCacheType ori_cache_type,
      const ObSSMacroCacheType new_cache_type,
      const ObSSMacroCacheModifyCacheTypeParam &modify_param);
  int modify_macro_cache_type_(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_ids,
      const ObSSMacroCacheType ori_cache_type,
      const ObSSMacroCacheType new_cache_type,
      const ObSSMacroCacheModifyCacheTypeParam &modify_param);
  int create_macro_caches();
  int get_macro_cache(const ObSSMacroCacheType cache_type, ObSSMacroCache *&macro_cache);
  int construct_meta_handle(const blocksstable::MacroBlockId &macro_id,
                            const ObSSMacroCacheInfo &cache_info,
                            ObSSMacroCacheMetaHandle &meta_handle);
  // update macro_cache_meta of @macro_id.
  // @update_param indicates which parts of macro_cache_meta are need to update.
  // @cache_info delivers content of the parts that are need to update.
  int update(const blocksstable::MacroBlockId &macro_id,
             const ObSSMacroCacheInfo &cache_info,
             const ObSSMacroCacheUpdateParam &update_param);
  int batch_erase(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);

  // interfaces for write cache ctrl (note: PRIVATE_DATA_MACRO and PRIVATE_META_MACRO)
  void inc_write_cache_size(const blocksstable::MacroBlockId &macro_id,
                            const int64_t delta_size)
  {
    if (macro_id.is_private_macro()) {
      IGNORE_RETURN ATOMIC_AAF(&write_cache_size_, delta_size);
    }
  }
  void dec_write_cache_size(const blocksstable::MacroBlockId &macro_id,
                            const int64_t delta_size)
  {
    if (macro_id.is_private_macro()) {
      IGNORE_RETURN ATOMIC_SAF(&write_cache_size_, delta_size);
    }
  }
  int find_and_flush_write_cache();
  int batch_flush_tail_write_cache(const ObSSMacroCacheType cache_type);

  // interfaces for evict
  int check_if_need_evict(bool &need_evict);
  int batch_evict();
  int expire_macro_cache_(const int64_t min_retention_time_us);
  int filter_expired_macro_cache_(const ObSSMacroCacheMetaHandle &meta_handle,
                                  const int64_t min_retention_time_us,
                                  common::ObIArray<blocksstable::MacroBlockId> &expired_entries);
  int batch_evict_expired_macro_cache_(const int64_t bucket_batch_cnt,
                                       int64_t &bucket_cnt,
                                       common::ObIArray<blocksstable::MacroBlockId> &expired_entries);
  int batch_evict_by_macro_id_(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  int evict_tablets(const ObTabletID *tablet_id,
                    const common::hash::ObHashSet<uint64_t> *tablet_id_set,
                    common::hash::ObHashSet<uint64_t> *evicted_tablet_ids,
                    int64_t &macro_read_cache_cnt,
                    int64_t &macro_write_cache_cnt);
  int handle_evict_tablet_node_(const blocksstable::MacroBlockId &macro_id,
                                const ObSSMacroCacheMetaHandle &meta_handle,
                                const ObTabletID *tablet_id,
                                const common::hash::ObHashSet<uint64_t> *tablet_id_set,
                                common::hash::ObHashSet<uint64_t> *evicted_tablet_ids,
                                ObIArray<blocksstable::MacroBlockId> &read_cache_macro_ids,
                                ObIArray<blocksstable::MacroBlockId> &write_cache_macro_ids);
  int handle_eliminate_macro_scp_node_(const blocksstable::MacroBlockId &macro_id,
                                       const ObSSMacroCacheMetaHandle &meta_handle,
                                       const common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_ids,
                                       const common::hash::ObHashSet<int64_t> &tablet_ids,
                                       const int64_t elmination_time_us,
                                       common::hash::ObHashSet<blocksstable::MacroBlockId> &eliminated_macro_ids);
  int handle_evict_bucket_batch_(const int64_t bucket_batch_cnt,
                                 int64_t &bucket_cnt,
                                 ObIArray<blocksstable::MacroBlockId> &read_cache_macro_ids,
                                 ObIArray<blocksstable::MacroBlockId> &write_cache_macro_ids,
                                 int64_t &macro_read_cache_cnt,
                                 int64_t &macro_write_cache_cnt);
  int evict_macro_id_array(const ObIArray<blocksstable::MacroBlockId> &read_cache_macro_ids,
                           const ObIArray<blocksstable::MacroBlockId> &write_cache_macro_ids,
                           int64_t &macro_read_cache_cnt,
                           int64_t &macro_write_cache_cnt);
  // interfaces for ckpt
  void set_next_ckpt_version(const uint64_t next_ckpt_version)
  {
    ckpt_task_.set_next_ckpt_version(next_ckpt_version);
  }
  void set_ckpt_replayed() { ATOMIC_STORE(&is_ckpt_replayed_, true); }
  int replay_ckpt_(uint64_t &next_ckpt_version);
  int restore_cache_from_ckpt_data_(const uint64_t version_id,
                                    const ObSSMacroCacheCkptMeta &ckpt_meta,
                                    int64_t &replay_item_cnt);
  int deserialize_metas_from_buf_(
      const char *buf, const int64_t buf_size,
      common::ObIArray<ObSSMacroCacheMetaHandle> &macro_cache_metas);
  int restore_cache_entries_(
      const int64_t ckpt_time_diff_us,
      common::ObIArray<ObSSMacroCacheMetaHandle> &macro_cache_metas);
  int update_write_cache_size_for_restore_(const ObSSMacroCacheMetaHandle &meta_handle,
                                           const ObSSMacroCacheRestoreUpdateCallback &update_callback);
  int restore_entry_for_insert_(ObSSMacroCacheMeta &macro_cache_meta);
  int restore_entry_for_update_(
      ObSSMacroCacheMeta &old_macro_cache_meta, ObSSMacroCacheMeta &new_macro_cache_meta);

  // interfaces for statistics
  void try_update_hit_stat_(const blocksstable::MacroBlockId &macro_id,
                            const common::ObTabletID &effective_tablet_id,
                            const int64_t size,
                            const ObSSMacroCacheGetFdCallback &get_fd_callback);
  void update_cache_hit_stat_(
      const bool is_hit, const ObSSMacroCacheType cache_type, const int64_t size);
  void update_block_hit_stat_(
      const bool is_hit, const blocksstable::MacroBlockId &macro_id, const int64_t size);
  void update_tablet_hit_stat_(
      const bool is_hit, const blocksstable::MacroBlockId &macro_id,
      const common::ObTabletID &tablet_id, const int64_t size);
  int calibrate_tablet_stat_cached_size_();
  int clean_deleted_tablet_();
  int clean_deleted_tablet_in_tablet_stat_map_(
      const common::hash::ObHashSet<uint64_t> &active_tablet_ids);
  int clean_deleted_tablet_in_meta_map_(
      const common::hash::ObHashSet<uint64_t> &active_tablet_ids,
      const bool enable_private_gc_hot_macro_cache_convert_to_auto);
  int update_ls_info_();
  void print_ls_info_();
  int batch_clean_old_tablet_stat_cached_size_(const ObIArray<uint64_t> &tablet_ids);
  int clean_old_tablet_stat_cached_size_(const SSMacroCacheTabletStatMap &tmp_tablet_stat_map);

public:
  static const int64_t CACHE_EVICT_THRESHOLD = 95; // 95%
  static constexpr int64_t MIN_REQUIRED_DISK_SIZE = common::GB;  // 1GB
  static const int64_t META_TENANT_CACHE_EVICT_THRESHOLD = 50; // 50%

private:
  static const int64_t PRINT_LOG_INTERVAL = 30L * 1000L * 1000L; //30s
  static const int INVALID_TG_ID = -1;
  static const int64_t BATCH_EVICT_CNT = 100;
  static constexpr int64_t DEFAULT_CACHE_EXPIRATION_TIME_US = 48 * 3600L * 1000L * 1000L; // 2 days
  static const int64_t WRITE_CACHE_THRESHOLD = 50; // 50%
  static const int64_t WRITE_CAHCE_BATCH_FLUSH_CNT = 128; // 128 * 2MB = 256MB
  bool is_inited_;
  volatile bool is_stopped_;
  bool is_ckpt_replayed_;
  ObSSMacroCacheClearFlag clear_flag_;
  // foreground evict trigger. set to true, when return OB_SERVER_OUTOF_DISK_SPACE on alloc_file_size
  bool fg_evict_trigger_;
  uint64_t tenant_id_;
  int high_tg_id_;
  int low_tg_id_;
  // record write cache size. the following interfaces should update @write_cache_size_:
  // put, put_or_update, erase, modify_from_write_cache_to_read_cache, restore_cache_entries
  int64_t write_cache_size_;
  int64_t flying_req_cnt_;
  int64_t macro_meta_cnt_;
  common::ObSmallAllocator macro_meta_allocator_;
  SSMacroCacheMetaMap meta_map_;
  ObSSMacroCache *macro_caches_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  // in order to address concurrent put/put and put/erase of one same macro_id
  ObSSMacroCacheLock macro_cache_lock_;
  ObSSMacroCacheFlushMap flush_map_;
  ObSSMacroCacheFlushTask flush_task_;
  ObSSMacroCacheFlushMap write_cache_flush_map_;
  ObSSWriteCacheFlushTask write_cache_flush_task_;
  ObSSMacroCacheEvictTask evict_task_;
  ObSSMacroCacheExpireTask expire_task_;
  ObSSHotTabletMacroCacheGCTask hot_tablet_macro_cache_gc_task_;
  ObSSHotTabletOldSSTableEvictTask hot_tablet_old_sstable_evict_task_;
  // control write cache under WRITE_CACHE_THRESHOLD
  ObSSWriteCacheCtrlTask write_cache_ctrl_task_;
  ObSSMacroCacheCkptTask ckpt_task_;
  ObSSMacroCacheReplayTask replay_task_;

  // hit rate statistics for different ObSSMacroCacheType
  // (e.g., META_FILE, TMP_FILE, MACRO_BLOCK, HOT_TABLET_MACRO_BLOCK)
  ObStorageCacheHitStat macro_caches_hit_stat_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  // hit rate statistics and used disk size for different ObSSMacroBlockType
  // (e.g., PRIVATE_MACRO, SHARED_MACRO, EXTERNAL_TABLE)
  ObSSCacheStatInfo macro_blocks_stat_[SS_MACRO_BLOCK_MAX_TYPE_VAL];
  SSMacroCacheTabletStatMap tablet_stat_map_;
  ObSSMacroCacheTabletStatTask tablet_stat_task_;
  // macro cache info of each ls
  SSMacroCacheLSInfoMap ls_info_map_;
  ObStorageCacheStat external_table_cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_MGR_H_ */
