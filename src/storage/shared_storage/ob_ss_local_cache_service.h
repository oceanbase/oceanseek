/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_SERVICE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_SERVICE_H_

#include "lib/container/ob_iarray.h"
#include "lib/queue/ob_link_queue.h"
#include "common/ob_tablet_id.h"
#include "share/shared_storage/ob_local_cache_control_mode.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_prewarm_entry.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_prewarm_map_clean_task.h"
#include "storage/shared_storage/task/ob_ss_local_cache_param_refresh_task.h"
#include "storage/shared_storage/task/ob_ss_local_cache_stat_clean_task.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/shared_storage/ob_ss_local_cache_stat.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/shared_storage/task/ob_ss_object_type_cached_stat_refresh_task.h"
#include "storage/shared_storage/task/ob_ss_write_lcache_task.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_hot_tablet_old_sstable_evict_task.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
namespace storage
{
struct ObSSLocalCacheLSInfo;
struct ObHACacheJobInfo;
struct ObHALocalCacheTaskCount;

using SSMacroCachePrewarmMap = common::hash::ObHashMap<blocksstable::MacroBlockId, ObSSMacroCachePrewarmEntry>;
using SSMacroCachePrewarmMapPairType = common::hash::HashMapPair<blocksstable::MacroBlockId, ObSSMacroCachePrewarmEntry>;

enum class ObSSLocalCacheEvictOldSSTablePolicy : uint8_t
{
  OFF = 0,
  MAJOR_SSTABLE = 1,
  MAX_POLICY = 2
};

int get_evict_old_sstable_policy(const ObString &policy_str, ObSSLocalCacheEvictOldSSTablePolicy &policy);


class ObSSMacroCachePrewarmEntryUpdateFn
{
public:
  explicit ObSSMacroCachePrewarmEntryUpdateFn(const common::ObTabletID &effective_tablet_id, const int64_t threshold_shrink_factor)
    : ret_(OB_SUCCESS), effective_tablet_id_(effective_tablet_id), threshold_shrink_factor_(threshold_shrink_factor) {}
  virtual ~ObSSMacroCachePrewarmEntryUpdateFn() {}
  void operator()(SSMacroCachePrewarmMapPairType &entry);
  OB_INLINE int get_ret() { return ret_; }
  TO_STRING_KV(K_(ret), K_(effective_tablet_id), K_(threshold_shrink_factor));

private:
  int ret_;
  common::ObTabletID effective_tablet_id_;
  int64_t threshold_shrink_factor_;  // shrink factor for cache_miss_threshold, default is 1 (no shrink)
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCachePrewarmEntryUpdateFn);
};

class ObSSLocalCacheParams
{
public:
  ObSSLocalCacheParams();
  virtual ~ObSSLocalCacheParams();
  void reset();
  TO_STRING_KV("local_cache_mode", local_cache_control_mode_.get_value(),
               K_(miss_threshold_for_prefetch), K_(disk_space_cal_interval_us),
               K(macro_cache_min_admit_block_size_));

public:
  static const int64_t DEF_MISS_THRESHOLD_FOR_PREFETCH = 10;
  static const int64_t DEF_DISK_SPACE_CAL_INTERVAL_US = 3600 * 1000L * 1000L; // 1h
  static const int64_t DEF_MACRO_CACHE_MIN_ADMIT_BLOCK_SIZE = 128 * 1024; // 128KB
  static const ObSSLocalCacheEvictOldSSTablePolicy DEF_EVICT_OLD_SSTABLE_POLICY = ObSSLocalCacheEvictOldSSTablePolicy::OFF;
  common::ObSSLocalCacheControlMode local_cache_control_mode_;
  int64_t miss_threshold_for_prefetch_;
  int64_t disk_space_cal_interval_us_;
  // Non-hot blocks whose size is smaller than this value are too small to enter macro cache.
  int64_t macro_cache_min_admit_block_size_;
  ObSSLocalCacheEvictOldSSTablePolicy evict_old_sstable_policy_;
};

/*
 *
 * To manage macro_cache
 *
 */
class ObSSLocalCacheService
{
public:
  ObSSLocalCacheService();
  virtual ~ObSSLocalCacheService() {}
  static int mtl_init(ObSSLocalCacheService *&local_cache);
  static bool is_need_update_hit_stat();
  int init();
  int start();
  void stop();
  int wait();
  void destroy();

  // alter system flush local cache
  int clear_ss_all_cache(const int64_t abs_timeout_us);
  int clear_ss_macro_cache(const int64_t abs_timeout_us);
  int erase_ss_macro_block_kvcache_entries();
  int clear_ss_hot_macro_cache(const int64_t abs_timeout_us);

  // old sstable evict task
  int push_old_sstable_evict_task(const common::ObTabletID &tablet_id,
                                  const ObSSHotTabletOldSSTableEvictInfo &evict_info);
  
  // effective_tablet_id
  void update_macro_cache_tablet_id(const ObIArray<blocksstable::MacroBlockId> &macro_id_arr,
                                    const common::ObTabletID &effective_tablet_id);
  // macro cache prewarm on read
  int refresh_macro_cache_prewarm_map(const blocksstable::MacroBlockId &macro_id,
                                      const common::ObTabletID &effective_tablet_id,
                                      const int64_t threshold_shrink_factor);
  int do_prewarm_map_clean_work();

  // _local_cache_control
  void set_local_cache_control_mode(const common::ObSSLocalCacheControlMode &mode);
  bool is_macro_read_cache_disabled() const;
  bool is_macro_write_cache_disabled() const;
  int64_t get_miss_threshold_for_prefetch() const
  {
    return local_cache_params_.miss_threshold_for_prefetch_;
  }
  void set_miss_threshold_for_prefetch(const int64_t threshold)
  {
    local_cache_params_.miss_threshold_for_prefetch_ = threshold;
  }
  int64_t get_disk_space_cal_interval_us() const
  {
    return local_cache_params_.disk_space_cal_interval_us_;
  }
  void set_disk_space_cal_interval_us(const int64_t disk_space_cal_interval_us)
  {
    local_cache_params_.disk_space_cal_interval_us_ = disk_space_cal_interval_us;
  }
  int64_t get_macro_cache_min_admit_block_size() const
  {
    return local_cache_params_.macro_cache_min_admit_block_size_;
  }
  void set_macro_cache_min_admit_block_size(const int64_t macro_cache_min_admit_block_size)
  {
    local_cache_params_.macro_cache_min_admit_block_size_ = macro_cache_min_admit_block_size;
  }
  ObSSLocalCacheEvictOldSSTablePolicy get_evict_old_sstable_policy() const
  {
    return local_cache_params_.evict_old_sstable_policy_;
  }
  void set_evict_old_sstable_policy(const ObSSLocalCacheEvictOldSSTablePolicy policy)
  {
    local_cache_params_.evict_old_sstable_policy_ = policy;
  }
  // local cache tablet stat
  int refresh_local_cache_tablet_stat_map(const common::ObTabletID &effective_tablet_id, const bool &is_hit, const int64_t &size);
  int do_local_cache_tablet_stat_map_clean_work();
  int get_local_cache_tablet_stat(const common::ObTabletID &effective_tablet_id, storage::ObStorageCacheHitStat &entry);

  // interfaces for ha prewarm
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSLocalCacheLSInfo &ls_cache_info);
  int get_job_info(const share::ObLSID &ls_id,
                   const ObHALocalCacheTaskCount &task_count,
                   common::ObIArray<ObHACacheJobInfo> &job_infos);
  int filter_repeated_local_cache_keys(ObHALocalCacheKeys &local_cache_keys);
  int get_local_cache_retention_rate(const ObSSLocalCacheLSInfo &cache_info,
                                     const uint64_t min_cluster_version,
                                     LocalCacheRetentionRateGroup &retention_rate_group);
  
  int update_object_type_stat(const blocksstable::ObStorageObjectType &object_type,
                              const ObSSObjectTypeStatType &stat_type, const bool is_remote, const int result,
                              const int64_t delta_cnt, const int64_t delta_size, const int64_t delta_time_us);
  int refresh_object_type_cached_stat();
  int get_object_type_stat(const blocksstable::ObStorageObjectType &object_type, const bool is_remote, ObSSObjectTypeStat &type_stat);
  int get_object_type_cached_stat(const blocksstable::ObStorageObjectType &object_type,
                                  const bool is_remote, ObSSObjectTypeCachedStat &cached_stat);
  ObSSLocalCacheStat &get_local_cache_stat() { return local_cache_stat_; }
  ObSSLocalCacheDiagnoseStat &get_local_cache_diagnose_stat() { return local_cache_diagnose_stat_; }

  int async_write_macro_to_lcache(
      const blocksstable::MacroBlockId &macro_id,
      const char *buf, const int64_t size,
      const uint64_t effective_tablet_id,
      const common::ObIOFlag &io_desc);
  // interfaces for WRITE_THROUGH_AND_TRY_WRITE_LCACHE strategy
  int push_write_lcache_queue(ObSSWriteLCacheEntry &entry);
  int check_exist_in_all_macro_cache(const blocksstable::MacroBlockId &macro_id, bool &is_exist);

  int prewarm_table_opt_index(const common::ObString &db_name, const common::ObString &table_name,
                              const bool with_local_index, const bool with_global_index);
  int prewarm_tablet_opt_index(const common::ObTabletID &tablet_id, const bool with_local_index);

private:
  int batch_erase_prewarm_entry(const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  void reset_cache_stat();
  int update_object_type_stat_by_stat_type(const blocksstable::ObStorageObjectType &object_type,
    const ObSSObjectTypeStatType &stat_type, const int result, const int64_t delta_cnt,
    const int64_t delta_size, const int64_t delta_time_us, ObSSObjectTypeStat object_type_stats[]);
  int do_prewarm_tablets(const ObIArray<common::ObTabletID> &tablet_ids, const PolicyStatus &policy_status,
    int64_t &succ_prewarm_cnt);
private:
  // 10w * 2MB / 5min = 666 MB/s, which is enough
  static const int64_t MACRO_CACHE_PREWARM_MAP_MAX_SIZE = 100000; //10w
  static const int64_t PREWARM_ENTRY_EXPIRATION_TIME_US = 300 * 1000L * 1000L; // 5min
  static const int INVALID_TG_ID = -1;
  bool is_inited_;
  volatile bool is_stopped_;
  int mid_tg_id_;
  int high_tg_id_;
  ObSSLocalCacheParams local_cache_params_;
  ObSSLocalCacheParamRefreshTask local_cache_param_refresh_task_;
  // this map stores 'macro-cache-miss' macro ids, when macro-cache has free space.
  SSMacroCachePrewarmMap macro_cache_prewarm_map_;
  ObSSPrewarmMapCleanTask prewarm_map_clean_task_;
  // this map stores hit and access info of tablet
  SSTabletHitStatMap local_cache_tablet_stat_map_;
  ObSSLocalCacheStatCleanTask local_cache_stat_clean_task_;
  // this map stores statistics of object type
  ObSSObjectTypeStat object_type_local_stats_[blocksstable::SS_OBJECT_MAX_TYPE_VAL];
  ObSSObjectTypeStat object_type_remote_stats_[blocksstable::SS_OBJECT_MAX_TYPE_VAL];
  ObSSObjectTypeCachedStat object_type_cached_local_stats_[blocksstable::SS_OBJECT_MAX_TYPE_VAL];
  ObSSObjectTypeCachedStat object_type_cached_remote_stats_[blocksstable::SS_OBJECT_MAX_TYPE_VAL];
  ObSSObjectTypeCachedStatRefreshTask object_type_cached_stat_refresh_task_;
  ObSSLocalCacheStat local_cache_stat_;
  // for WRITE_THROUGH_AND_TRY_WRITE_LCACHE strategy
  ObSSWriteLCacheTask write_lcache_task_;
  ObSSLocalCacheDiagnoseStat local_cache_diagnose_stat_;
};

bool is_macro_read_cache_disabled();
bool is_macro_write_cache_disabled();

#define ADD_LOCAL_CACHE_DIAGNOSE(mod_name, sub_mod_name, status, diagnose_info_str, extra_info_str) \
  do { \
    if (nullptr != MTL(ObSSLocalCacheService *)) { \
      ObSSLocalCacheDiagnoseStat &diag_stat = MTL(ObSSLocalCacheService *)->get_local_cache_diagnose_stat(); \
      diag_stat.add_local_cache_diagnose_info(mod_name, sub_mod_name, status, diagnose_info_str, extra_info_str); \
    } \
  } while (0); \

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_SERVICE_H_ */
