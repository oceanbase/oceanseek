/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_STAT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/macro_cache/ob_storage_disk_space_meta.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSLocalCachePrewarmType : uint8_t
{
  MAJOR_COMPACTION = 0,
  MIGRATION = 1,
  DDL = 2,
  STORAGE_CACHE_POLICY = 3,
  MINOR_COMPACTION = 4,
  PREREAD_TASK = 5,
  SPLIT_PART = 6,
  TRY_WRITE_LCACHE = 7,
  MAX,
};

void inc_local_cache_prewarm_stat(
    const ObSSLocalCachePrewarmType prewarm_type,
    const int64_t delta_cnt,
    const int64_t delta_bytes);

struct ObSSCacheStatInfo
{
public:
  ObStorageCacheHitStat hit_stat_;
  int64_t alloc_disk_size_;
  int64_t used_disk_size_;
  int64_t used_mem_size_;

  ObSSCacheStatInfo() { reset(); }
  ~ObSSCacheStatInfo() { reset(); }

  void reset()
  {
    hit_stat_.reset();
    alloc_disk_size_ = 0;
    used_disk_size_ = 0;
    used_mem_size_ = 0;
  }

  void update_hit_stat(const bool is_hit, const int64_t delta_cnt, const int64_t delta_bytes)
  {
    if (is_hit) {
      hit_stat_.update_cache_hit(delta_cnt, delta_bytes);
    } else {
      hit_stat_.update_cache_miss(delta_cnt, delta_bytes);
    }
  }
  
  void update_used_disk_size(const int64_t delta_bytes)
  {
    ATOMIC_AAF(&used_disk_size_, delta_bytes);
  }

  void set_used_disk_size(const int64_t used_disk_size)
  {
    ATOMIC_STORE(&used_disk_size_, used_disk_size);
  }

  int64_t get_hit_cnt() const { return hit_stat_.get_hit_cnt(); }
  int64_t get_hit_bytes() const { return hit_stat_.get_hit_bytes(); }
  int64_t get_miss_cnt() const { return hit_stat_.get_miss_cnt(); }
  int64_t get_miss_bytes() const { return hit_stat_.get_miss_bytes(); }
  int64_t get_used_disk_size() const { return ATOMIC_LOAD(&used_disk_size_); }

  TO_STRING_KV(K_(hit_stat), K_(alloc_disk_size), K_(used_disk_size), K_(used_mem_size));
};

struct ObSSMacroCacheAllStat
{
public:
  ObSSCacheStatInfo tmpfile_stat_;
  ObSSCacheStatInfo macro_block_stat_[SS_MACRO_BLOCK_MAX_TYPE_VAL];

  ObSSMacroCacheAllStat() { reset(); }
  ~ObSSMacroCacheAllStat() { reset(); }

  void reset()
  {
    tmpfile_stat_.reset();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      macro_block_stat_[i].reset();
    }
  }

  void update_tmpfile_hit_stat(const bool is_hit, const int64_t delta_cnt, const int64_t delta_bytes)
  {
    if (is_hit) {
      tmpfile_stat_.hit_stat_.update_cache_hit(delta_cnt, delta_bytes);
    } else {
      tmpfile_stat_.hit_stat_.update_cache_miss(delta_cnt, delta_bytes);
    }
  }

  void update_macro_block_hit_stat(const ObSSMacroBlockType macro_block_type, const bool is_hit, const int64_t delta_cnt, const int64_t delta_bytes)
  {
    if (OB_LIKELY(is_valid_ss_macro_block_type(macro_block_type))) {
      if (is_hit) {
        macro_block_stat_[static_cast<uint8_t>(macro_block_type)].hit_stat_.update_cache_hit(delta_cnt, delta_bytes);
      } else {
        macro_block_stat_[static_cast<uint8_t>(macro_block_type)].hit_stat_.update_cache_miss(delta_cnt, delta_bytes);
      }
    }
  }

  int64_t get_total_hit_cnt() const
  {
    int64_t total_hit_cnt = tmpfile_stat_.get_hit_cnt();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      total_hit_cnt += macro_block_stat_[i].get_hit_cnt();
    }
    return total_hit_cnt;
  }

  int64_t get_total_hit_bytes() const
  {
    int64_t total_hit_bytes = tmpfile_stat_.get_hit_bytes();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      total_hit_bytes += macro_block_stat_[i].get_hit_bytes();
    }
    return total_hit_bytes;
  }

  int64_t get_total_miss_cnt() const
  {
    int64_t total_miss_cnt = tmpfile_stat_.get_miss_cnt();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      total_miss_cnt += macro_block_stat_[i].get_miss_cnt();
    }
    return total_miss_cnt;
  }

  int64_t get_total_miss_bytes() const
  {
    int64_t total_miss_bytes = tmpfile_stat_.get_miss_bytes();
    for (uint8_t i = 0; i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
      total_miss_bytes += macro_block_stat_[i].get_miss_bytes();
    }
    return total_miss_bytes;
  }

  TO_STRING_KV(K_(tmpfile_stat), "macro_block_stat", ObArrayWrap<ObSSCacheStatInfo>(macro_block_stat_, SS_MACRO_BLOCK_MAX_TYPE_VAL));
};

struct ObSSLocalCacheStat
{
public:
  // L1 macro block KVCACHE and L2 disk MacroCache aggregate stat
  ObSSCacheStatInfo local_cache_stat_;
  ObSSMacroCacheAllStat macro_cache_stat_;

  ObSSLocalCacheStat() { reset(); }
  ~ObSSLocalCacheStat() { reset(); }

  void reset()
  {
    local_cache_stat_.reset();
    macro_cache_stat_.reset();
  }

  void update_local_cache_hit_stat(const bool need_read_macro_cache, const bool is_macro_cache_hit,
    const blocksstable::MacroBlockId &macro_id, const int64_t delta_cnt, const int64_t delta_bytes);

  TO_STRING_KV(K_(local_cache_stat), K_(macro_cache_stat));
};

enum ObSSObjectTypeStatType : uint8_t
{
  READ = 0,
  WRITE = 1, 
  DELETE = 2,
  MAX,
};

static constexpr uint8_t SS_OBJECT_TYPE_MAX_STAT_VAL = static_cast<uint8_t>(ObSSObjectTypeStatType::MAX);

struct ObSSBaseStat
{
public:
  ObSSBaseStat() { reset(); }
  virtual ~ObSSBaseStat() { reset(); }

  void reset()
  {
    cnt_ = 0;
    fail_cnt_ = 0;
    size_ = 0;
    time_us_ = 0;
  }

  void add_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&cnt_, delta_cnt);
  }

  void add_fail_cnt(const int64_t delta_cnt)
  {
    ATOMIC_AAF(&fail_cnt_, delta_cnt);
  }

  void add_size(const int64_t delta_size)
  {
    ATOMIC_AAF(&size_, delta_size);
  }
 
   void add_time_us(const int64_t delta_time_us)
   {
     ATOMIC_AAF(&time_us_, delta_time_us);
   }

  int64_t get_cnt() const { return ATOMIC_LOAD(&cnt_); }
  int64_t get_fail_cnt() const { return ATOMIC_LOAD(&fail_cnt_); }
  int64_t get_size() const { return ATOMIC_LOAD(&size_); }
  int64_t get_time_us() const { return ATOMIC_LOAD(&time_us_); }

  TO_STRING_KV(K_(cnt), K_(fail_cnt), K_(size), K_(time_us));

private:
  int64_t cnt_;
  int64_t fail_cnt_;
  int64_t size_;
  int64_t time_us_;
};

struct ObSSObjectTypeStat
{
public:
  ObSSBaseStat object_type_stat_[SS_OBJECT_TYPE_MAX_STAT_VAL];
  ObSSObjectTypeStat() { reset(); }
  virtual ~ObSSObjectTypeStat() { reset(); }

  void reset()
  {
    for (uint8_t i = 0; i < SS_OBJECT_TYPE_MAX_STAT_VAL; i++) {
      object_type_stat_[i].reset();
    }
  }
  
  void add_base_info(const ObSSObjectTypeStatType stat_type, const int64_t delta_cnt, const int64_t delta_size, const int64_t delta_time_us)
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      object_type_stat_[static_cast<uint8_t>(stat_type)].add_cnt(delta_cnt);
      if (stat_type != ObSSObjectTypeStatType::DELETE) {
        object_type_stat_[static_cast<uint8_t>(stat_type)].add_size(delta_size);
        object_type_stat_[static_cast<uint8_t>(stat_type)].add_time_us(delta_time_us);
      }
    }
  }

  void add_fail_info(const ObSSObjectTypeStatType stat_type, const int64_t delta_cnt)
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      object_type_stat_[static_cast<uint8_t>(stat_type)].add_fail_cnt(delta_cnt);
    }
  }

  void get_stat(const ObSSObjectTypeStatType stat_type, ObSSBaseStat &stat) const
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      stat = object_type_stat_[static_cast<uint8_t>(stat_type)];
    }
  }

  TO_STRING_KV("object_type_stat", ObArrayWrap<ObSSBaseStat>(object_type_stat_, SS_OBJECT_TYPE_MAX_STAT_VAL));
};

struct ObSSBaseCachedStat
{
public:
  int64_t pre_cnt_;
  int64_t cur_cnt_;
  int64_t pre_bytes_;
  int64_t cur_bytes_;
  int64_t pre_io_time_us_;
  int64_t cur_io_time_us_;
  int64_t pre_update_time_us_;
  int64_t cur_update_time_us_;

  ObSSBaseCachedStat() { reset(); }
  virtual ~ObSSBaseCachedStat() { reset(); }

  void reset()
  {
    pre_cnt_ = 0;
    cur_cnt_ = 0;
    pre_bytes_ = 0;
    cur_bytes_ = 0;
    pre_io_time_us_ = 0;
    cur_io_time_us_ = 0;
    pre_update_time_us_ = 0;
    cur_update_time_us_ = 0;
  }

  void update_cached_stat(const int64_t cur_io_time_us, const int64_t cur_cnt, const int64_t cur_bytes)
  {
    pre_cnt_ = cur_cnt_;
    cur_cnt_ = cur_cnt;
    pre_bytes_ = cur_bytes_;
    cur_bytes_ = cur_bytes;
    pre_io_time_us_ = cur_io_time_us_;
    cur_io_time_us_ = cur_io_time_us;
    const int64_t now_us = ObTimeUtility::current_time_us();
    pre_update_time_us_ = cur_update_time_us_;
    cur_update_time_us_ = now_us;
  }

  int64_t get_iops() const
  {
    int64_t iops = 0;
    const int64_t now_us = ObTimeUtility::current_time_us();
    const int64_t interval_us = cur_update_time_us_ - pre_update_time_us_;
    if (interval_us > 0) {
      const double tmp_iops = static_cast<double>((cur_cnt_ - pre_cnt_) * 1000L * 1000L) / interval_us;
      // round half up
      iops = static_cast<int64_t>(std::round(tmp_iops));
    }
    return iops;
  }
 
  int64_t get_bandwidth() const
  {
    int64_t bandwidth = 0;
    const int64_t interval_us = cur_update_time_us_ - pre_update_time_us_;
    if (interval_us > 0) {
      bandwidth = ((cur_bytes_ - pre_bytes_) * 1000L * 1000L) / interval_us;  // byte/s
    }
    return bandwidth;
  }
 
  int64_t get_delay_us() const
  {
    int64_t delay_us = 0;
    const int64_t interval_us = cur_io_time_us_ - pre_io_time_us_;
    const int64_t delta_cnt = cur_cnt_ - pre_cnt_;
    if (interval_us > 0 && delta_cnt > 0) {
      delay_us = interval_us / delta_cnt;
    }
    return delay_us;
  }
 
  TO_STRING_KV("pre_cnt", pre_cnt_, "cur_cnt", cur_cnt_, "pre_bytes", pre_bytes_, "cur_bytes", cur_bytes_, "pre_io_time_us", pre_io_time_us_,
               "cur_io_time_us", cur_io_time_us_, "pre_update_time_us", pre_update_time_us_, "cur_update_time_us", cur_update_time_us_);
};

struct ObSSObjectTypeCachedStat
{
public:
  ObSSBaseCachedStat object_type_cached_stat_[SS_OBJECT_TYPE_MAX_STAT_VAL];

  ObSSObjectTypeCachedStat() { reset(); }
  virtual ~ObSSObjectTypeCachedStat() { reset(); }
  void reset()
  {
    for (uint8_t i = 0; i < SS_OBJECT_TYPE_MAX_STAT_VAL; i++) {
      object_type_cached_stat_[i].reset();
    }
  }

  void update_cached_stat(const ObSSObjectTypeStatType stat_type, const int64_t cur_time_us, const int64_t cur_cnt, const int64_t cur_bytes)
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
       object_type_cached_stat_[static_cast<uint8_t>(stat_type)].update_cached_stat(cur_time_us, cur_cnt, cur_bytes);
    }
  }

  int64_t get_iops(const ObSSObjectTypeStatType stat_type) const
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      return object_type_cached_stat_[static_cast<uint8_t>(stat_type)].get_iops();
    }
    return 0;
  }
 
  int64_t get_bandwidth(const ObSSObjectTypeStatType stat_type) const
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      return object_type_cached_stat_[static_cast<uint8_t>(stat_type)].get_bandwidth();
    }
    return 0;
  }
 
  int64_t get_delay_us(const ObSSObjectTypeStatType stat_type) const
  {
    if (stat_type >= ObSSObjectTypeStatType::READ && stat_type < ObSSObjectTypeStatType::MAX) {
      return object_type_cached_stat_[static_cast<uint8_t>(stat_type)].get_delay_us();
    }
    return 0;
  }

  TO_STRING_KV("object_type_cached_stat", ObArrayWrap<ObSSBaseCachedStat>(object_type_cached_stat_, SS_OBJECT_TYPE_MAX_STAT_VAL));
};

using SSTabletHitStatMap = common::hash::ObHashMap<uint64_t, ObStorageCacheHitStat>;
using SSTabletHitStatMapPairType = common::hash::HashMapPair<uint64_t, ObStorageCacheHitStat>;

class ObSSTabletHitStatEntryUpdateFn
{
public:
  ObSSTabletHitStatEntryUpdateFn(const bool is_hit, const int64_t size)
    : is_hit_(is_hit), size_(size) {}
  virtual ~ObSSTabletHitStatEntryUpdateFn() {}
  OB_INLINE bool get_is_hit() { return is_hit_; }
  OB_INLINE int64_t get_size() { return size_; }

  void operator()(SSTabletHitStatMapPairType &entry)
  {
    ObStorageCacheHitStat &stat_entry = entry.second;
    if (get_is_hit()) {
      stat_entry.update_cache_hit(1/*delta_cnt*/, get_size());
    } else {
      stat_entry.update_cache_miss(1/*delta_cnt*/, get_size());
    }
  }

  TO_STRING_KV(K_(is_hit), K_(size));
private:
  bool is_hit_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletHitStatEntryUpdateFn);
};

enum class ObSSLocalCacheModStatus : uint8_t
{
  INVALID_STATUS = 0,
  NORMAL_STATUS = 1,
  WARNING_STATUS = 2,
  ERROR_STATUS = 3,
};

struct ObSSLocalCacheDiagnoseInfo
{
public:
  char mod_name_[OB_SS_DIAGNOSE_MOD_NAME_LENGTH];
  char sub_mod_name_[OB_SS_DIAGNOSE_MOD_NAME_LENGTH];
  ObSSLocalCacheModStatus status_;
  int64_t modify_time_;
  char diagnose_info_[common::OB_DIAGNOSE_INFO_LENGTH];
  char extra_info_[common::OB_DIAGNOSE_INFO_LENGTH];

  ObSSLocalCacheDiagnoseInfo() { reset(); }
  ~ObSSLocalCacheDiagnoseInfo() { reset(); }
  void reset();
  int init(const char *mod_name, const char *sub_mod_name, const ObSSLocalCacheModStatus &status,
           const char *diagnose_info, const char *extra_info);
  int update(const ObSSLocalCacheModStatus &status, const char *diagnose_info, const char *extra_info);
  bool is_valid() const;
  bool match(const char *mod_name, const char *sub_mod_name) const;

  TO_STRING_KV(K_(mod_name), K_(sub_mod_name), K_(status), K_(modify_time), K_(diagnose_info));
};

class ObSSLocalCacheDiagnoseStat
{
public:
  ObSSLocalCacheDiagnoseStat();
  ~ObSSLocalCacheDiagnoseStat();

  int init(const uint64_t tenant_id);
  void reset();

  void add_local_cache_diagnose_info(const char *mod_name, const char *sub_mod_name,
       const ObSSLocalCacheModStatus &status, const char *diagnose_info_str, const char *extra_info_str);
  int get_diagnose_info_list(common::ObIArray<ObSSLocalCacheDiagnoseInfo> &info_list);

  TO_STRING_KV("diag_count", diagnose_info_arr_.count());

private:
  int inner_get_diagnose_info_idx(const char *mod_name, const char *sub_mod_name, int64_t &idx);

private:
  common::SpinRWLock lock_;
  common::ObSEArray<ObSSLocalCacheDiagnoseInfo, 16> diagnose_info_arr_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_STAT_H_ */
