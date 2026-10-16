/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_SS_BASE_PREWARMER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_SS_BASE_PREWARMER_H_
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
namespace oceanbase
{
namespace storage
{
struct ObSSPrewarmStat
{
public:
  ObSSPrewarmStat() { reset(); }

  void reset()
  {
    total_macro_count_ = 0;
    macro_block_num_ = 0;
    macro_block_bytes_ = 0;
    macro_block_fail_cnt_ = 0;
    macro_block_hit_cnt_ = 0;
    macro_block_add_cnt_ = 0;
    macro_cache_max_available_size_ = INT64_MAX;
  }

  void set_total_macro_count(const int64_t total_macro_count)
  {
    total_macro_count_ = total_macro_count;
  }
  int64_t get_total_macro_count() const { return total_macro_count_; }

  void update_macro_block_num_and_bytes(const int64_t delta_cnt, const int64_t delta_size)
  {
    macro_block_num_ += delta_cnt;
    macro_block_bytes_ += delta_size;
  }
  int64_t get_macro_block_num() const { return macro_block_num_; }
  int64_t get_macro_block_bytes() const { return macro_block_bytes_; }
  

  void inc_macro_block_fail_cnt()
  {
    macro_block_fail_cnt_++;
  }
  void inc_macro_block_hit_cnt(const int64_t delta = 1)
  {
    macro_block_hit_cnt_ += delta;
  }
  void inc_macro_block_add_cnt()
  {
    macro_block_add_cnt_++;
  }
  int64_t get_macro_block_hit_cnt() const { return macro_block_hit_cnt_; }
  int64_t get_macro_block_add_cnt() const { return macro_block_add_cnt_; }
  int64_t get_macro_block_fail_cnt() const { return macro_block_fail_cnt_; }
  int64_t get_macro_cache_max_available_size() const { return macro_cache_max_available_size_; }
  void set_macro_cache_max_available_size(const int64_t macro_cache_max_available_size)
  {
    macro_cache_max_available_size_ = macro_cache_max_available_size;
  }
  
  TO_STRING_KV(K(total_macro_count_), K(macro_block_num_), K(macro_block_bytes_),
      K(macro_block_fail_cnt_), K(macro_block_hit_cnt_), K(macro_block_add_cnt_),
      K(macro_cache_max_available_size_));

private:
  int64_t total_macro_count_; // Total number of macro blocks to process for the current task.
  int64_t macro_block_num_; // Number of macro blocks expected to be read from object storage.
  int64_t macro_block_bytes_; // Total macro block bytes actually read from object storage.
  int64_t macro_block_hit_cnt_; // Number of macro blocks already present in the local macro cache.
  int64_t macro_block_fail_cnt_; // Number of macro blocks that failed to be written to the local macro cache.
  int64_t macro_block_add_cnt_; // Number of macro blocks successfully added to the local macro cache.
  int64_t macro_cache_max_available_size_; // Maximum macro cache space available to the current task.
};

struct ObSSPrewarmLocalCacheType
{
  enum PrewarmLocalCacheType: uint8 //FARM COMPAT WHITELIST
  {
    SS_PREWARM_MACRO_CACHE = 0,
    SS_PREWARM_SKIPPED_TYPE = 1,
    SS_PREWARM_MAX_TYPE = 2
  };
static bool is_skipped(const PrewarmLocalCacheType local_cache_type)
{
  return (SS_PREWARM_SKIPPED_TYPE == local_cache_type);
}
static bool is_macro_cache(const PrewarmLocalCacheType local_cache_type)
{
  return (SS_PREWARM_MACRO_CACHE == local_cache_type);
}
};

typedef ObSSPrewarmLocalCacheType::PrewarmLocalCacheType PrewarmLocalCacheType;

class ObSSBasePrewarmer
{
public:
  ObSSBasePrewarmer() : stat_() {}
  virtual ~ObSSBasePrewarmer() 
  {
    reset();
  }
  void init();
  void reset();
  //virtual int write_to_local_cache(const blocksstable::MacroBlockId &macro_id, const char *macro_block_buf,
  //    const int64_t macro_block_size, const uint64_t effective_tablet_id, bool &is_skipped);
  //virtual int write_to_macro_cache(const blocksstable::MacroBlockId &macro_id, const char *macro_data, 
  //    const int64_t macro_size, const uint64_t effective_tablet_id) = 0;
  int open_micro_iterator(const blocksstable::MacroBlockId &macro_id, const char *macro_data,
      const int64_t macro_size, blocksstable::ObMicroBlockBareIterator &micro_iter);
  int check_prewarm_cache_type(const blocksstable::MacroBlockId &macro_id, const char *macro_data,
      const int64_t macro_size, const bool is_hot_tablet, PrewarmLocalCacheType &local_cache_type);
  int set_max_available_size_for_macro_cache(const PolicyStatus &policy_status);
  int set_max_available_size(const PolicyStatus &policy_status);
  const ObSSPrewarmStat &get_prewarm_stat() const { return stat_; }

protected:
  ObSSPrewarmStat stat_;

};
} // namespace storage  
} // namespace oceanbase
#endif
