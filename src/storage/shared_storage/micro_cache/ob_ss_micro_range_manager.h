/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_RANGE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_RANGE_MANAGER_H_


#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "lib/allocator/ob_small_allocator.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator;
}
namespace storage
{
struct ObSSMicroCacheStat;

/* Manage all micro_range_info, to serve micro_meta persisted function.
 *
 */
class ObSSMicroRangeManager
{
public:
  ObSSMicroRangeManager(ObSSMicroCacheStat &cache_stat, common::ObConcurrentFIFOAllocator &allocator);
  virtual ~ObSSMicroRangeManager() {}
  void destroy();
  int init(const uint64_t tenant_id, const uint32_t micro_ckpt_split_cnt);
  void clear();
  bool is_inited() const { return is_inited_; }
  bool is_valid_init_range_idx(const int64_t init_rng_idx) const;
  int64_t get_init_range_cnt() const { return init_range_cnt_; }
  uint32_t get_per_range_length() const { return per_range_len_; }
  ObSSMicroInitRangeInfo **get_init_range_arr() const { return init_range_arr_; }
  int get_range_cnt_per_ckpt_round(uint32_t &micro_ckpt_rng_cnt);
  int get_init_range_info(const int64_t init_range_idx, uint32_t &start_hval, uint32_t &end_hval);
  int get_total_range_micro_cnt(int64_t &total_micro_cnt);
  int get_total_sub_range_cnt(int64_t &total_sub_range_cnt);
  int insert_micro_meta_into_range(ObSSMicroBlockMetaHandle &micro_meta_handle);
  int create_sub_ranges(const uint32_t start_hval, const uint32_t end_hval, const int64_t sub_rng_cnt, 
                        ObSSMicroSubRangeInfo *&first_range_ptr, const bool retry_alloc);
  int create_sub_range(ObSSMicroSubRangeInfo *&range_ptr, const bool retry_alloc);
  int create_sub_range_bloom_filter(ObSSMicroSubRangeInfo *range_ptr, const int64_t item_cnt, const bool retry_alloc);
  void destroy_sub_range_bloom_filter(ObSSMicroSubRangeInfo *range_ptr);
  void destroy_sub_range(ObSSMicroSubRangeInfo *range_ptr);
  int try_free_sub_ranges(ObSSMicroSubRangeInfo *first_sub_range);
  // Clear all sub_ranges, and continue to clear the next sub_range even if some sub_ranges fail to free.
  // If any init_range is empty, will create and init it.
  int clear_all_sub_ranges();
  int calc_ckpt_exe_range_idx(const uint32_t micro_ckpt_split_cnt, const int64_t exe_idx, int64_t &start_range_idx, int64_t &end_range_idx);
  int get_init_range_by_hval(const uint32_t hval, ObSSMicroInitRangeInfo *&int_range);
  int clear_init_range_persisted_meta_info(const int64_t init_rng_idx, ObSSAllSegMetaInfo &all_seg_meta_info);

  // below funcs for test now
  int create_init_range(ObSSMicroInitRangeInfo *&range_ptr);

private:
  int inner_calc_initial_range_cnt(const int64_t micro_cnt_limit, const uint32_t micro_ckpt_split_cnt);
  int inner_build_initial_range();
  int inner_destroy_initial_range();
  int inner_get_range(const uint32_t hval, ObSSMicroSubRangeHandle &range_handle);
  int inner_get_initial_range(const uint32_t hval, ObSSMicroInitRangeInfo *&range_ptr);
  int64_t inner_get_initial_range_idx(const uint32_t hval) const;
  bool is_valid_hash_val(const uint32_t hval) const;
  bool is_valid_range_idx(const int64_t range_idx) const;

private:
  bool is_inited_;
  uint32_t init_range_cnt_;
  uint32_t per_range_len_; // determine one init_range's cover length, means [start, start+per_range_len-1]
  uint64_t tenant_id_;
  uint64_t min_hash_val_;
  uint64_t max_hash_val_;
  ObSSMicroInitRangeInfo **init_range_arr_;
  ObSSMicroCacheStat &cache_stat_;
  common::ObConcurrentFIFOAllocator &allocator_;
  common::ObSmallAllocator sub_rng_allocator_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_RANGE_MANAGER_H_ */
