/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

int ObSSMicroCacheTaskStat::get_stuck_task_names(const int64_t timeout_us, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    int64_t pos = 0;
    const int64_t now_us = ObTimeUtility::current_time_us();
    bool need_comma = false;  // add “,” before item when a previous item was output
    for (int64_t i = 1; i < SS_MICRO_CACHE_TASK_OP_TYPE_CNT; ++i) {
      const int64_t start_time_us = ATOMIC_LOAD(&task_start_time_us_arr_[i]);
      if (start_time_us > 0 && (now_us - start_time_us > timeout_us)) {
        const char *name = SS_MICRO_CACHE_TASK_OP_NAME_ARRAY[i];
        if (need_comma) {
          IGNORE_RETURN(common::databuff_printf(buf, buf_len, pos, ","));
        }
        IGNORE_RETURN(common::databuff_printf(buf, buf_len, pos, "%s", name));
        need_comma = true;
      }
    }
  }
  return ret;
}

// NOTICE:
//    We only need to serialize these structs when we use ob_admin to get micro_cache_stat. Thus, 
//    we will mark these structs as 'FARM COMPAT WHITELIST', cuz it doesn't matter if there exists
//    some change.

OB_SERIALIZE_MEMBER(ObSSMicroCacheIOStatParam, // FARM COMPAT WHITELIST
    add_cnt_, add_bytes_, get_cnt_, get_bytes_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheIOStat, // FARM COMPAT WHITELIST
    common_io_param_, prewarm_io_param_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheHitStat, // FARM COMPAT WHITELIST
    cache_hit_cnt_, cache_hit_bytes_, cache_miss_cnt_, cache_miss_bytes_, 
    fail_get_cnt_, fail_add_cnt_, new_add_cnt_, hot_micro_lack_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCachePrewarmStat, // FARM COMPAT WHITELIST
    new_add_cnt_, fail_add_cnt_, major_compaction_pwm_stat_, migration_pwm_stat_,
    replica_pwm_stat_, ddl_pwm_stat_, scp_pwm_stat_, minor_compaction_pwm_stat_,
    preread_task_pwm_stat_, split_part_pwm_stat_, try_write_lcache_pwm_stat_);

OB_SERIALIZE_MEMBER(ObSSMicroCachePhyBlkStat, // FARM COMPAT WHITELIST
    block_size_, cache_file_size_, total_blk_cnt_, super_blk_cnt_, reusable_blk_cnt_, sparse_blk_cnt_, 
    phy_ckpt_blk_cnt_, phy_ckpt_blk_used_cnt_, meta_blk_cnt_, meta_blk_used_cnt_, data_blk_cnt_, 
    data_blk_used_cnt_, shared_blk_cnt_, shared_blk_used_cnt_, reorgan_blk_used_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheMemBlkStat, // FARM COMPAT WHITELIST
    mem_blk_fg_max_cnt_, mem_blk_fg_used_cnt_, mem_blk_bg_max_cnt_, mem_blk_bg_used_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheMicroStat,  // FARM COMPAT WHITELIST 
    total_micro_cnt_, total_micro_size_, valid_micro_cnt_, valid_micro_size_, mark_del_micro_cnt_, 
    mark_del_micro_size_, mark_del_ghost_micro_cnt_, mark_del_ghost_micro_size_, pending_delete_tablet_cnt_,
    max_micro_meta_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheRangeStat, // FARM COMPAT WHITELIST
    init_range_cnt_, sub_range_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheTaskStat, // FARM COMPAT WHITELIST
    is_mini_mode_, reorgan_invoke_time_, reorgan_cnt_, reorgan_free_blk_cnt_, t1_evict_cnt_, t2_evict_cnt_,
    b1_delete_cnt_, b2_delete_cnt_, blk_ckpt_cnt_, blk_ckpt_item_cnt_, micro_ckpt_op_cnt_, micro_ckpt_cnt_,
    micro_ckpt_op_item_cnt_, micro_ckpt_item_cnt_, phy_blk_min_len_, expired_cnt_, expired_ghost_cnt_, 
    force_erase_cnt_, force_erase_ghost_cnt_, erase_persisted_cnt_, readd_persisted_cnt_, dropped_persisted_cnt_,
    entry_exist_persisted_cnt_, abnormal_flag_, calibration_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheMemoryStat, // FARM COMPAT WHITELIST
    micro_alloc_cnt_, micro_cnt_limit_, micro_meta_usage_, micro_map_usage_, phy_blk_usage_, mem_blk_usage_,
    micro_range_usage_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheStat, // FARM COMPAT WHITELIST
    io_stat_, hit_stat_, prewarm_stat_, phy_blk_stat_, mem_blk_stat_, micro_stat_, range_stat_, task_stat_,
    mem_stat_);
} /* namespace storage */
} /* namespace oceanbase */
