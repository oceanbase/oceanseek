/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_H_

#include <stdint.h>
#include "lib/allocator/ob_small_allocator.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_runner.h"

namespace oceanbase
{
namespace storage
{
struct SSMicroMetaReqGuard
{
public:
  SSMicroMetaReqGuard(int64_t &flying_req_cnt) 
    : flying_req_cnt_(flying_req_cnt)
  { 
    ATOMIC_INC(&flying_req_cnt_); 
  }
  ~SSMicroMetaReqGuard() 
  {
    ATOMIC_DEC(&flying_req_cnt_);
  }

public:
  int64_t &flying_req_cnt_;
};

/* The basic entry of the whole shared_storage micro_block data cache.
 *
 * 1. include a micro_meta_manager, which owns a map(key is macro_block_id, value is
 *    a hashmap which will store this macro_block's micro_blocks' meta)
 * 
 * 2. include a physical_block_manager, which will manage all phy_blocks of cache_file.
 *    We can allocate or free block through this manager.
 *
 * 3. include a mem_data_manager, which will store micro_blocks' data into memory util
 *    the data_size is more than block_size. Then we will persist this data into phy_block.
 * 
 * 4. include a task_runner, which will run some background tasks, like persist_micro_data_task.
 */
class ObSSMicroCache
{
public:
  ObSSMicroCache();
  virtual ~ObSSMicroCache() {}
  static int mtl_init(ObSSMicroCache *&micro_cache);

  // if 'micro_ckpt_split_cnt' is set as 0, we will calculate a valid count based on meta_ckpt block count
  int init(const uint64_t tenant_id, const int64_t cache_file_size, 
           const uint32_t micro_ckpt_split_cnt = 0);
  int start();
  void stop();
  int wait();
  void destroy();

  ObSSPhysicalBlockManager &get_phy_block_mgr() { return phy_blk_mgr_; }
  ObSSMicroMetaManager &get_micro_meta_mgr() { return micro_meta_mgr_; }
  ObSSMicroRangeManager &get_micro_range_mgr() { return micro_range_mgr_; }
  common::ObSmallAllocator &get_meta_allocator() { return meta_allocator_; }
  ObSSMicroCacheTaskRunner &get_task_runner() { return task_runner_; }
  ObSSMicroCacheStat &get_micro_cache_stat() { return cache_stat_; }
  const ObSSMicroCacheAdminInfo &get_admin_info() const { return admin_info_; }
  uint32_t get_micro_ckpt_split_cnt() const { return micro_ckpt_split_cnt_; }

  // Check whether micro_cache is close to evict data
  int is_ready_to_evict_cache(bool &prepare_eviction);
  // Add new micro_block data into micro_cache.
  // Cuz it will be invoked in io_callback, to avoid cause io failure, we set the return type as 'void'
  int add_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const char *micro_data, const int32_t size,
                            const uint64_t effective_tablet_id, const ObSSMicroCacheAccessType access_type);
  // get micro_block data if it existed in cache. If cache miss, get it from object storage and add it into cache.
  int get_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroBlockId &phy_micro_id,
                            const ObSSMicroCacheGetType get_type, common::ObIOInfo &io_info,
                            blocksstable::ObStorageObjectHandle &obj_handle,
                            const ObSSMicroCacheAccessType access_type, bool &hit_cache);
  // delete micro_block from cache if it existed. We will mark it as deleted.
  int delete_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key);
  int get_not_exist_micro_blocks(const common::ObIArray<ObSSMicroPrewarmMeta> &in_micro_warmup_infos,
                                 common::ObIArray<ObSSMicroPrewarmMeta> &out_micro_warmup_infos);
  // check the micro_block exist in micro_cache or not. If not exist, return hit_type=CACHE_MISS
  int check_micro_block_exist(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_info,
                              ObSSMicroCacheHitType &hit_type);
  // update a batch of micro_blocks' arc_seg/heat
  int update_micro_block_heat(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
                              const bool transfer_seg/*need to transfer T1 -> T2*/,
                              const bool update_access_time/*need to update access_time to current_time*/,
                              const int64_t time_delta_s = 120/*default 120s*/);
  int resize_micro_cache_file_size(const int64_t new_file_size);
  int get_batch_la_micro_keys(ObLS *ls, common::ObIArray<ObSSMicroPrewarmMeta> &keys);
  int is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id, bool &is_filter);

  int divide_phy_block_range(const share::ObLSID &ls_id, const int64_t split_count,
                             common::ObIArray<ObSSPhyBlockIdxRange> &block_ranges);
  int update_effective_tablet_id(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr, 
                                 const common::ObTabletID &effective_tablet_id);
  int read_checkpoint();
  /*-----------------------------------------For prewarm-----------------------------------------*/
  void begin_free_space_for_prewarm();
  void finish_free_space_for_prewarm();
  int get_available_space_for_prewarm(int64_t &available_size);
  // Computes how much the micro cache file needs to be expanded for LS migration.
  //
  // @param src_ls_micro_cache_info  Micro-cache usage of the source LS (total size and hot size).
  // @param min_expand_size          Minimal extra space (in bytes) required on the destination
  //                                 to cache all hot data from the source LS, assuming cold data
  //                                 on both the source and the destination may be discarded.
  // @param best_expand_size         Extra space (in bytes) required to cache all data (hot and
  //                                 cold) from the source LS without discarding any source data,
  //                                 which preserves the best post-migration cache performance.
  int cal_expand_size_for_ls_migration(
    const ObSSMicroCacheLSInfo &src_ls_micro_cache_info,
    int64_t &min_expand_size, int64_t &best_expand_size);
  // this function is designed for prewarm, which can specify max_retry_times and 
  // whether need to transfer micro_block cache from T1 to T2.
  int add_micro_block_cache_for_prewarm(const ObSSMicroBlockCacheKey &micro_key, const char *micro_data, 
                                        const int32_t size, const uint64_t effective_tablet_id,
                                        const ObSSMicroCacheAccessType access_type,
                                        const int64_t max_retry_times, const bool transfer_seg);
  void update_hot_micro_lack_count(const bool full_scan, const int64_t lack_count);
  // Returns the total size (in bytes) of total data and user-designated hot data currently
  // cached in the micro cache.
  int get_data_cached_size(int64_t &data_cached_size, int64_t &hot_data_cached_size);
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSMicroCacheLSInfo &ls_cache_info);
  int get_tablet_cache_info(const common::ObTabletID &tablet_id, ObSSMicroCacheTabletInfo &tablet_cache_info);
  int get_phy_block_handle(const int64_t phy_blk_idx, ObSSPhyBlockHandle &phy_blk_handle);
  int get_phy_block_size(int64_t &phy_blk_size);
  /*-----------------------------------------For Operation/Maintenance-----------------------------------------*/
  bool is_normal_state() const;
  void enable_cache();
  void disable_cache();
  bool is_cache_enabled() const;
  void enable_write();
  void disable_write();
  bool is_write_allowed() const;
  void enable_read();
  void disable_read();
  bool is_read_allowed() const;
  void enable_calibration();
  void disable_calibration();
  bool is_calibration_enabled() const;
  bool is_operating() const { return ATOMIC_LOAD(&is_operating_); }
  bool try_start_operating();
  void finish_operating() { ATOMIC_STORE(&is_operating_, false); }
  // Wait for flying add requests to finish. Requires disable_write() to be called beforehand;
  // otherwise new requests may arrive after the wait returns, making the result unreliable.
  int wait_for_add_requests_finish(const int64_t timeout_us = 0);
  // Prepare for calibration: enable_read, disable_write, wait add requests, pause and wait bg tasks
  int start_calibration();
  // Finish calibration: release is_operating; on success enable bg tasks and write, clear abnormal flag.
  void finish_calibration(const bool calibration_succeeded);
  // for_replay: set to true when called from the replay checkpoint failure path (meta_ckpt timer thread).
  // In that case, release_cache_task and calibration_task share the same timer thread and cannot run, so we skip
  // waiting for them to close to avoid deadlock.
  int clear_micro_cache(bool for_replay = false);
  int64_t get_max_micro_block_size();
  int get_phy_block_info(const int64_t phy_blk_idx, ObSSPhysicalBlock& phy_blk_info);
  int get_micro_meta_info(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_meta_info);
  int get_micro_cache_info(ObSSMicroCacheStat &cache_stat, ObSSMicroCacheSuperBlk &super_blk, ObSSARCInfo &arc_info);
  int clear_micro_meta_by_tablet_id(const common::ObTabletID &tablet_id);
  void set_micro_meta_compressor_type(const common::ObCompressorType type);
  common::ObCompressorType get_micro_meta_compressor_type() const;
  int64_t get_micro_cache_size() const { return ATOMIC_LOAD(&cache_file_size_); }

private:
  int read_or_format_super_block();
  int read_phy_block_checkpoint(const int64_t blk_entry, const bool reserve_ckpt_info);
  int read_micro_meta_checkpoint(const common::ObIArray<ObSSMicroCkptEntryItem> &blk_entries, 
                                 const int64_t micro_ckpt_time_us,
                                 const bool reserve_ckpt_info);
  // Only get micro_block data from cache, if cache miss, won't get micro_block data.
  // This function is only used for test.
  int get_cached_micro_block(const ObSSMicroBlockCacheKey &micro_key, common::ObIOInfo &io_info, 
                             blocksstable::ObStorageObjectHandle &obj_handle,
                             const ObSSMicroCacheAccessType access_type);
  // Get micro_block data from micro_cache(in memory or in disk) if cache hit.
  // If cache miss, it will send io_request to fetch data from remote and resave back to micro_cache.
  int inner_get_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroBlockId &phy_micro_id,
                                  const ObSSMicroCacheGetType get_type, common::ObIOInfo &io_info,
                                  blocksstable::ObStorageObjectHandle &obj_handle,
                                  const ObSSMicroCacheAccessType access_type, bool &hit_cache);
  int inner_get_micro_block_handle(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_info,
                                   ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle, 
                                   ObSSPhyBlockHandle &phy_blk_handle, ObSSMicroCacheHitType &hit_type, 
                                   const uint64_t effective_tablet_id, const bool update_arc);
  int simulate_io_result(const ObSSMicroCacheGetType get_type, const blocksstable::MacroBlockId &macro_id,
                         common::ObIOInfo &io_info, blocksstable::ObStorageObjectHandle &obj_handle) const;
  int inner_update_free_space_for_prewarm(const bool is_start_op);
  void inner_update_micro_ckpt_split_cnt(const int64_t cache_file_size, const bool for_init);
  int push_la_micro_key(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroCacheHitType &hit_type, 
                        const ObSSMicroBlockMetaInfo &micro_info);
  void inner_check_micro_meta_cnt(); // used when destory or clear micro_cache
  int inner_handle_after_resize(const int64_t new_file_size);

  int get_cur_available_space_for_prewarm_(int64_t &available_size);

private:
  bool is_inited_;
  bool is_stopped_;
  bool is_enabled_; // enable/disable micro_cache
  bool is_operating_; // clear_micro_cache、resize、calibration
  bool is_write_allowed_;
  bool is_read_allowed_;
  bool enable_calibration_; // enable/disable calibrating, default: true
  int32_t phy_blk_size_;
  uint64_t tenant_id_;
  int64_t cache_file_size_;
  int64_t flying_add_req_cnt_; // the count of flying add request which will access micro_meta_map
  int64_t flying_get_req_cnt_; // the count of flying get request which will access micro_meta_map
  uint32_t micro_ckpt_split_cnt_; // determine how may batchs a complete round of micro_meta ckpt will be splitted into
  ObSSMicroCacheStat cache_stat_;
  ObSSMicroCacheAdminInfo admin_info_;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObSmallAllocator meta_allocator_;
  ObSSPhysicalBlockManager phy_blk_mgr_;
  ObSSMemBlockManager mem_blk_mgr_;
  ObSSMicroRangeManager micro_range_mgr_;
  ObSSMicroMetaManager micro_meta_mgr_;
  ObSSLAMicroKeyManager la_micro_key_mgr_;
  ObSSMicroCacheTaskRunner task_runner_;
};

#define SSPhyBlockMgr (MTL(ObSSMicroCache *)->get_phy_block_mgr())
#define SSMicroMetaMgr (MTL(ObSSMicroCache *)->get_micro_meta_mgr())
#define SSMicroRangeMgr (MTL(ObSSMicroCache *)->get_micro_range_mgr())
#define SSMicroCacheStat (MTL(ObSSMicroCache *)->get_micro_cache_stat())
#define SSMicroMetaAlloc (MTL(ObSSMicroCache *)->get_meta_allocator())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_H_ */
