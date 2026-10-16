/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_

#include <stdint.h>
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_arc_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator;
}
namespace storage
{
struct SSMicroMapGetMicroHandleFunc;
struct ObSSMicroCacheStat;
class ObSSCkptPhyBlockItemReader;
class ObSSMicroRangeManager;
class ObLSHandle;

struct SSTryLoadPersistedMicroMetaGuard
{
public:
  SSTryLoadPersistedMicroMetaGuard(int &ret, bool &all_mem_meta,
    ObSSMicroInitRangeInfo *init_range, ObSSMicroSubRangeInfo *sub_range);
  ~SSTryLoadPersistedMicroMetaGuard();

public:
  int &ret_;
  bool &all_mem_meta_;
  ObSSMicroInitRangeInfo *init_range_;
  ObSSMicroSubRangeInfo *sub_range_;
  
private:
  int start_load_meta();
  int finish_load_meta();
};

class ObSSMicroMetaManager
{
public:
  typedef common::ObLinearHashMap<const ObSSMicroBlockCacheKey *, ObSSMicroBlockMetaHandle, UniqueMemMgrTag> SSMicroMetaMap;

  ObSSMicroMetaManager(ObSSMicroCacheStat &cache_stat, common::ObConcurrentFIFOAllocator &allocator);
  virtual ~ObSSMicroMetaManager() {}

  bool is_inited() const { return is_inited_; }
  bool is_mem_limited() const { return ATOMIC_LOAD(&mem_limited_); }
  bool check_mem_limit(); // if reach mem_limit, return true
  const ObSSARCInfo &get_arc_info() const { return arc_info_; }
  void reset_arc_seg_info() { arc_info_.reset_seg_info(); }
  int64_t get_cache_expiration_time() const { return ATOMIC_LOAD(&expiration_time_s_); }
  int64_t get_arc_limit_pct() const { return ATOMIC_LOAD(&arc_limit_pct_); }
  int64_t get_max_micro_block_size() const { return ATOMIC_LOAD(&max_micro_blk_size_); }
  bool enable_save_meta_memory() const { return enable_save_meta_mem_; }
  uint64_t get_micro_meta_map_cnt() const { return micro_meta_map_.count(); }

  int init(const uint64_t tenant_id, const int32_t block_size, const int64_t cache_limit_size,
           ObSSMicroRangeManager &micro_range_mgr);
  void destroy();
  void clear();
  int update_arc_limit(const int64_t new_cache_limit_size);
  int update_arc_work_limit_for_prewarm(const bool start_update);
  void update_cache_mem_limit_by_config();
  void update_cache_expiration_time_by_config();
  void update_arc_limit_pct_by_config(bool &succ_update);
  void update_max_micro_blk_size_by_config();
  // Link micro_meta into sub_range and update ARC seg info. Used by calibration.
  int link_meta_and_update_arc(const ObSSMicroBlockMetaInfo &micro_meta_info, ObSSMicroBlockMetaHandle &micro_meta_handle);
  int adjust_arc_seg_info(const bool is_in_l1, const bool is_in_ghost, const ObSSMicroArcOpType &op_type,
                          const int32_t delta_size, const int32_t delta_cnt);
  int alloc_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle);
  int add_or_update_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const int32_t micro_size, 
                                     const uint32_t micro_crc, const uint64_t effective_tablet_id,
                                     ObSSMemBlockHandle &mem_blk_handle, bool &is_first_add,
                                     const ObSSMicroCacheAccessType access_type = ObSSMicroCacheAccessType::COMMON_IO_TYPE);
  
  // If the micro_meta exists and is valid, return the micro_meta_handle
  int get_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle,
                           const uint64_t effective_tablet_id, const bool update_arc);
  // Get the micro_block_meta handle if exists in micro_cache. Else return OB_ENTRY_NOT_EXIST
  // Cuz the upper layer needs to read micro_block data later, thus we need to hold the mem_blk
  // or phy_blk handle. A micro_block's data will be cached in a mem_block or a phy_block.
  int get_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_info,
                           ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle,
                           ObSSPhyBlockHandle &phy_blk_handle, const uint64_t effective_tablet_id, const bool update_arc);
  int get_micro_meta_info(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_info);
  int get_micro_meta_info_for_serialize(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroRangeScanFilter &filter,
                                        ObSSMicroBlockMetaInfo &micro_info, bool &need_serialize);
  // To support prewarm, update micro_meta's arc_seg and access_time
  int update_micro_block_meta_heat(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_keys, const bool transfer_seg, 
                                   const bool update_access_time, const int64_t time_delta_s);
  // For one arc_seg(T1/T2/B1/B2), choose some cold micro_meta to evict or delete
  int acquire_cold_micro_blocks(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx);
  // To evict micro_meta based on ARC algo
  int try_evict_micro_block_meta(const ObSSMicroBlockMetaInfo &cold_micro_info);
  // To delete micro_meta based on ARC algo(etc. Mark Deleted)
  int try_delete_micro_block_meta(const ObSSMicroBlockMetaInfo &cold_micro_info);
  // If some micro_meta of this mem_blk fail to persist, need to delete them(etc. Mark Deleted)
  int try_delete_micro_block_meta(ObSSMemBlockHandle &sealed_mem_blk_handle);
  // To force delete micro_meta(eg. we need to remove micro_meta by tablet_id、remove ckm_error micro_meta)(etc. Mark Deleted)
  int try_force_delete_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key);
  // To erase micro_meta from micro_meta_map if it is marked deleted
  int try_erase_deleted_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key);
  // To erase expired micro_meta from micro_meta_map
  int try_erase_expired_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const int64_t expiration_time_s);
  // To erase micro_meta from micro_meta_map if it is in T1/T2 but invalid
  int try_erase_abnormal_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key);
  // To force erase micro_meta when execute micro_ckpt(eg. if this micro's tablet_id is abnormal, means not relate to ls)
  int try_force_erase_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const uint64_t tablet_id);
  // To update micro_meta if this micro_block succ to be reorganized
  int try_update_reorganizing_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key,
                                               const uint64_t exp_micro_data_loc,
                                               const uint32_t exp_micro_crc,
                                               const uint64_t exp_reuse_version,
                                               ObSSMemBlockHandle &mem_blk_handle,
                                               bool &succ_update);
  // After persist micro_block data, need to update micro_blocks' meta: location, is_persisted, etc.
  int update_persisted_micro_block_meta(ObSSMemBlockHandle &sealed_mem_blk_handle, const int64_t block_offset,
                                        const uint32_t reuse_version, int64_t &updated_micro_size, 
                                        int64_t &update_micro_cnt);
  // To mark all micro_meta of this tablet as deleted, thus they will be deleted by bg task
  int clear_tablet_micro_block_meta(const common::ObTabletID &tablet_id, int64_t &remain_micro_cnt);
   // Record tablet_id and micro_ckpt_op_cnt_ for deferred deletion during meta ckpt
  int record_need_delete_tablet(const common::ObTabletID &tablet_id, const int64_t micro_ckpt_op_cnt);
  // Check if tablet_id need to be deleted
  int is_tablet_need_delete(const common::ObTabletID &tablet_id, bool &need_delete) const;
  // Check and clean up completed delete records after meta ckpt
  int check_and_cleanup_pending_delete_tablets(const int64_t cur_micro_ckpt_op_cnt, const int64_t meta_ckpt_split_cnt);
  // To iterate micro_meta according to micro_range.
  int scan_ranges_micro_block_meta(ObSSMCTabletInfoMap &tablet_cache_map, const common::ObIArray<ObLSHandle> &ls_handles, 
                                   const int64_t start_range_idx, const int64_t end_range_idx, int64_t &handled_range_idx,
                                   bool &is_empty_range);
  // When restart, can read persisted micro_meta and load them into memory
  int read_micro_meta_checkpoint(ObSSCkptPhyBlockItemReader &item_reader, const int64_t micro_ckpt_time_us,
                                 common::ObIArray<int64_t> &ckpt_blk_id_list, bool need_reset_stat = true);
  // Check if the persisted micro_metas of a sub_range need to be erased from micro_meta_map to free memory
  int try_erase_persisted_micro_metas(ObSSMicroSubRangeInfo *sub_range, 
                                      const common::ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr,
                                      int64_t &force_erase_cnt);
  // When ghost micro_meta is not enough to evict, we can choose sub_ranges to execute evict 
  int try_delete_ghost_persisted_micro_meta(ObSSMicroInitRangeInfo *init_range, ObSSMicroSubRangeInfo *sub_range,
                                            const bool is_in_l1, int64_t &delete_ghost_cnt);
  // Try to update the persisted micro_meta's info, like 'is_meta_persisted、is_meta_serialized、etc.'
  int try_update_persisted_micro_meta(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_key_arr);
  // Try to update effective tablet_id in ss_micro_meta
  int update_effective_tablet_id(const ObSSMicroBlockCacheKey &micro_key, const common::ObTabletID &effective_tablet_id,
                                 bool &succ_update);
  // NOTE: Uses micro_meta_map_.remove_if; may erase metas during calibration, not only in meta_ckpt path.
  int traverse_meta_for_calibration(common::ObArray<int64_t> &expected_valid_len);
  int update_micro_stat_from_arc();

private:
  void set_mem_limited(const bool mem_limited) { ATOMIC_STORE(&mem_limited_, mem_limited); }
  int inner_update_arc_limit_pct(const uint64_t tenant_id, bool &succ_update);
  int inner_update_cache_expiration_time(const uint64_t tenant_id);
  int inner_update_mem_limit_size(const uint64_t tenant_id);
  int inner_update_arc_limit(const int64_t cache_limit_size);
  int inner_update_max_micro_blk_size(const uint64_t tenant_id);
  int init_arc_info(const uint64_t tenant_id, const int64_t cache_limit_size, const int64_t micro_cnt_limit);
  int inner_adjust_arc_seg_info(const bool is_in_l1, const bool is_in_ghost, const ObSSMicroArcOpType &op_type,
                                const int32_t delta_size, const int32_t delta_cnt);
  int inner_update_tablet_cache_map(ObSSMCTabletInfoMap &tablet_cache_map, const ObSSMicroBlockCacheKey &micro_key,
                                    const ObSSMicroBlockMetaInfo &micro_info, const common::ObIArray<ObLSHandle> &ls_handles,
                                    bool &need_erase);
  int inner_get_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaInfo &micro_info,
                                 SSMicroMapGetMicroHandleFunc &get_func, const bool need_update_tablet_id, 
                                 const uint64_t effective_tablet_id);
  int inner_get_micro_block_meta_directly(const ObSSMicroBlockCacheKey &micro_key,
                                          SSMicroMapGetMicroHandleFunc &get_func,
                                          ObSSMicroBlockMetaInfo &micro_info);
  // Whether micro_key has an entry in micro_meta_map_ only. 
  // NOTE: do not check on meta validity (deleted, etc.).
  int check_micro_key_exist(const ObSSMicroBlockCacheKey &micro_key);
  int do_add_or_update_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const int32_t micro_size, 
                                        const uint32_t micro_crc, const uint64_t effective_tablet_id,
                                        ObSSMemBlockHandle &mem_blk_handle, bool &is_first_add,
                                        const ObSSMicroCacheAccessType access_type);
  int do_add_persisted_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle);
  int inner_add_ckpt_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle);
  int inner_update_phy_blk_valid_len(const ObSSMicroBlockMetaInfo &micro_meta_info);
  int inner_obtain_persisted_micro_metas(ObSSMicroInitRangeInfo *init_range,
                                         ObSSMicroSubRangeInfo *sub_range,
                                         common::ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos);
  int inner_add_persisted_micro_metas(const common::ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos,
                                      const int64_t micro_info_idx, ObSSMicroBlockMetaHandle &micro_meta_handle,
                                      ObSSMicroBlockMetaInfo &micro_meta_info);
  int inner_add_persisted_micro_metas(const common::ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos);
  double inner_calc_deleted_micro_pct() const;
  int handle_init_range(const int64_t init_range_idx, ObSSMicroInitRangeInfo *init_range, 
                        ObSSMCTabletInfoMap &tablet_cache_map, const common::ObIArray<ObLSHandle> &ls_handles, 
                        bool &empty_range);
  int handle_init_range_micro_metas(const int64_t init_range_idx, ObSSMicroInitRangeInfo *init_range,
                                    const int64_t init_rng_micro_cnt, ObSSMCTabletInfoMap &tablet_cache_map,
                                    const common::ObIArray<ObLSHandle> &ls_handles, ObSSMicroSubRangeInfo *&new_range_header, 
                                    ObSSMicroSubRangeInfo *&old_range_header, bool &empty_range);
  int create_new_sub_ranges(const int64_t init_range_idx, ObSSMicroInitRangeInfo *init_range, const int64_t init_rng_micro_cnt, 
                            const double del_micro_pct, ObSSMicroSubRangeInfo *&new_range_header, int64_t &sub_rng_cnt);
  int handle_sub_range_micro_metas(ObSSMicroSubRangeInfo *new_range_header, ObSSMicroBlockMeta *first_micro_meta,
                                   ObSSMCTabletInfoMap &tablet_cache_map, const common::ObIArray<ObLSHandle> &ls_handles,
                                   int64_t &scanned_micro_cnt, int64_t &remained_micro_cnt);
  int check_need_erase_micro_meta(const ObSSMicroBlockMetaInfo &cur_micro_info, ObSSMicroRangeScanFilter &tablet_cache_filter,
                                  ObSSMCTabletInfoMap &tablet_cache_map, const common::ObIArray<ObLSHandle> &ls_handles, 
                                  bool &succ_erase);
  int rebuild_initial_range(ObSSMicroInitRangeInfo *init_range, ObSSMicroSubRangeInfo *&new_range_header, bool &empty_range);
  int handle_inc_sub_range(ObSSMicroInitRangeInfo *init_range, ObSSMicroSubRangeInfo *&inc_sub_range, bool &empty_range);
  int free_tmp_sub_ranges(ObSSMicroSubRangeInfo *&first_sub_range);
  int try_load_persisted_micro_meta(const ObSSMicroBlockCacheKey &micro_key, SSMicroMapGetMicroHandleFunc &get_func,
                                    ObSSMicroBlockMetaInfo &micro_meta_info);

private:
  static const int64_t TABLETS_DELETE_BUCKET_NUM = 16;
  static const int64_t DEFAULT_TABLETS_NUM = 16;
  static const int32_t MASSIVE_SCAN_MICRO_CKPT_CNT = 999; // if scan count is more than this val, not scan another hash_range anymore.

private:
  bool is_inited_;
  bool mem_limited_;
  bool enable_save_meta_mem_; // true: erase some persisted micro_meta to save memory, when needed, read it from disk
  int32_t block_size_;
  uint64_t tenant_id_;
  int64_t micro_cnt_limit_;
  int64_t mem_limit_size_;
  int64_t expiration_time_s_;
  int64_t arc_limit_pct_;
  int64_t max_micro_blk_size_;  // the maximum size (in bytes) of a micro block that can be added to micro cache
  // NOTE: Besides meta_ckpt can erase meta, calibration can also erase via remove_if. 
  SSMicroMetaMap micro_meta_map_;
  SSMicroMetaMap::BlurredIterator bg_micro_iter_; // only used for evict_task
  SSMicroMetaMap::BlurredIterator fg_micro_iter_;
  ObSSARCInfo arc_info_;
  ObSSMicroRangeManager *micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat_;
  ObSSReplayMicroCkptCtx replay_ctx_;
  common::ObConcurrentFIFOAllocator &allocator_;
  common::hash::ObHashMap<common::ObTabletID, int64_t> tablets_to_delete_;  // Map to store tablet_id pending for deletion and corresponding micro_ckpt_op_cnt_
                                                                            // Key: tablet_id, Value: micro_ckpt_op_cnt_ when delete was requested
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_ */
