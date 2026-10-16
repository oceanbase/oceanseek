/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_

#include "lib/container/ob_bitmap.h"
#include "lib/container/ob_2d_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_info.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator;
}
namespace storage
{
struct ObSSMicroCacheStat;
class ObSSCkptPhyBlockItemReader;

/* Manage all blocks of micro_cache_file, to alloc and free block, to manage block info.
 *
 * 1. The first/second block(phy_blk_idx = 0/1) are used to store micro_cache super_block interchangeably.
 * 2. We won't use all blocks to store micro_block_data as cache. We will reserve some blocks.
 */
class ObSSPhysicalBlockManager
{
public:
  ObSSPhysicalBlockManager(ObSSMicroCacheStat &cache_stat, common::ObConcurrentFIFOAllocator &allocator);
  virtual ~ObSSPhysicalBlockManager() {}
  void destroy();
  int init(const uint64_t tenant_id, const int64_t cache_file_size, const int32_t block_size);
  bool is_inited() const { return is_inited_; }
  void clear();
  // Check if any phy_blk_handle is held (ref_cnt_ > 1).
  // If any phy_blk_handle is held, wait until all phy_blk_handle are released or timeout.
  void check_phy_block_safety();
  int32_t get_block_size() const { return block_size_; }
  double get_blk_reorgan_pct() const { return phy_blk_reorgan_pct_; }
  int64_t get_data_block_used_cnt() const;
  int64_t get_data_block_max_cnt() const;
  int64_t get_meta_block_min_cnt() const;
  int64_t get_cache_limit_size() const;
  int64_t get_total_blk_cnt() const;
  int get_and_update_cache_limit_size(const bool need_update_reorgan_pct = false);
  void get_data_block_info(int64_t &data_block_max_cnt, int64_t &data_blk_usage_pct) const;
  void get_blk_ckpt_block_info(int64_t &blk_ckpt_cnt, int64_t &blk_cnpt_used_cnt) const;
  bool exist_free_block(const ObSSPhyBlockType blk_type) const;
  int resize_file_size(const int64_t new_file_size, const int32_t block_size);
  void adjust_micro_meta_block();
  void reserve_micro_meta_block(int64_t &available_cnt);
  void reserve_reorganize_block(int64_t &available_cnt);
  void check_all_blocks_length();
  void check_blocks_info(common::ObIArray<int64_t> &blk_list);

  int alloc_block(int64_t &blk_idx, ObSSPhyBlockHandle &phy_blk_handle, const ObSSPhyBlockType blk_type);
  int free_block(const int64_t blk_idx, bool &succ_free);
  int get_block_handle(const uint64_t data_loc, const uint64_t exp_reuse_version, ObSSPhyBlockHandle &phy_blk_handle,
                       uint64_t &real_reuse_version);
  int get_block_handle(const int64_t phy_blk_idx, ObSSPhyBlockHandle &phy_blk_handle);
  int get_block_info(const int64_t blk_idx, ObSSPhysicalBlock& phy_blk_info);

  int scan_blocks_to_reorgan();
  int scan_blocks_to_reuse();
  int scan_blocks_to_ckpt(common::ObIArray<ObSSPhyBlockReuseInfo> &reuse_info_arr);
  int scan_reusable_ckpt_blocks_to_free(bool &exist_reusable_ckpt_blks);
  int update_block_valid_length(const uint64_t data_loc, const uint64_t reuse_version, 
                                const int32_t delta_len, int64_t &blk_idx);
  int read_phy_block_checkpoint(ObSSCkptPhyBlockItemReader &item_reader, common::ObIArray<int64_t> &ckpt_blk_id_list);
  int update_block_state_for_restart();
  int divide_cache_data_block_range(const share::ObLSID &ls_id, const int64_t split_count,
                                    common::ObIArray<ObSSPhyBlockIdxRange> &blk_idx_ranges);

  int try_free_batch_blocks(const common::ObIArray<int64_t> &blk_idx_arr, int64_t &free_cnt);
  int rebuild_phy_blk_valid_len(const common::ObIArray<int64_t> &expected_valid_len);
  int delete_reusable_block(const int64_t blk_idx);
  int add_reusable_block(const int64_t blk_idx);
  int add_batch_reusable_blocks(const common::ObIArray<int64_t> &blk_idx_arr);
  int get_batch_reusable_blocks(common::ObIArray<int64_t> &blk_idx_arr, const int64_t max_cnt);
  int64_t get_reusable_blocks_cnt() const { return reusable_blks_.size(); }
  int64_t get_sparse_block_cnt() const { return sparse_blk_map_.count(); }
  int add_sparse_block(const ObSSPhyBlockIdx &blk_idx);
  int delete_sparse_block(const ObSSPhyBlockIdx &blk_idx);
  int get_batch_sparse_blocks(common::ObIArray<ObSSPhyBlkReorganEntry> &sparse_blks, 
                              const int64_t max_reorgan_cnt);
  int update_compressor_type(const common::ObCompressorType new_type);
  int get_blk_ckpt_info(ObSSBlkCkptInfo &blk_ckpt_info);
  int get_micro_ckpt_info(ObSSMicroCkptInfo &micro_ckpt_info);
  int format_ss_super_block(const uint32_t micro_ckpt_split_cnt);
  int read_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk);
  int get_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk);
  int reformat_ss_super_block(const int64_t micro_ckpt_split_cnt);
  int update_ss_super_block(ObSSMicroCacheSuperBlk &new_super_blk, const bool do_check = false,
                            const int64_t exp_modify_time = 0);
  int update_ss_super_block_for_blk_ckpt(const ObSSMicroCacheSuperBlk &ckpt_super_blk);
  int update_ss_super_block_for_micro_ckpt(const int64_t idx, const ObSSMicroCacheSuperBlk &ckpt_super_blk);
  int exist_in_ss_super_block_ckpt_list(const int64_t blk_idx, bool &is_exist);
  int read_persisted_sub_range(char *buf, const int64_t phy_blk_idx, const int64_t offset, const int64_t size,
                               const uint64_t reuse_version);
  int reuse_meta_blks_on_meta_ckpt_reset(const uint32_t micro_ckpt_split_cnt);

  // when replay ckpt, we may need to update super_block's blk/micro ckpt used_block list
  int update_blk_ckpt_used_block_list(const common::ObIArray<int64_t> &blk_list);
  int update_micro_ckpt_used_block_list(const int64_t idx, const common::ObIArray<int64_t> &blk_list);
  void allow_alloc_all_blk();
  void forbid_alloc_all_blk();

private:
  int init_phy_block_array(const uint64_t tenant_id, const int64_t blk_cnt);
  int init_free_bitmap(const uint64_t tenant_id, const int64_t blk_cnt, const bool first_init);
  int init_block_count(const int64_t cache_file_size, const int32_t block_size, const bool is_mini_cache);
  bool is_valid_block_idx(const int64_t blk_idx) const;
  ObSSPhysicalBlock *inner_get_phy_block_by_idx(const int64_t blk_idx);
  int do_resize_file_size(const int64_t new_file_size, const int32_t block_size);
  int inner_write_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk, const bool is_format, const bool allow_failed = false);
  int inner_do_write_ss_super_block(const int64_t blk_offset, const int64_t align_size, const char *buf, int64_t &write_size);
  int inner_read_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk);
  // if assgin ss_super_blk with read result, set has_updated to true, else set has_updated to false
  int inner_build_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk, char *read_buf, const int64_t read_size, bool &is_blk_updated);
  int inner_get_ss_super_block(ObSSMicroCacheSuperBlk &ss_super_blk);
  int inner_check_ss_super_block();
  int inner_set_ckpt_blk_type();
  void set_phy_blk_stat(const int64_t cache_file_size);
  bool inner_can_alloc_blk(const ObSSPhyBlockType blk_type) const;
  void inner_update_alloc_blk(const ObSSPhyBlockType blk_type, const bool is_allowed);
  void inner_update_alloc_all_blk(const bool is_allowed);

private:
  struct SparsePhyBlockInfo
  {
  public:
    int64_t blk_idx_;
    int32_t valid_len_;

    SparsePhyBlockInfo() : blk_idx_(-1), valid_len_(0) {}
    SparsePhyBlockInfo(const int64_t blk_idx, const int32_t valid_len)
        : blk_idx_(blk_idx), valid_len_(valid_len) {}
    TO_STRING_KV(K_(blk_idx), K_(valid_len));
  };

  struct SparsePhyBlockInfoCmp 
  {
  public:
    bool operator()(const SparsePhyBlockInfo &l, const SparsePhyBlockInfo &r)
    {
      return l.valid_len_ < r.valid_len_;
    }
  };

private:
  static constexpr int32_t PHY_BLOCK_SUB_ARR_SIZE = 4096;
  static constexpr uint8_t SS_PHY_BLK_TYPE_CNT = static_cast<uint8_t>(ObSSPhyBlockType::SS_REORGAN_BLK) + 1;
  static constexpr int64_t PHY_BLK_HANDLE_WAIT_TIMEOUT_US = 10 * 1000 * 1000L;  // 10s
  static constexpr int64_t PHY_BLK_HANDLE_WAIT_SLEEP_US = 100 * 1000L;          // 100ms

private:
  typedef common::Ob2DArray<ObSSPhysicalBlock, PHY_BLOCK_SUB_ARR_SIZE> PhyBlockArr;
  typedef common::ObLinearHashMap<ObSSPhyBlockIdx, bool, UniqueMemMgrTag> SparsePhyBlockMap;

  bool is_inited_;
  int32_t block_size_;
  int32_t sub_arr_blk_cnt_;
  uint64_t tenant_id_;
  int64_t cache_file_size_;
  double phy_blk_reorgan_pct_;
  bool can_alloc_blk_[SS_PHY_BLK_TYPE_CNT];
  ObSSPhyBlockCountInfo blk_cnt_info_;
  int32_t newer_super_blk_index_; // Stores the phy_blk index of the newer super_blk that was last written.
                                  // When writing to super_blk, the older super_blk is targeted first, followed by the newer one. 
                                  // When reading from super_blk, the newer super_blk is prioritized first, followed by the older one.
  ObSSMicroCacheSuperBlk super_blk_;
  common::SpinRWLock resize_lock_; // control 'RW on cache_file_size/phy_blk_arr/super_blk/blk_cnt_info(static stat)'
  common::SpinRWLock info_lock_; // control 'RW on blk_cnt_info(dynamic stat)/free_bitmap'
  ObBitmap *free_bitmap_; // 1: free, 0: used
  common::hash::ObHashSet<int64_t> reusable_blks_; // ready to reuse these phy_blocks
  SparsePhyBlockMap sparse_blk_map_; // when phy_blk usage ration <= N, will add it into map
  PhyBlockArr phy_blk_arr_;
  ObSSMicroCacheStat &cache_stat_;
  common::ObConcurrentFIFOAllocator &allocator_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_ */
