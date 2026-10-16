/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_BASIC_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_BASIC_OP_H_

#include "lib/container/ob_array.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_arc_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase 
{
namespace storage 
{

struct SSMicroMapCheckMetaDirtyGuard
{
public:
  SSMicroMapCheckMetaDirtyGuard(int &ret, ObSSMicroBlockMetaHandle &micro_meta_handle);
  ~SSMicroMapCheckMetaDirtyGuard();

public:
  int &ret_;
  ObSSMicroBlockMetaHandle &micro_meta_handle_;
  ObSSMicroBlockMetaInfo ori_micro_info_;
};

/*
 * To get a micro_block meta handle. A micro_block may be stored in mem_block or phy_block.
 * When is_persisted_ = false, that means stored in mem_block; Otherwise, stored in phy_block.
 * If this micro_block's meta is invalid, we treat this situation as NOT_EXIST.
 */
struct SSMicroMapGetMicroHandleFunc 
{
public:
  int ret_;
  bool exist_meta_; // true: micro_meta exist in micro_meta_map
  bool update_arc_; // true: transfer arc_seg
  uint64_t effective_tablet_id_;
  ObSSMicroArcOpType arc_op_type_;
  ObSSMicroBlockMetaInfo micro_info_;
  ObSSMicroBlockMetaHandle &micro_meta_handle_;
  ObSSMemBlockHandle &mem_blk_handle_;
  ObSSPhyBlockHandle &phy_blk_handle_;
  ObSSMicroCacheAccessInfo &access_info_;

  SSMicroMapGetMicroHandleFunc(ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle,
                               ObSSPhyBlockHandle &phy_blk_handle, const bool update_arc, 
                               ObSSMicroCacheAccessInfo &access_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
  void set_effective_tablet_id(const uint64_t effective_tablet_id) { effective_tablet_id_ = effective_tablet_id; }

  TO_STRING_KV(K_(exist_meta), K_(update_arc), K_(arc_op_type), K_(effective_tablet_id), K_(micro_info), K_(access_info),
    K_(micro_meta_handle), K_(mem_blk_handle), K_(phy_blk_handle));
};

struct SSMicroMapGetMicroInfoFunc
{
public:
  int ret_;
  ObSSMicroBlockMetaInfo &micro_info_;
  explicit SSMicroMapGetMicroInfoFunc(ObSSMicroBlockMetaInfo &micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

struct SSMicroMapGetMicroInfoForSerializeFunc
{
public:
  int ret_;
  bool need_serialize_;
  ObSSMicroRangeScanFilter &filter_;
  ObSSMicroBlockMetaInfo &micro_info_;
  SSMicroMapGetMicroInfoForSerializeFunc(ObSSMicroRangeScanFilter &filter, ObSSMicroBlockMetaInfo &micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * When write micro_block into mem_block, if this micro_meta already exist, we should update it:
 * If micro_block exist but invalid, update micro_block meta to valid;
 * If micro_block exist but valid, just need to update T1/T2/B1/B2;
 */
struct SSMicroMapUpdateMicroFunc
{
public:
  int ret_;
  bool is_updated_;
  bool is_deleted_micro_;
  bool is_invalid_micro_;
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;
  uint32_t crc_;
  uint64_t effective_tablet_id_;
  uint64_t access_type_;
  const ObSSMemBlockHandle &mem_blk_handle_;
  ObSSMicroArcOpType arc_op_type_;

  SSMicroMapUpdateMicroFunc(const int32_t size, const ObSSMemBlockHandle &mem_blk_handle, const uint32_t crc,
                            const uint64_t effective_tablet_id, const uint64_t access_type);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
  TO_STRING_KV(K_(ret), K_(is_updated), K_(is_deleted_micro), K_(is_invalid_micro), K_(is_in_l1), K_(is_in_ghost), 
    K_(size), K_(crc), K_(effective_tablet_id), K_(access_type), K_(mem_blk_handle), K_(arc_op_type));
};

struct SSMicroMapUpdateMicroHeatFunc
{
public:
  int ret_;
  bool transfer_seg_;
  bool update_access_time_;
  int64_t time_delta_s_;
  int32_t size_;
  ObSSMicroArcOpType arc_op_type_;

  SSMicroMapUpdateMicroHeatFunc(const bool transfer_seg, const bool update_access_time, const int64_t heat_delta);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * When a mem_block is sealed and persisted into cache_data_file, we need to update
 * all relative micro_blocks' meta, to change its location from mem_block to disk_offset.
 * It should be successful, otherwise, it is a logical error!!!
 */
struct SSMicroMapUpdateMicroDestFunc 
{
public:
  int ret_;
  int64_t data_loc_;   // micro_block data persisted location in cache_data_file
  uint32_t reuse_version_; // reuse_version of persisted phy_block
  ObSSMemBlockHandle &mem_blk_handle_;
  bool succ_update_;

  SSMicroMapUpdateMicroDestFunc(const int64_t data_loc, const uint32_t reuse_version, 
                                 ObSSMemBlockHandle &mem_blk_handle);

  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle); 
};

/*
 * Evict one micro_block, change its meta(update is_in_ghost and mark invalid) and return its 
 * offset & reuse_version, cuz we need to update its relative phy_block's valid_length
 */
struct SSMicroMapEvictMicroFunc
{
public:
  int ret_;
  const ObSSMicroBlockMetaInfo &cold_micro_info_;

  explicit SSMicroMapEvictMicroFunc(const ObSSMicroBlockMetaInfo &cold_micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Delete one micro_block, before delete its meta, return its offset & reuse_version, cuz we need to
 * update its relative phy_block's valid_length
 */
struct SSMicroMapDeleteMicroFunc 
{
public:
  int ret_;
  const ObSSMicroBlockMetaInfo &cold_micro_info_;

  explicit SSMicroMapDeleteMicroFunc(const ObSSMicroBlockMetaInfo &cold_micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Delete one micro_block no matter it is valid or nor.
 */
struct SSMicroMapForceDeleteMicroFunc
{
public:
  int ret_;
  bool succ_delete_;
  ObSSMicroBlockMetaInfo &micro_meta_info_;

  explicit SSMicroMapForceDeleteMicroFunc(ObSSMicroBlockMetaInfo &micro_meta_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If a sealed mem_block(which is dynamiclly created) is being persisted, but part fail to 
 * update micro_block's meta from mem_address to a disk address, we need to delete all of 
 * its unpersisted micro_block before free this mem_block.
 */
struct SSMicroMapDeleteUnpersistedMicroFunc
{
public:
  int ret_;
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;
  bool succ_delete_;
  ObSSMemBlockHandle &mem_blk_handle_;

  explicit SSMicroMapDeleteUnpersistedMicroFunc(ObSSMemBlockHandle &mem_blk_handle);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If micro_meta's is_meta_deleted=true, we need to erase it from micro_meta_map.
 */
struct SSMicroMapEraseDeletedMicroFunc
{
public:
  int ret_;
  bool is_in_ghost_;
  int32_t micro_size_;

  SSMicroMapEraseDeletedMicroFunc();
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If micro_meta is expired, we need to erase it from micro_meta_map.
 */
struct SSMicroMapEraseExpiredMicroFunc
{
public:
  int ret_;
  int64_t expiration_time_s_;
  ObSSMicroBlockMetaInfo &micro_meta_info_;

  SSMicroMapEraseExpiredMicroFunc(const int64_t expiration_time_s, ObSSMicroBlockMetaInfo &micro_meta_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If micro_meta is in T1/T2, but invalid, we need to erase it from micro_meta_map
 */
struct SSMicroMapEraseAbnormalMicroFunc
{
public:
  int ret_;
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;

  SSMicroMapEraseAbnormalMicroFunc();
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If micro_meta is in T1/T2, but invalid, we need to erase it from micro_meta_map
 */
struct SSMicroMapForceEraseMicroFunc
{
public:
  int ret_;
  int64_t expiration_time_s_;
  uint64_t tablet_id_;
  ObSSMicroBlockMetaInfo &micro_meta_info_;

  SSMicroMapForceEraseMicroFunc(const int64_t expiration_time_s, const uint64_t tablet_id,
    ObSSMicroBlockMetaInfo &micro_meta_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Try to update this micro_block meta when we reorganize it from phy_block to mem_block
 *
 * That means, we need to update some attributes, like: 'is_data_persisted': true -> false
 */
struct SSMicroMapUpdateReorganizingMicroFunc
{
public:
  bool succ_update_;
  int ret_;
  uint64_t exp_micro_data_loc_;
  uint32_t exp_micro_crc_;
  uint64_t exp_reuse_version_;
  ObSSMemBlockHandle &mem_blk_handle_;

  SSMicroMapUpdateReorganizingMicroFunc(const uint64_t exp_micro_data_loc, const uint32_t exp_micro_crc, 
                                        const uint64_t exp_reuse_version, ObSSMemBlockHandle &mem_blk_handle);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle); 
};

/*
 * Try to update effective_tablet_id in ss_micro_meta
 */
struct SSMicroMapUpdateEffectiveTabletID
{
public:
  int ret_;
  uint64_t effective_tablet_id_;

  explicit SSMicroMapUpdateEffectiveTabletID(const uint64_t effective_tablet_id);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * For a persisted micro_meta, we may need to erase it to free memory, or update it
 */
struct SSMicroMapEraseOrUpdateMicroMetaFunc
{
public:
  int ret_;
  bool can_erase_micro_;
  ObSSMicroBlockMetaInfo &micro_meta_info_;
  explicit SSMicroMapEraseOrUpdateMicroMetaFunc(const bool can_erase_micro, ObSSMicroBlockMetaInfo &micro_meta_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If tablet_id's recorded_ckpt_op_cnt meets cleanup condition, erase it from tablets_to_delete map.
 */
struct SSMicroCacheTabletMapEraseFunc
{
public:
  int64_t cur_micro_ckpt_op_cnt_;
  int64_t meta_ckpt_split_cnt_;
  int64_t recorded_ckpt_op_cnt_;

  SSMicroCacheTabletMapEraseFunc(const int64_t cur_micro_ckpt_op_cnt, const int64_t meta_ckpt_split_cnt);
  bool operator()(common::hash::HashMapPair<common::ObTabletID, int64_t> &pair);
};

/*
 * For calibration: traverse meta map, erase invalid/expired/reuse_version_mismatch,
 * keep valid ones by linking to sub_range and updating arc_info.
 */
class ObSSMicroMetaManager;

struct SSCalibrationTraverseMetaFunc
{
public:
  ObSSMicroMetaManager *micro_meta_mgr_;
  common::ObArray<int64_t> &expected_valid_len_arr_;
  int32_t block_size_;
  int ret_;
  int64_t link_fail_cnt_;    // number of micro_meta failed to link into sub_range (includes alloc failures)
  int64_t fail_alloc_cnt_;   // link failures with OB_ALLOCATE_MEMORY_FAILED; kept in map, subset of link_fail_cnt_
  int64_t erased_cnt_;       // number of micro_meta erased from meta_map
  int64_t processed_cnt_;    // number of micro_meta traversed

  SSCalibrationTraverseMetaFunc(ObSSMicroMetaManager *micro_meta_mgr,
                                common::ObArray<int64_t> &expected_valid_len,
                                const int32_t block_size);
  bool operator()(const ObSSMicroBlockCacheKey *key, ObSSMicroBlockMetaHandle &micro_handle);

private:
  int do_link_and_update(const ObSSMicroBlockMetaInfo &micro_meta_info, ObSSMicroBlockMetaHandle &micro_handle);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_BASIC_OP_H_ */
