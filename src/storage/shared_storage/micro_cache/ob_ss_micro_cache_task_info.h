/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_INFO_H_

#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase 
{
namespace storage 
{

struct ObSSPhyBlkReorganEntry
{
public:
  bool need_read_blk_;
  int64_t phy_blk_idx_;
  uint64_t reuse_version_;
  ObSSPhyBlockHandle phy_blk_handle_;
  common::ObIOHandle io_handle_;

  ObSSPhyBlkReorganEntry() { reset(); }
  ObSSPhyBlkReorganEntry(const int64_t phy_blk_idx, ObSSPhysicalBlock *phy_blk, const uint64_t reuse_version)
    : need_read_blk_(false), phy_blk_idx_(phy_blk_idx), reuse_version_(reuse_version), phy_blk_handle_(), io_handle_()
  {
    phy_blk_handle_.set_ptr(phy_blk);
    io_handle_.reset();
  }
  ~ObSSPhyBlkReorganEntry() { reset(); }
  void reset();
  bool is_valid() const;
  int assign(const ObSSPhyBlkReorganEntry &other);
  TO_STRING_KV(K_(need_read_blk), K_(phy_blk_idx), K_(reuse_version), K_(phy_blk_handle), K_(io_handle));
};

struct ObSSMicroReorganEntry
{
public:
  int64_t idx_; // the candidate_arr index of phy_blk which contains this micro_block
  ObSSMicroBlockMetaHandle micro_meta_handle_;
  char *data_ptr_;
  uint64_t exp_data_loc_;
  int32_t exp_micro_size_;
  uint32_t exp_crc_;

  ObSSMicroReorganEntry() 
    : idx_(-1), micro_meta_handle_(), data_ptr_(nullptr), exp_data_loc_(0), exp_micro_size_(0), exp_crc_(0)
  {}
  ~ObSSMicroReorganEntry() { reset(); }

  void reset();
  bool is_valid() const;
  int assign(const ObSSMicroReorganEntry &other);
  int32_t size() const { return exp_micro_size_; }
  uint64_t data_loc() const { return exp_data_loc_; }
  uint32_t crc() const { return exp_crc_; }
  // when use below func, please ensure this entry is valid
  bool is_in_l1() const { return micro_meta_handle_()->is_in_l1(); }
  bool is_in_ghost() const { return micro_meta_handle_()->is_in_ghost(); }
  uint64_t effective_tablet_id() const { return micro_meta_handle_()->effective_tablet_id(); }
  const ObSSMicroBlockCacheKey &micro_key() const { return micro_meta_handle_()->get_micro_key(); }

  TO_STRING_KV(K_(idx), K_(micro_meta_handle), KP_(data_ptr), K_(exp_data_loc), K_(exp_micro_size), K_(exp_crc));
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_INFO_H_ */
