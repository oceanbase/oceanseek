/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_info.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------ObSSPhyBlkReorganEntry-----------------------------------------*/
int ObSSPhyBlkReorganEntry::assign(const ObSSPhyBlkReorganEntry &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(phy_blk_handle_.assign(other.phy_blk_handle_))) {
      LOG_WARN("fail to assign phy_block handle", KR(ret));
    } else {
      need_read_blk_ = other.need_read_blk_;
      phy_blk_idx_ = other.phy_blk_idx_;
      reuse_version_ = other.reuse_version_;
      io_handle_.reset();
    }
  }
  return ret;
}

void ObSSPhyBlkReorganEntry::reset()
{
  need_read_blk_ = false;
  phy_blk_idx_ = -1;
  reuse_version_ = 0;
  phy_blk_handle_.reset();
  io_handle_.reset();
}

bool ObSSPhyBlkReorganEntry::is_valid() const 
{ 
  return ((phy_blk_idx_ >= 0) && (reuse_version_ > 0) && phy_blk_handle_.is_valid()); 
}

/*-----------------------------------------ObSSMicroReorganEntry-----------------------------------------*/
int ObSSMicroReorganEntry::assign(const ObSSMicroReorganEntry &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(micro_meta_handle_.assign(other.micro_meta_handle_))) {
      LOG_WARN("fail to assign micro_meta_handle", KR(ret), K(other));
    } else {
      idx_ = other.idx_;
      data_ptr_ = other.data_ptr_;
      exp_data_loc_ = other.exp_data_loc_;
      exp_micro_size_ = other.exp_micro_size_;
      exp_crc_ = other.exp_crc_;
    }
  }
  return ret;
}

void ObSSMicroReorganEntry::reset() 
{ 
  idx_ = -1; 
  micro_meta_handle_.reset(); 
  data_ptr_ = nullptr; 
  exp_data_loc_ = 0; 
  exp_micro_size_ = 0;
  exp_crc_ = 0;
}

bool ObSSMicroReorganEntry::is_valid() const 
{ 
  return (idx_ >= 0) && (micro_meta_handle_.is_valid()) && (nullptr != data_ptr_) && 
         (micro_meta_handle_()->is_valid_field()) && (exp_data_loc_ > 0) && (exp_micro_size_ > 0); 
}

} /* namespace storage */
} /* namespace oceanbase */
