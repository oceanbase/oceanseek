/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_OP_H_

#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"

namespace oceanbase
{
namespace storage
{
struct SSPersistOPEntry
{
public:
  int64_t phy_blk_idx_;
  int64_t updated_micro_cnt_;
  ObSSPhyBlockHandle phy_blk_handle_;
  ObSSMemBlockHandle mem_blk_handle_;
  common::ObIOHandle io_handle_;

  SSPersistOPEntry();
  ~SSPersistOPEntry() { reset(); }
  int init(const int64_t phy_blk_idx, ObSSPhysicalBlock *phy_blk, ObSSMemBlock *mem_blk);
  void reset();
  bool is_valid() const
  {
    return (phy_blk_idx_ >= 0) && phy_blk_handle_.is_valid() && mem_blk_handle_.is_valid();
  }
  TO_STRING_KV(K_(phy_blk_idx), K_(updated_micro_cnt), K_(phy_blk_handle), K_(mem_blk_handle));
};

class ObSSPersistMicroDataOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSPersistMicroDataOp();
  virtual ~ObSSPersistMicroDataOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  int execute();
  int64_t get_alloc_blk_fail_cnt() const { return alloc_blk_fail_cnt_; }
  
private:
  int pre_alloc_persist_entry();
  int persist_sealed_mem_blocks();
  int async_write_mem_block(SSPersistOPEntry &persist_entry, const ObSSMemBlockHandle &sealed_mem_blk_handle);
  int async_write_batch_mem_blocks(ObArray<ObSSMemBlockHandle> &sealed_mem_blk_handles);
  int update_micro_meta_for_mem_block(SSPersistOPEntry &persist_entry);
  int update_micro_meta_for_batch_mem_blocks();
  int handle_sealed_mem_block(const int64_t updated_micro_cnt, const bool valid_phy_blk, ObSSMemBlockHandle &mem_blk_handle);

private:
  int64_t max_mem_blk_cnt_;
  int64_t alloc_blk_fail_cnt_;
  SSPersistOPEntry *entry_arr_;
  common::ObFixedQueue<SSPersistOPEntry> free_list_;
  common::ObFixedQueue<SSPersistOPEntry> in_use_list_; // sealed_mem_blocks that succeed to async write
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_OP_H_ */