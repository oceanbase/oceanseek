/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_do_checkpoint_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{

class ObSSDoBlkCheckpointOp : public ObSSDoCheckpointBaseOp
{
public:
  ObSSDoBlkCheckpointOp();
  virtual ~ObSSDoBlkCheckpointOp() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx) override;
  void destroy();

private:
  virtual int start_op() override;
  virtual int gen_checkpoint() override;
  int gen_phy_block_checkpoint();
  int mark_reusable_blocks(common::ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr);
  int build_blk_ckpt_item(common::ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr);
  int handle_block_id_list(ObSSCkptPhyBlockItemWriter &item_writer);
  virtual int update_super_block(bool &succ_ckpt) override;
  virtual int try_free_phy_blocks(const bool succ_ckpt) override;
  virtual void handle_extra_func() override {}
  bool need_scan_reusable_blocks();
  void check_before_free_phy_blocks(common::ObIArray<int64_t> &reusable_blk_list);
  void check_after_free_phy_blocks(const bool succ_ckpt);

private:
  const static int64_t MIN_REUSE_PHY_BLOCK_CNT = 100; // the min count of phy_block to reuse per round
  const static int64_t DEFAULT_BLK_CNT = 256;

private:
  SSBlkCkptCtx blk_ckpt_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_BLK_CHECKPOINT_OP_H_ */