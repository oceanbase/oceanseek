/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_do_checkpoint_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase
{
namespace storage
{
class ObSSCkptPhyBlockItemWriter;

class ObSSPersistMicroMetaOp : public ObSSDoCheckpointBaseOp
{
public:
  ObSSPersistMicroMetaOp();
  virtual ~ObSSPersistMicroMetaOp() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx) override;
  void destroy();
  void enable_replay_ckpt();
  void update_latest_expiration_time();

private:
  virtual int start_op() override;
  virtual int gen_checkpoint() override;
  virtual int update_super_block(bool &succ_ckpt) override;
  virtual int try_free_phy_blocks(const bool succ_ckpt) override;
  virtual void handle_extra_func() override;

  bool check_need_ckpt();
  int gen_micro_meta_checkpoint();
  int handle_and_persist_micro_meta(ObSSCkptPhyBlockItemWriter &item_writer);
  int handle_block_id_list(ObSSCkptPhyBlockItemWriter &item_writer);
  int write_batch_micro_meta(ObSSCkptPhyBlockItemWriter &item_writer,
                             const int64_t init_range_idx);
  int write_single_range_micro_meta(ObSSCkptPhyBlockItemWriter &item_writer,
                                    ObSSMicroInitRangeInfo *init_range,
                                    ObSSMicroSubRangeInfo *sub_range);
  int do_persist_micro_meta(ObSSMicroBlockMeta *micro_meta, ObSSCkptPhyBlockItemWriter &item_writer,
                            ObSSMicroRangeScanFilter &filter, ObSSMicroBlockMetaInfo &ser_micro_info,
                            bool &is_item_serialized);
  int handle_persisted_sub_ranges(ObSSCkptPhyBlockItemWriter &item_writer);
  int build_sub_range_bloom_filter(ObSSMicroSubRangeInfo *sub_range,
                                   const ObIArray<uint32_t> &ghost_hval_arr);
  int update_sub_ranges_persisted_info(const int64_t phy_blk_idx);
  int handle_persisted_micro_meta();
  int replay_cache_ckpt_async();
  void handle_meta_ckpt_failure();
  void inner_reset_persist_range_arr();
  int check_split_cnt_changed();

private:
  const static int64_t DEFAULT_MAP_BUCKET_NUM = 100000;

private:
  bool enable_save_meta_memory_;
  int32_t trigger_type_;
  int64_t retry_duration_us_;
  SSMicroCkptCtx micro_ckpt_ctx_;
  ObSSMCTabletInfoMap tablet_info_map_; // To save each tablet's cache info
  int64_t full_ckpt_item_cnt_;
  common::ObSEArray<ObSSPersistedSubRangeMicroInfo, SS_DEF_ARRAY_CNT> persist_range_arr_;
  common::ObArray<int64_t> handled_range_idx_arr_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_META_OP_H_ */