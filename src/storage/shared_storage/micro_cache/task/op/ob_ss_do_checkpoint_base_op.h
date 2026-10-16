/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_CHECKPOINT_BASE_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_CHECKPOINT_BASE_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
class ObSSCkptPhyBlockItemWriter;

struct SSCkptBaseCtx
{
public:
  ObSSMicroCacheCkptType ckpt_type_;
  bool need_ckpt_;
  int64_t ser_item_cnt_; // serialized item count
  int64_t ckpt_item_cnt_; // persisted item count
  int64_t prev_ckpt_us_; // save prev full checkpoint execute finish time
  int64_t ckpt_start_us_; // save cur round checkpoint execute start time
  int64_t ckpt_cost_us_; // save cur round checkpoint execute cost time
  int64_t free_blk_cnt_;
  common::ObSEArray<int64_t, SS_DEF_SMALL_ARRAY_CNT> failed_blk_id_list_;
  ObSSMicroCacheSuperBlk prev_super_blk_; // only use ckpt_info in prev & cur super_blk
  ObSSMicroCacheSuperBlk cur_super_blk_;

  SSCkptBaseCtx() 
    : ckpt_type_(ObSSMicroCacheCkptType::SS_CACHE_MICRO_CKPT), need_ckpt_(false), 
      ser_item_cnt_(0), ckpt_item_cnt_(0), prev_ckpt_us_(0), ckpt_start_us_(0), ckpt_cost_us_(0), 
      free_blk_cnt_(0), failed_blk_id_list_(), prev_super_blk_(), cur_super_blk_()
  {}

  OB_INLINE bool need_ckpt() const { return need_ckpt_; }
  OB_INLINE int64_t prev_ckpt_us() const { return prev_ckpt_us_; }
  OB_INLINE int64_t ckpt_cost_us() const { return ckpt_cost_us_; }
  OB_INLINE int64_t ser_item_cnt() const { return ser_item_cnt_; }
  OB_INLINE int64_t ckpt_item_cnt() const { return ckpt_item_cnt_; }

  virtual int handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt);
  virtual void reuse();
  virtual void reset();
  virtual void start_ckpt();
  virtual void finish_ckpt();

  TO_STRING_KV(K_(ckpt_type), K_(need_ckpt), K_(ser_item_cnt), K_(ckpt_item_cnt), K_(prev_ckpt_us),
    K_(ckpt_start_us), K_(ckpt_cost_us), K_(free_blk_cnt), "failed_blk_cnt", failed_blk_id_list_.count(), 
    K_(prev_super_blk), K_(cur_super_blk));
};

struct SSBlkCkptCtx : public SSCkptBaseCtx
{
public:
  int64_t prev_scan_blk_time_us_;

  SSBlkCkptCtx();
  virtual void reset() override;
  virtual void reuse() override;
  virtual void start_ckpt() override;
  virtual void finish_ckpt() override;
  virtual int handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt) override;
  bool need_exe_full_ckpt() const;
  bool need_scan_reusable_blk() const;
  void finish_scan_phy_blk();

  INHERIT_TO_STRING_KV("SSCkptBaseCtx", SSCkptBaseCtx, K_(prev_scan_blk_time_us));
};

struct SSMicroCkptCtx : public SSCkptBaseCtx
{
public:
  bool lack_phy_blk_;
  // when restart observer, we will replay ss_micro_cache ckpt async. After replay all ckpt, ss_micro_cache
  // will start to serve.
  bool need_replay_ckpt_;
  // in one round micro_ckpt operation, we only persist part of micro_metas. when we handled all micro_metas,
  // set this flag as true
  bool complete_full_ckpt_;
  int64_t prev_update_expiration_us_;
  int64_t handle_expire_interval_us_;
  int64_t cur_ckpt_exe_idx_;
  uint32_t cur_ckpt_split_cnt_;

  SSMicroCkptCtx();
  virtual void reset() override;
  virtual void reuse() override;
  virtual void start_ckpt() override;
  virtual void finish_ckpt() override;
  virtual int handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt) override;

  bool need_exe_full_ckpt() const;
  bool need_exe_part_ckpt() const;
  bool need_handle_expiration() const;
  OB_INLINE bool complete_full_ckpt() const { return complete_full_ckpt_; }
  OB_INLINE bool need_replay_ckpt() const { return need_replay_ckpt_; }
  bool need_update_expiration() const;
  void enable_replay_ckpt();
  void disable_replay_ckpt();
  void finish_update_expiration();
  int64_t get_cur_ckpt_exe_idx() const { return cur_ckpt_exe_idx_; }
  uint32_t get_cur_ckpt_split_cnt() const { return cur_ckpt_split_cnt_; }

  INHERIT_TO_STRING_KV("SSCkptBaseCtx", SSCkptBaseCtx, K_(lack_phy_blk), K_(need_replay_ckpt), K_(complete_full_ckpt),
    K_(prev_update_expiration_us), K_(handle_expire_interval_us), K_(cur_ckpt_exe_idx), K_(cur_ckpt_split_cnt));
};

struct ObSSPersistedSubRangeMicroInfo
{
public:
  int64_t offset_;
  int64_t length_;
  ObSSMicroSubRangeInfo *sub_range_;
  ObSSAllSegMetaInfo all_pmeta_info_;
  common::ObSEArray<ObSSMicroBlockCacheKey, SS_DEF_ARRAY_CNT> serialized_micro_keys_;
  common::ObSEArray<uint32_t, SS_DEF_ARRAY_CNT> ghost_hval_arr_;
  
  ObSSPersistedSubRangeMicroInfo() { reset(); }
  ~ObSSPersistedSubRangeMicroInfo() { reset(); }
  void reset();
  int assign(const ObSSPersistedSubRangeMicroInfo &other);
  void set_sub_range(ObSSMicroSubRangeInfo *sub_range) { sub_range_ = sub_range; }
  ObSSMicroSubRangeInfo *get_sub_range() const { return sub_range_; }
  int64_t get_serialized_micro_cnt() const { return serialized_micro_keys_.count(); }
  int add_serialized_micro_key(const ObSSMicroBlockCacheKey &micro_key);
  bool is_valid() const;
  void set_offset_length(const int64_t offset, const int64_t length) { offset_ = offset; length_ = length; }

  TO_STRING_KV(K_(offset), K_(length), KP_(sub_range), K_(all_pmeta_info), "ser_key_cnt", 
    serialized_micro_keys_.count(), "ghost_hval_cnt", ghost_hval_arr_.count());
};

class ObSSDoCheckpointBaseOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSDoCheckpointBaseOp();
  virtual ~ObSSDoCheckpointBaseOp() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  virtual int execute_checkpoint();

protected:
  int execute_checkpoint_op();
  virtual int start_op();
  virtual int gen_checkpoint() = 0;
  virtual void finish_op();

  bool has_free_ckpt_blk();
  int get_ckpt_block_id_list(ObSSCkptPhyBlockItemWriter &item_writer, common::ObIArray<int64_t> &block_id_list,
                             const bool is_failed_blk_list);
  virtual int update_super_block(bool &succ_ckpt) = 0;
  virtual int try_free_phy_blocks(const bool succ_ckpt) = 0;
  virtual void handle_extra_func() {}

private:
  int inner_ensure_ckpt_ctx_valid();

protected:
  SSCkptBaseCtx *ckpt_ctx_;
  char *item_buf_;
  int64_t item_buf_size_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DO_CHECKPOINT_BASE_OP_H_ */