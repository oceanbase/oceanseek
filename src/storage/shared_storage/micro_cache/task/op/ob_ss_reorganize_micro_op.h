/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_
#define OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_

#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_arc_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase
{
namespace storage
{
struct SSReorganizeBlkCtx
{
public:
  bool has_scanned_;
  int32_t block_size_;
  int64_t last_scan_time_;
  int64_t last_reorgan_time_;
  int64_t candidate_blk_cnt_;
  int64_t candidate_micro_cnt_;
  int64_t reorgan_micro_cnt_;
  int64_t reorgan_total_size_;
  int64_t reorgan_used_blk_cnt_;
  int64_t reorgan_free_blk_cnt_;

  SSReorganizeBlkCtx() {}
  ~SSReorganizeBlkCtx() { reset(); }
  int init(const uint64_t tenant_id, const int32_t block_size);
  void reset();
  void reuse();

  OB_INLINE void update_reorgan_total_size(const int64_t delta) { reorgan_total_size_ += delta; }
  OB_INLINE void update_reorgan_used_blk_cnt(const int64_t delta) { reorgan_used_blk_cnt_ += delta; }
  OB_INLINE void update_reorgan_free_blk_cnt(const int64_t delta) { reorgan_free_blk_cnt_ += delta; }
  OB_INLINE int64_t new_free_blk_cnt() const { return reorgan_free_blk_cnt_ - reorgan_used_blk_cnt_; }

  TO_STRING_KV(K_(has_scanned), K_(last_scan_time), K_(last_reorgan_time), K_(candidate_blk_cnt), K_(candidate_micro_cnt), 
    K_(reorgan_micro_cnt), K_(reorgan_total_size), K_(reorgan_used_blk_cnt), K_(reorgan_free_blk_cnt), "free_blk_cnt", 
    new_free_blk_cnt());
};

class ObSSReorganizeMicroOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSReorganizeMicroOp();
  virtual ~ObSSReorganizeMicroOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();

  int execute_reorganization();
  void clear_for_next_round();

private:
  struct SSCandidateMicroInfo
  {
  public:
    int64_t idx_;
    ObSSAggregateMicroInfo aggr_info_;

    SSCandidateMicroInfo() : idx_(-1), aggr_info_() {}
    SSCandidateMicroInfo(const int64_t idx, const ObSSAggregateMicroInfo &aggr_info)
      : idx_(idx), aggr_info_(aggr_info)
    {}

    TO_STRING_KV(K_(idx), K_(aggr_info));
  };

private:
  bool can_trigger_reorganize();
  bool reach_scan_interval();
  bool need_scan_sparse_blk();
  int reorganize_phy_block();
  int read_sparse_phy_blocks();
  int handle_sparse_phy_blocks();
  int select_valid_micro_blocks(const int64_t idx, ObSSPhyBlkReorganEntry &blk_reorgan_entry, char *data_buf);
  int check_micro_need_reorganize(ObSSMicroReorganEntry &micro_reorgan_entry, const int64_t exp_phy_blk_idx,
                                 const int32_t phy_blk_size, bool &need_reorganize);
  int reaggregate_micro_blocks();
  int write_micro_blocks(const common::ObIArray<SSCandidateMicroInfo> &micro_info_arr);
  int write_micro_block(const int64_t micro_idx);
  int alloc_all_read_buf();
  void free_all_read_buf();

private:
  struct SSCandidateMicroInfoCmp
  {
  public:
    bool operator() (const SSCandidateMicroInfo &le, const SSCandidateMicroInfo &re)
    {
      return le.aggr_info_ < re.aggr_info_;
    }
  };

private:
  static constexpr double MEDIUM_SPACE_USAGE_RATIO = 0.5;
  static constexpr int64_t SCAN_BLOCK_CNT_HINT = 10000;

private:
  typedef common::hash::ObHashMap<ObSSMicroBlockCacheKey, bool, common::hash::NoPthreadDefendMode> ObSSReorganMicroKeyMap;
  SSReorganizeBlkCtx reorgan_ctx_;
  common::ObArray<void *> read_buf_arr_;
  common::ObArray<ObSSPhyBlkReorganEntry> candidate_phy_blks_;
  common::ObArray<ObSSMicroReorganEntry> candidate_micro_blks_;
  ObSSReorganMicroKeyMap candidate_key_map_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_ */