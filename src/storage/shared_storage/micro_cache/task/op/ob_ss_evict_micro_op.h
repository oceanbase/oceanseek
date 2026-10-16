/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARE_STORAGE_OB_EVICT_MICRO_OP_H_
#define OCEANBASE_STORAGE_SHARE_STORAGE_OB_EVICT_MICRO_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_arc_info.h"

namespace oceanbase
{
namespace storage
{
struct ObSSMicroCacheTaskCtx;

struct SSHandleSegEvictionCtx
{
public:
  bool need_handle_;
  bool to_delete_;
  int64_t handle_cnt_;
  int64_t handle_size_;
  int64_t exe_round_;
  int64_t start_time_us_;
  int64_t exe_cost_us_;

  SSHandleSegEvictionCtx() { reset(); }
  void reset();
  void update_handle_info(const int64_t delta_cnt, const int64_t delta_size) 
  { 
    handle_cnt_ += delta_cnt;
    handle_size_ += delta_size;
  }
  bool can_run_next_round() const;
  void finish_one_round();

  TO_STRING_KV(K_(need_handle), K_(to_delete), K_(handle_cnt), K_(handle_size), K_(exe_round), K_(exe_cost_us));

private:
  const int64_t MAX_EXECUTE_ROUND = 10;
  const int64_t MAX_HANDLE_ONE_SEG_OP_TIME_US = 60 * 1000;
};

struct SSHandleEvictionCtx
{
public:
  bool need_handle_[SS_ARC_SEG_COUNT];
  int64_t handle_cnt_[SS_ARC_SEG_COUNT];
  int64_t total_handle_cnt_;
  int64_t total_handle_size_;
  int64_t total_handle_cost_us_;

  SSHandleEvictionCtx() { reset(); }
  void reset();
  bool exist_handle_op() const { return total_handle_cnt_ > 0; }
  bool handled_seg_op(const int64_t seg_idx);
  bool is_timeout() const { return total_handle_cost_us_ > MAX_HANDLE_SEG_OP_TIME_US; }
  void set_total_handle_cost(const int64_t cost_us) { total_handle_cost_us_ = cost_us; }
  void add_handle_seg_op_ctx(const int64_t seg_idx, const SSHandleSegEvictionCtx &seg_eviction_ctx);

  TO_STRING_KV("t1_need_handle", need_handle_[SS_ARC_T1], "b1_need_handle", need_handle_[SS_ARC_B1], "t2_need_handle",
      need_handle_[SS_ARC_T2], "b2_need_handle", need_handle_[SS_ARC_B2], "t1_handle_cnt", handle_cnt_[SS_ARC_T1], 
      "b1_handle_cnt", handle_cnt_[SS_ARC_B1], "t2_handle_cnt", handle_cnt_[SS_ARC_T2], "b2_handle_cnt", 
      handle_cnt_[SS_ARC_B2], K_(total_handle_cnt), K_(total_handle_size), K_(total_handle_cost_us));

private:
  const int64_t MAX_HANDLE_SEG_OP_TIME_US = 240 * 1000;
};

class ObSSEvictMicroOp : public ObSSMicroCacheBaseOp
{
public:
  ObSSEvictMicroOp();
  virtual ~ObSSEvictMicroOp() { destroy(); }

  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  int execute_eviction();
  void clear_for_next_round();

private:
  int handle_arc_eviction();
  int handle_arc_seg_eviction(const int64_t seg_idx);
  int handle_cold_micro_blocks(const int64_t seg_idx, SSHandleSegEvictionCtx &seg_eviction_ctx);
  int handle_cold_micro_block(const int64_t seg_idx, const ObSSMicroBlockMetaInfo &cold_micro_info, 
                              const bool to_delete, SSHandleSegEvictionCtx &seg_eviction_ctx);
  bool need_evict_persisted_micro(const int64_t seg_idx);
  int evict_persisted_cold_micro_blocks(const int64_t seg_idx, const ObSSARCInfo &arc_info,
                                        bool &finish_handle);
  int evict_mem_cold_micro_blocks(const int64_t seg_idx, const ObSSARCInfo &arc_info,
                                  SSHandleSegEvictionCtx &seg_eviction_ctx, bool &finish_handle);

private:
  ObSSARCIterInfo arc_iter_info_;
  SSHandleEvictionCtx eviction_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARE_STORAGE_OB_EVICT_MICRO_OP_H_ */