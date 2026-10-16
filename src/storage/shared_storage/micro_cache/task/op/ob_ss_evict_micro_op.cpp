/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_evict_micro_op.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------SSHandleSegEvictionCtx------------------------------------------*/
void SSHandleSegEvictionCtx::reset()
{
  need_handle_ = false;
  to_delete_ = false;
  handle_cnt_ = 0;
  handle_size_ = 0;
  exe_round_ = 0;
  start_time_us_ = ObTimeUtility::current_time_us();
  exe_cost_us_ = 0;
}

bool SSHandleSegEvictionCtx::can_run_next_round() const
{
  return ((exe_round_ < MAX_EXECUTE_ROUND) && (exe_cost_us_ < MAX_HANDLE_ONE_SEG_OP_TIME_US));
}

void SSHandleSegEvictionCtx::finish_one_round()
{
  ++exe_round_;
  exe_cost_us_ = ObTimeUtility::current_time_us() - start_time_us_;
}

/*-----------------------------------------SSHandleEvictionCtx------------------------------------------*/
void SSHandleEvictionCtx::reset()
{
  MEMSET(need_handle_, 0, sizeof(need_handle_));
  MEMSET(handle_cnt_, 0, sizeof(handle_cnt_));
  total_handle_cnt_ = 0;
  total_handle_size_ = 0;
  total_handle_cost_us_ = 0;
}

bool SSHandleEvictionCtx::handled_seg_op(const int64_t seg_idx)
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = (need_handle_[seg_idx] && (handle_cnt_[seg_idx] > 0));
  }
  return b_ret;
}

void SSHandleEvictionCtx::add_handle_seg_op_ctx(const int64_t seg_idx, const SSHandleSegEvictionCtx &seg_eviction_ctx)
{
  if (is_valid_arc_seg_idx(seg_idx)) {
    need_handle_[seg_idx] |= seg_eviction_ctx.need_handle_;
    handle_cnt_[seg_idx] += seg_eviction_ctx.handle_cnt_;
    total_handle_cnt_ += seg_eviction_ctx.handle_cnt_;
    total_handle_size_ += seg_eviction_ctx.handle_size_;
    total_handle_cost_us_ += seg_eviction_ctx.exe_cost_us_;
  }
}

/*-----------------------------------------ObSSEvictMicroOp------------------------------------------*/
ObSSEvictMicroOp::ObSSEvictMicroOp()
  : ObSSMicroCacheBaseOp(), arc_iter_info_(), eviction_ctx_()
{}

void ObSSEvictMicroOp::destroy()
{
  eviction_ctx_.reset();
  arc_iter_info_.destroy();
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSEvictMicroOp::init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arc_iter_info_.init(tenant_id))) {
    LOG_WARN("fail to init arc_iter_info", KR(ret), K(tenant_id));
  } else {
    op_type_ = ObSSMicroCacheTaskOpType::SS_EVICT_MICRO_OP;
    is_inited_ = true;
  }
  return ret;
}

int ObSSEvictMicroOp::execute_eviction()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (check_need_execute()) {
    handle_arc_eviction();
  }
  return ret;
}

int ObSSEvictMicroOp::handle_arc_eviction()
{
  int ret = OB_SUCCESS;
  int64_t seg_idx = SS_ARC_T1;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  }

  while (OB_SUCC(ret) && (is_op_enabled()) && (seg_idx < SS_ARC_SEG_COUNT) && (!eviction_ctx_.is_timeout())) {
    if (OB_FAIL(handle_arc_seg_eviction(seg_idx))) {
      LOG_WARN("fail to handle one arc_seg eviction", KR(ret), K(seg_idx));
    }
    arc_iter_info_.reuse(seg_idx);
    if (eviction_ctx_.handled_seg_op(seg_idx)) {
      micro_meta_mgr()->check_mem_limit();
    }
    ++seg_idx;
  }

  if (eviction_ctx_.exist_handle_op()) {
    const int64_t cost_us = get_cost_us();
    LOG_TRACE("ss_micro_cache: finish handle arc op", KR(ret), "arc_info", micro_meta_mgr()->get_arc_info(),
      K_(eviction_ctx), K(cost_us));
  }
  return ret;
}

int ObSSEvictMicroOp::handle_arc_seg_eviction(const int64_t seg_idx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    SSHandleSegEvictionCtx seg_eviction_ctx;
    bool finish_handle = false;
    const ObSSARCInfo &arc_info = micro_meta_mgr()->get_arc_info();

    while (OB_SUCC(ret) && (is_op_enabled()) && (seg_eviction_ctx.can_run_next_round()) && (!finish_handle)) {
      arc_info.calc_arc_iter_info(arc_iter_info_, seg_idx);
      if (arc_iter_info_.need_handle_arc_seg(seg_idx)) {
        seg_eviction_ctx.need_handle_ = true;
        if (need_evict_persisted_micro(seg_idx)) {
          if (OB_FAIL(evict_persisted_cold_micro_blocks(seg_idx, arc_info, finish_handle))) {
            LOG_WARN("fail to evict persisted cold micro_blocks", KR(ret), K(seg_idx), K(arc_info));
          }
        } else if (OB_FAIL(evict_mem_cold_micro_blocks(seg_idx, arc_info, seg_eviction_ctx, finish_handle))) {
          LOG_WARN("fail to evict memory cold micro_blocks", KR(ret), K(seg_idx), K(seg_eviction_ctx), K(arc_info));
        }
      } else {
        finish_handle = true;
      }
      seg_eviction_ctx.finish_one_round();
    }

    if (OB_SUCC(ret)) {
      eviction_ctx_.add_handle_seg_op_ctx(seg_idx, seg_eviction_ctx);
    }
  }
  return ret;
}

bool ObSSEvictMicroOp::need_evict_persisted_micro(const int64_t seg_idx)
{
  bool need_evict = false;
  if ((seg_idx == SS_ARC_B1 || seg_idx == SS_ARC_B2) && (micro_meta_mgr()->enable_save_meta_memory())) {
    const bool exist_persisted_ghost = cache_stat()->task_stat().exist_erase_persisted_micro();
    if (exist_persisted_ghost) {
      // If memory ghost micro_metas' count is less than valid_count * 10%
      const int64_t valid_micro_cnt = cache_stat()->micro_stat().get_valid_micro_cnt();
      const int64_t ghost_micro_cnt = cache_stat()->micro_stat().get_ghost_micro_cnt();
      const int64_t TRIGGER_EVICT_PCT = 10;
      const int64_t target_ghost_micro_cnt = valid_micro_cnt * TRIGGER_EVICT_PCT / 100;
      need_evict = (target_ghost_micro_cnt > 0 && ghost_micro_cnt < target_ghost_micro_cnt);
    }
  }
  return need_evict;
}

// Now, only evict ghost micro_blocks
int ObSSEvictMicroOp::evict_persisted_cold_micro_blocks(
    const int64_t seg_idx,
    const ObSSARCInfo &arc_info,
    bool &finish_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(seg_idx != SS_ARC_B1 && seg_idx != SS_ARC_B2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);

    ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr()->get_init_range_arr();
    const int64_t init_range_cnt = micro_range_mgr()->get_init_range_cnt();
    const bool is_in_b1 = (seg_idx == SS_ARC_B1);
    int64_t total_delete_ghost_cnt = 0;
    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && (is_op_enabled()) && (!finish_handle) && (i < init_range_cnt); ++i) {
      ObSSMicroInitRangeInfo *init_range = init_range_arr[i];
      if (OB_ISNULL(init_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("init_range is nullptr", KR(ret), K(i), K(init_range_cnt), KP(init_range_arr));
      } else {
        bool go_next_init_rng = false;
        int64_t max_loop_cnt = MIN(init_range->get_sub_range_cnt(), UINT32_MAX);
        do {
          ObSSMicroSubRangeHandle sub_rng_handle;
          int64_t delete_ghost_cnt = 0;
          if (OB_FAIL(init_range->get_persisted_sub_range(seg_idx, sub_rng_handle))) {
            LOG_WARN("fail to get persisted sub_range", KR(ret), K(i), K(seg_idx), KPC(init_range), K(arc_info));
          } else if (sub_rng_handle.is_valid()) {
            if (OB_FAIL(micro_meta_mgr()->try_delete_ghost_persisted_micro_meta(init_range, sub_rng_handle.get_ptr(),
                is_in_b1, delete_ghost_cnt))) {
              LOG_WARN("fail to try delete ghost persisted micro_meta", KR(ret), K(i), K(seg_idx), K(is_in_b1), 
                KPC(init_range), KPC(sub_rng_handle.get_ptr()), K(arc_info));
            } else {
              total_delete_ghost_cnt += delete_ghost_cnt;
              if (total_delete_ghost_cnt >= arc_iter_info_.get_op_micro_cnt(seg_idx)) {
                finish_handle = true;
                go_next_init_rng = true;
              }
            }
          } else {
            go_next_init_rng = true;
          }
          --max_loop_cnt;
        } while (OB_SUCC(ret) && (!go_next_init_rng) && (max_loop_cnt > 0));
      }
    }

    if (OB_SUCC(ret) && (i >= init_range_cnt)) {
      finish_handle = true;
    }
    LOG_TRACE("finish handle some persisted cold micro", K(finish_handle), K(seg_idx), K_(arc_iter_info));
  }
  return ret;
}

int ObSSEvictMicroOp::evict_mem_cold_micro_blocks(
    const int64_t seg_idx,
    const ObSSARCInfo &arc_info,
    SSHandleSegEvictionCtx &seg_eviction_ctx,
    bool &finish_handle)
{
  int ret = OB_SUCCESS;
  arc_iter_info_.adjust_arc_iter_seg_info(seg_idx);
  if (OB_FAIL(micro_meta_mgr()->acquire_cold_micro_blocks(arc_iter_info_, seg_idx))) {
    LOG_WARN("fail to acquire cold micro_blocks", KR(ret), K(seg_idx), K_(arc_iter_info), K(arc_info),
      K(seg_eviction_ctx));
  } else {
    finish_handle = (arc_iter_info_.get_obtained_micro_cnt(seg_idx) < arc_iter_info_.get_op_micro_cnt(seg_idx));
    if (arc_iter_info_.need_handle_cold_micro(seg_idx)) {
      if (OB_FAIL(handle_cold_micro_blocks(seg_idx, seg_eviction_ctx))) {
        LOG_WARN("fail to handle cold micro_blocks", KR(ret), K(seg_idx), K_(arc_iter_info), K(seg_eviction_ctx));
      }
    }
    LOG_TRACE("finish handle some cold micro", K(finish_handle), K(seg_idx), K_(arc_iter_info));
  }
  return ret;
}

int ObSSEvictMicroOp::handle_cold_micro_blocks(const int64_t seg_idx, SSHandleSegEvictionCtx &seg_eviction_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  
  const bool to_delete = arc_iter_info_.is_delete_op_type(seg_idx);
  while (OB_SUCC(ret) && arc_iter_info_.need_handle_cold_micro(seg_idx)) {
    ObSSMicroBlockMetaInfo cold_micro_info;
    if (OB_FAIL(arc_iter_info_.pop_cold_micro(seg_idx, cold_micro_info))) {
      LOG_WARN("fail to pop cold micro_meta_handle", KR(ret), K_(arc_iter_info), K(seg_idx));
    } else if (OB_FAIL(handle_cold_micro_block(seg_idx, cold_micro_info, to_delete, seg_eviction_ctx))) {
      LOG_WARN("fail to handle cold micro_block", KR(ret), K(seg_idx), K(cold_micro_info), K(to_delete), 
        K_(arc_iter_info), K(seg_eviction_ctx));
    }
  }
  return ret;
}

int ObSSEvictMicroOp::handle_cold_micro_block(
    const int64_t seg_idx,
    const ObSSMicroBlockMetaInfo &cold_micro_info,
    const bool to_delete,
    SSHandleSegEvictionCtx &seg_eviction_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    if (to_delete && OB_FAIL(micro_meta_mgr()->try_delete_micro_block_meta(cold_micro_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // skip it
      } else {
        LOG_WARN("fail to try delete micro_block meta", KR(ret), K(cold_micro_info));
      }
    } else if (!to_delete && OB_FAIL(micro_meta_mgr()->try_evict_micro_block_meta(cold_micro_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // skip it
      } else {
        LOG_WARN("fail to try evict micro_block meta", KR(ret), K(cold_micro_info));
      }
    } else {
      const int32_t micro_size = cold_micro_info.size();
      seg_eviction_ctx.update_handle_info(1, micro_size);
      if (!to_delete) {
        const int64_t delta_size = micro_size * -1;
        const uint64_t data_loc = cold_micro_info.data_loc();
        const uint64_t reuse_version = cold_micro_info.reuse_version();
        int64_t phy_blk_idx = -1;
        if (OB_TMP_FAIL(phy_blk_mgr()->update_block_valid_length(data_loc, reuse_version, delta_size, phy_blk_idx))) {
          if (OB_S2_REUSE_VERSION_MISMATCH == tmp_ret) {
            tmp_ret = OB_SUCCESS;
            LOG_TRACE("fail to update phy_block valid length cuz reuse_version mismatch", K(data_loc), K(reuse_version),
              K(delta_size));
          } else {
            LOG_ERROR("fail to update phy_block valid length", KR(tmp_ret), K(cold_micro_info), K(data_loc), 
              K(reuse_version), K(delta_size), K(phy_blk_idx));
          }
        }
      }
      // decrease iter_info's seg_op count
      if (FAILEDx(arc_iter_info_.finish_handle_cold_micro(seg_idx))) {
        LOG_WARN("fail to finish handle cold_micro", KR(ret), K(seg_idx), K(to_delete), K(cold_micro_info));
      }
    }
  }
  return ret;
}

void ObSSEvictMicroOp::clear_for_next_round()
{
  eviction_ctx_.reset();
}

} // storage
} // oceanbase
