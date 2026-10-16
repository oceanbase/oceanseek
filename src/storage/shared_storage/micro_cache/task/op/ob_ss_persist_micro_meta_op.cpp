/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_meta_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObSSPersistMicroMetaOp::ObSSPersistMicroMetaOp()
  : ObSSDoCheckpointBaseOp(), enable_save_meta_memory_(false), trigger_type_(0), retry_duration_us_(0),
    micro_ckpt_ctx_(), tablet_info_map_(), full_ckpt_item_cnt_(0), persist_range_arr_(), handled_range_idx_arr_()
{}

void ObSSPersistMicroMetaOp::destroy()
{
  enable_save_meta_memory_ = false;
  trigger_type_ = 0;
  retry_duration_us_ = 0;
  micro_ckpt_ctx_.reset();
  tablet_info_map_.destroy();
  full_ckpt_item_cnt_ = 0;
  persist_range_arr_.reset();
  handled_range_idx_arr_.reset();
  ObSSDoCheckpointBaseOp::destroy();
}

void ObSSPersistMicroMetaOp::enable_replay_ckpt()
{
  micro_ckpt_ctx_.enable_replay_ckpt();
}

int ObSSPersistMicroMetaOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = DEFAULT_MAP_BUCKET_NUM;
  ObMemAttr buf_attr(tenant_id, "SSMicroItem");
  uint32_t ckpt_split_cnt = 0;
  if (OB_FAIL(ObSSDoCheckpointBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_ctx should be valid", KR(ret), KP_(task_ctx));
  } else if (OB_FAIL(tablet_info_map_.create(bucket_num, ObMemAttr(tenant_id, "SSTabletInfoMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id), K(bucket_num));
  } else if (FALSE_IT(item_buf_size_ = ObSSMicroBlockMetaInfo::get_max_serialize_size())) {
  } else if (OB_ISNULL(item_buf_ = static_cast<char *>(task_allocator()->alloc(item_buf_size_, buf_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(item_buf_size), K(buf_attr));
  } else if (OB_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(ckpt_split_cnt))) {
    LOG_WARN("fail to get micro_ckpt_split_cnt", KR(ret));
  } else if (OB_FAIL(micro_ckpt_ctx_.handle_init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to init micro_ckpt_ctx", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    retry_duration_us_ = SS_PERSIST_META_MAX_RETRY_DURATION_US;
    uint32_t micro_ckpt_rng_cnt = 0;
    if (OB_FAIL(micro_range_mgr()->get_range_cnt_per_ckpt_round(micro_ckpt_rng_cnt))) {
      LOG_WARN("fail to get range_cnt_per_ckpt_round", KR(ret));
    } else if (OB_UNLIKELY(micro_ckpt_rng_cnt == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_ckpt range_cnt is invalid", KR(ret), K(micro_ckpt_rng_cnt));
    } else {
      handled_range_idx_arr_.reserve(micro_ckpt_rng_cnt); // ignore ret
      op_type_ = ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_META_OP;
      update_latest_expiration_time();
      ckpt_ctx_ = &micro_ckpt_ctx_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::start_op()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSDoCheckpointBaseOp::start_op())) {
    LOG_WARN("fail to check start ckpt operation", KR(ret), K_(micro_ckpt_ctx));
  } else {
    enable_save_meta_memory_ = micro_meta_mgr()->enable_save_meta_memory();
    trigger_type_ = 0;
    handled_range_idx_arr_.reuse();
    micro_ckpt_ctx_.need_ckpt_ = check_need_ckpt();
    if (micro_ckpt_ctx_.complete_full_ckpt()) {
      tablet_info_map_.clear();
      full_ckpt_item_cnt_ = 0;
      cache_stat()->task_stat().set_erase_persisted_cnt(0);
    }
  }
  return ret;
}

bool ObSSPersistMicroMetaOp::check_need_ckpt()
{
  bool need_ckpt = false;
  // 1. check micro_ckpt execute interval
  if (micro_ckpt_ctx_.complete_full_ckpt()) {
    need_ckpt = micro_ckpt_ctx_.need_exe_full_ckpt();
    trigger_type_ = (need_ckpt ? 1 : 0);
  } else {
    need_ckpt = micro_ckpt_ctx_.need_exe_part_ckpt();
    trigger_type_ = (need_ckpt ? 2 : 0);
  }

  // 2. check micro_cache TTL interval
  if (!need_ckpt) {
    need_ckpt = micro_ckpt_ctx_.need_handle_expiration();
    trigger_type_ = (need_ckpt ? 3 : 0);
  }

  const int64_t valid_micro_cnt = cache_stat()->micro_stat().get_valid_micro_cnt();
  // 3. check memory ghost micro_meta count
  if (!need_ckpt) {
    const int64_t ghost_micro_cnt = cache_stat()->micro_stat().get_ghost_micro_cnt();
    const int64_t TRIGGER_CLEAN_PCT = 30;
    const int64_t target_ghost_micro_cnt = valid_micro_cnt * TRIGGER_CLEAN_PCT / 100;
    need_ckpt = ((target_ghost_micro_cnt > 0) && (ghost_micro_cnt >= target_ghost_micro_cnt));
    trigger_type_ = (need_ckpt ? 4 : 0);
  }

  // 4. check memory usage
  if (!need_ckpt) {
    const int64_t deleted_micro_cnt = cache_stat()->micro_stat().get_deleted_micro_cnt();
    const int64_t TRIGGER_CLEAN_PCT = 30;
    need_ckpt = ((deleted_micro_cnt > 0) && (deleted_micro_cnt >= valid_micro_cnt * TRIGGER_CLEAN_PCT / 100));
    trigger_type_ = (need_ckpt ? 5 : 0);
  }

  // 5. check if memory usage reaches 90% of limit
  if (!need_ckpt) {
    const int64_t total_micro_cnt = cache_stat()->micro_stat().get_total_micro_cnt();
    const int64_t micro_cnt_limit = cache_stat()->mem_stat().micro_cnt_limit_;
    const int64_t threshold = micro_cnt_limit * SS_MEM_LIMIT_TRIGGER_CKPT_PCT / 100;
    need_ckpt = (micro_cnt_limit > 0 && total_micro_cnt >= threshold);
    trigger_type_ = (need_ckpt ? 6 : 0);
  }

  // 6. check if there are tablet_ids that need to be deleted
  if (!need_ckpt) {
    const int64_t delete_tablet_cnt = cache_stat()->micro_stat().get_pending_delete_tablet_cnt();
    need_ckpt = (delete_tablet_cnt > 0);
    trigger_type_ = (need_ckpt ? 7 : 0);
  }
  
  return need_ckpt;
}

int ObSSPersistMicroMetaOp::gen_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    if (OB_TMP_FAIL(check_split_cnt_changed())) { // check and update parameters if ckpt_split_cnt changed
      LOG_WARN("fail to check and update params when ckpt_split_cnt changed", KR(tmp_ret),
        K(micro_ckpt_ctx_.cur_ckpt_split_cnt_));
      tmp_ret = OB_SUCCESS;
    } 
  
    if (micro_ckpt_ctx_.need_ckpt()) {
      LOG_INFO("ss_micro_cache: start ss_micro_cache micro ckpt", "micro_ckpt_cnt", cache_stat()->task_stat_.micro_ckpt_cnt_,
        "cur_exe_idx", micro_ckpt_ctx_.get_cur_ckpt_exe_idx());
      micro_ckpt_ctx_.start_ckpt();

      bool succ_ckpt = false;
      if (OB_FAIL(gen_micro_meta_checkpoint())) {
        LOG_WARN("fail to try gen micro_meta checkpoint", KR(ret));
      } else if (OB_FAIL(update_super_block(succ_ckpt))) {
        LOG_WARN("fail to update super_block", KR(ret));
      } else {
        // update cache_stat
        cache_stat()->task_stat_.update_micro_ckpt_op_cnt(1, micro_ckpt_ctx_.ckpt_item_cnt_);
        full_ckpt_item_cnt_ += micro_ckpt_ctx_.ckpt_item_cnt_;
        if (micro_ckpt_ctx_.get_cur_ckpt_exe_idx() + 1 >= micro_ckpt_ctx_.get_cur_ckpt_split_cnt()) {
          // complete a full micro_meta ckpt
          cache_stat()->task_stat_.update_micro_ckpt_cnt(1, full_ckpt_item_cnt_);
        }
      }
      
      // Check and cleanup tablets pending for deletion after updating micro_ckpt_op_cnt_
      const int64_t cur_micro_ckpt_op_cnt = cache_stat()->task_stat().get_micro_ckpt_op_cnt();
      uint32_t meta_ckpt_split_cnt = 0;
      if (OB_TMP_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(meta_ckpt_split_cnt))) {
        LOG_WARN("fail to get micro_ckpt_split_cnt", KR(tmp_ret));
        tmp_ret = OB_SUCCESS;
      } else if (OB_TMP_FAIL(micro_meta_mgr()->check_and_cleanup_pending_delete_tablets(cur_micro_ckpt_op_cnt, meta_ckpt_split_cnt))) {
        LOG_WARN("fail to check and cleanup tablets pending for deletion", KR(tmp_ret), K(cur_micro_ckpt_op_cnt), K(meta_ckpt_split_cnt));
        tmp_ret = OB_SUCCESS;
      }

      if (OB_TMP_FAIL(try_free_phy_blocks(succ_ckpt))) {
        LOG_WARN("fail to try free phy_blocks", KR(tmp_ret), K(succ_ckpt));
        tmp_ret = OB_SUCCESS;
      } else if (!succ_ckpt) {
        handle_meta_ckpt_failure();
      }

      micro_ckpt_ctx_.finish_ckpt();
      LOG_INFO("ss_micro_cache: finish ss_micro_cache micro ckpt", KR(ret), KR(tmp_ret), K_(trigger_type), K(micro_ckpt_ctx_.ckpt_cost_us()),
        K(micro_ckpt_ctx_.ser_item_cnt()), K(micro_ckpt_ctx_.get_cur_ckpt_exe_idx()),
        "erase_cnt", cache_stat()->task_stat_.erase_persisted_cnt_,
        "micro_ckpt_cnt", cache_stat()->task_stat_.micro_ckpt_cnt_,
        "micro_stat", cache_stat()->micro_stat_);
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::gen_micro_meta_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  const int64_t start_us = ObTimeUtility::current_time_us();
  ObCompressorType compressor_type = ObCompressorType::INVALID_COMPRESSOR;
  ObSSCkptPhyBlockItemWriter item_writer;
  inner_reset_persist_range_arr();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret), KP(micro_cache));
  } else if (FALSE_IT(compressor_type = micro_cache->get_micro_meta_compressor_type())) {
  } else if (OB_FAIL(item_writer.init(tenant_id_, *(phy_blk_mgr()), ObSSPhyBlockType::SS_MICRO_META_BLK,
             compressor_type))) {
    LOG_WARN("fail to init ckpt phy_block item writer", KR(ret), K_(tenant_id), K(compressor_type));
  } else if (is_op_enabled()) {
    if (OB_FAIL(handle_and_persist_micro_meta(item_writer))) {
      LOG_WARN("fail to handle and persist micro_meta", KR(ret), K(compressor_type), K_(micro_ckpt_ctx));
    } else {
      micro_ckpt_ctx_.cur_super_blk_.update_micro_ckpt_time(ObTimeUtility::current_time_us());
      if (micro_ckpt_ctx_.get_cur_ckpt_exe_idx() + 1 >= micro_ckpt_ctx_.get_cur_ckpt_split_cnt()) {
        // complete a full micro_meta ckpt
        if (OB_TMP_FAIL(ObSSMicroCacheUtil::calc_ls_tablet_cache_info(tenant_id_, tablet_info_map_,
            micro_ckpt_ctx_.cur_super_blk_.tablet_info_list_, micro_ckpt_ctx_.cur_super_blk_.ls_info_list_))) {
          LOG_WARN("fail to calc ls/tablet cache info", KR(tmp_ret), K_(tenant_id), K_(micro_ckpt_ctx));
          tmp_ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::handle_and_persist_micro_meta(ObSSCkptPhyBlockItemWriter &item_writer)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);

  ObSEArray<ObLSHandle, 16> ls_handles;
  if (OB_TMP_FAIL(ObSSMicroCacheUtil::get_ls_handles(ls_handles))) {
    LOG_WARN("fail to get ls_handles", KR(tmp_ret));
    tmp_ret = OB_SUCCESS;
  }
  
  // handle range_idx in [start_range_idx, end_range_idx]
  const int64_t cur_exe_idx = micro_ckpt_ctx_.get_cur_ckpt_exe_idx();
  int64_t start_range_idx = -1;
  int64_t end_range_idx = -1;
  const uint32_t curr_ckpt_split_cnt = micro_ckpt_ctx_.get_cur_ckpt_split_cnt();
  if (OB_FAIL(micro_range_mgr()->calc_ckpt_exe_range_idx(curr_ckpt_split_cnt, cur_exe_idx, start_range_idx, end_range_idx))) {
    LOG_WARN("fail to calc ckpt exe_range idx", KR(ret), K(cur_exe_idx));
  } else if (OB_UNLIKELY(start_range_idx > end_range_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected start/end range_idx", KR(ret), K(cur_exe_idx), K(start_range_idx), K(end_range_idx));
  }

  int64_t cur_start_range_idx = start_range_idx;
  int64_t handled_range_idx = -1;
  bool is_empty_range = true;
  while (OB_SUCC(ret) && is_op_enabled() && (cur_start_range_idx <= end_range_idx)) {
    if (OB_FAIL(micro_meta_mgr()->scan_ranges_micro_block_meta(tablet_info_map_, ls_handles, cur_start_range_idx,
        end_range_idx, handled_range_idx, is_empty_range))) {
      LOG_WARN("fail to scan micro_blocks", KR(ret), K(start_range_idx), K(end_range_idx), K(cur_start_range_idx),
        K(handled_range_idx));
    } else if (OB_UNLIKELY(handled_range_idx < cur_start_range_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected handled_range_idx", KR(ret), K(handled_range_idx), K(cur_start_range_idx), K(end_range_idx));
    } else if (!is_empty_range) {
      if (OB_TMP_FAIL(handled_range_idx_arr_.push_back(handled_range_idx))) {
        LOG_WARN("fail to push back", KR(ret), KR(tmp_ret), K(handled_range_idx), K(handled_range_idx_arr_.count()));
      } else if (OB_TMP_FAIL(write_batch_micro_meta(item_writer, handled_range_idx))) {
        LOG_WARN("fail to write batch micro_meta", KR(ret), KR(tmp_ret), K(handled_range_idx));
      }

      if (OB_TMP_FAIL(tmp_ret)) {
        LOG_INFO("skip this init_range micro_meta persisted", K(handled_range_idx));
        tmp_ret = OB_SUCCESS;
      }
    }

    cur_start_range_idx = MAX(cur_start_range_idx, handled_range_idx) + 1; // in case previous function failed
    handled_range_idx = -1;
    is_empty_range = true;
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && OB_TMP_FAIL(item_writer.close())) {
    if (OB_EAGAIN == tmp_ret) {
      micro_ckpt_ctx_.lack_phy_blk_ = true;
      LOG_WARN("micro_ckpt phy_block is not enough", KR(tmp_ret), K_(micro_ckpt_ctx));
    } else {
      LOG_WARN("fail to close item writer", KR(tmp_ret), K_(micro_ckpt_ctx));
    }
    tmp_ret = OB_SUCCESS;
  } else if (FALSE_IT(micro_ckpt_ctx_.ckpt_item_cnt_ += micro_ckpt_ctx_.ser_item_cnt())) {
  } else if (persist_range_arr_.count() > 0) {
    if (OB_TMP_FAIL(handle_persisted_sub_ranges(item_writer))) {
      LOG_WARN("fail to update persisted range info", KR(tmp_ret), K_(micro_ckpt_ctx));
      tmp_ret = OB_SUCCESS;
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(handle_block_id_list(item_writer))) {
    LOG_WARN("fail to handle block_id list", KR(ret), KR(tmp_ret));
    tmp_ret = OB_SUCCESS;
  }

  ls_handles.reset();
  return ret;
}

int ObSSPersistMicroMetaOp::handle_block_id_list(ObSSCkptPhyBlockItemWriter &item_writer)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t cur_exe_idx = micro_ckpt_ctx_.get_cur_ckpt_exe_idx();
  if (OB_UNLIKELY(cur_exe_idx >= micro_ckpt_ctx_.cur_super_blk_.get_micro_ckpt_used_blk_list_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur exe_idx is invalid", KR(ret), K(cur_exe_idx), K_(micro_ckpt_ctx));
  } else if (OB_FAIL(get_ckpt_block_id_list(item_writer,
             micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_used_blk_list_by_idx(cur_exe_idx), false))) {
    LOG_ERROR("fail to get ckpt block_id list", KR(ret), K(cur_exe_idx), K_(micro_ckpt_ctx));
  } else if (OB_FAIL(micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_info_.update_entry_info_by_idx(cur_exe_idx))) {
    LOG_WARN("fail to update micro_ckpt entry info by idx", KR(ret), K(cur_exe_idx), K_(micro_ckpt_ctx));
  }

  if (OB_TMP_FAIL(get_ckpt_block_id_list(item_writer, micro_ckpt_ctx_.failed_blk_id_list_, true))) {
    LOG_ERROR("fail to get failed_block_id list", KR(tmp_ret), K_(micro_ckpt_ctx));
    tmp_ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSPersistMicroMetaOp::write_batch_micro_meta(
    ObSSCkptPhyBlockItemWriter &item_writer,
    const int64_t init_range_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_range_mgr()->is_valid_init_range_idx(init_range_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init range index", KR(ret), K(init_range_idx));
  } else {
    ObSSMicroInitRangeInfo *init_range = micro_range_mgr()->get_init_range_arr()[init_range_idx];
    if (OB_ISNULL(init_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("init_range should not be null", KR(ret), K(init_range_idx));
    } else {
      ObSSMicroSubRangeInfo *cur_sub_range = init_range->get_first_sub_range();
      bool stop_write = false;
      while (OB_SUCC(ret) && (nullptr != cur_sub_range) && (!stop_write)) {
        if (OB_FAIL(write_single_range_micro_meta(item_writer, init_range, cur_sub_range))) {
          if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_EAGAIN == ret) {
            LOG_WARN("lack of memory or phy_block, skip this init_range micro_meta persisted", KR(ret), K(init_range_idx));
            stop_write = true;
          } else {
            LOG_WARN("fail to write single range micro_meta", KR(ret), KP(cur_sub_range), K(persist_range_arr_.count()));
          }
        }
        ret = OB_SUCCESS; // continue to handle next sub_range
        cur_sub_range = cur_sub_range->get_next_range();
      }
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::write_single_range_micro_meta(
    ObSSCkptPhyBlockItemWriter &item_writer,
    ObSSMicroInitRangeInfo *init_range,
    ObSSMicroSubRangeInfo *sub_range)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroSubRangeInfo sub_range_snapshot;
  if (OB_ISNULL(init_range) || OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(init_range), KP(sub_range));
  } else if (OB_FAIL(init_range->get_sub_range_snapshot_info(sub_range, sub_range_snapshot))) {
    LOG_WARN("fail to get sub_range snapshot info", KR(ret), KPC(sub_range));
  } else if (!sub_range_snapshot.has_micro_meta()) { // skip it
  } else {
    ObSSPersistedSubRangeMicroInfo cur_rng_info;
    cur_rng_info.set_sub_range(sub_range);

    ObSSMicroRangePersistScanFilter persist_meta_filter;
    ObSSMicroBlockMeta *cur_micro_meta = sub_range_snapshot.get_first_micro_meta();
    int64_t ser_item_cnt = 0;
    uint32_t iter_meta_cnt = 0;
    while (OB_SUCC(ret) && (nullptr != cur_micro_meta) && (ser_item_cnt < SS_SUB_RNG_PERSIST_META_MAX_CNT) &&
          (iter_meta_cnt < UINT32_MAX)) {
      ObSSMicroBlockMetaInfo ser_micro_info;
      bool is_item_serialized = false;
      if (OB_FAIL(do_persist_micro_meta(cur_micro_meta, item_writer, persist_meta_filter, ser_micro_info,
          is_item_serialized))) {
        LOG_WARN("fail to do persist micro_meta", KR(ret), KPC(cur_micro_meta), K(iter_meta_cnt), K(ser_item_cnt));
      } else if (is_item_serialized) {
        const ObSSMicroBlockCacheKey &cur_micro_key = cur_micro_meta->get_micro_key();
        const uint32_t key_hval = cur_micro_key.micro_hash();
        if (OB_FAIL(cur_rng_info.add_serialized_micro_key(cur_micro_key))) {
          LOG_WARN("fail to push back", KR(ret), KPC(cur_micro_meta), K(cur_rng_info));
        } else {
          ++ser_item_cnt;
          const bool is_in_l1 = ser_micro_info.is_in_l1();
          const bool is_in_ghost = ser_micro_info.is_in_ghost();
          const int64_t micro_size = ser_micro_info.size();
          cur_rng_info.all_pmeta_info_.update_seg_meta_info(is_in_l1, is_in_ghost, 1, micro_size);
          if (is_in_ghost) {
            IGNORE_RETURN cur_rng_info.ghost_hval_arr_.push_back(key_hval);
          }
        }
      }
      ++iter_meta_cnt;
      cur_micro_meta = cur_micro_meta->get_next_micro();
    }
    ret = OB_SUCCESS; // ignore ret

    if (ser_item_cnt > 0) {
      bool presist_prev_seg = false;
      int64_t cur_seg_offset = -1;
      int64_t cur_seg_length = 0;
      if (OB_FAIL(item_writer.close_segment(presist_prev_seg, cur_seg_offset, cur_seg_length))) {
        LOG_WARN("fail to close segment", KR(ret), K(ser_item_cnt), K(presist_prev_seg), KPC(sub_range));
      } else if (OB_UNLIKELY(cur_seg_offset <= 0 || cur_seg_length <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected segment offset and length",
          KR(ret), K(ser_item_cnt), K(cur_seg_offset), K(cur_seg_length), KPC(sub_range));
      } else if (presist_prev_seg) {
        micro_ckpt_ctx_.ckpt_item_cnt_ += micro_ckpt_ctx_.ser_item_cnt();
        micro_ckpt_ctx_.ser_item_cnt_ = ser_item_cnt;
        if (OB_TMP_FAIL(handle_persisted_sub_ranges(item_writer))) {
          LOG_WARN("fail to handle persisted sub_ranges", KR(tmp_ret), K_(micro_ckpt_ctx));
          tmp_ret = OB_SUCCESS;
        }
      } else {
        micro_ckpt_ctx_.ser_item_cnt_ += ser_item_cnt;
      }

      // update sub_range offset and length under init_range lock to avoid bitfield race condition
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(cur_rng_info.set_offset_length(cur_seg_offset, cur_seg_length))) {
      } else if (OB_FAIL(persist_range_arr_.push_back(cur_rng_info))) {
        LOG_WARN("fail to push back", KR(ret), K(cur_rng_info));
      }
    }

    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
      inner_reset_persist_range_arr();
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::do_persist_micro_meta(
    ObSSMicroBlockMeta *micro_meta,
    ObSSCkptPhyBlockItemWriter &item_writer,
    ObSSMicroRangeScanFilter &filter,
    ObSSMicroBlockMetaInfo &ser_micro_info,
    bool &is_item_serialized)
{
  int ret = OB_SUCCESS;
  is_item_serialized = false;
  if (OB_ISNULL(micro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_meta));
  } else if (OB_ISNULL(item_buf_) || OB_UNLIKELY(item_buf_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item buf/size is invalid", KR(ret), KP_(item_buf), K_(item_buf_size));
  } else {
    bool need_serialize = false;
    // NOTICE: we will set 'is_meta_serialized=tue' in advance, thus there may exist error when serialize 
    //         meta or write ckpt block. It doesn't matter. Cuz a 'serialized' micro_meta can become 'persisted'
    //         only when its sub_range has physical location.
    if (OB_FAIL(micro_meta_mgr()->get_micro_meta_info_for_serialize(micro_meta->get_micro_key(), filter,
        ser_micro_info, need_serialize))) {
      LOG_WARN("fail to get micro_meta info for serialize", KR(ret), KPC(micro_meta));
    } else if (need_serialize) {
      int64_t pos = 0;
      if (OB_FAIL(ser_micro_info.serialize(item_buf_, item_buf_size_, pos))) {
        LOG_WARN("fail to serialize micro_block meta", KR(ret), KP_(item_buf), K_(item_buf_size), K(ser_micro_info));
      } else if (OB_FAIL(item_writer.write_item(item_buf_, pos))) {
        if (OB_EAGAIN == ret) {
          micro_ckpt_ctx_.lack_phy_blk_ = true;
          LOG_WARN("micro_ckpt phy_block is not enough", KR(ret), KP_(item_buf), K_(item_buf_size), K(ser_micro_info),
            K_(micro_ckpt_ctx));
        } else {
          LOG_WARN("fail to write micro_ckpt item", KR(ret), K(pos), KP_(item_buf), K_(item_buf_size), K(ser_micro_info),
            K_(micro_ckpt_ctx));
        }
      } else {
        is_item_serialized = true;
      }
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::handle_persisted_sub_ranges(ObSSCkptPhyBlockItemWriter &item_writer)
{
  int ret = OB_SUCCESS;
  const int64_t persist_blk_idx = item_writer.get_latest_block_id();
  if (OB_FAIL(update_sub_ranges_persisted_info(persist_blk_idx))) {
    LOG_WARN("fail to update sub_ranges persisted info", KR(ret), K(persist_blk_idx));
  } else if (OB_FAIL(handle_persisted_micro_meta())) {
    LOG_WARN("fail to handle persisted micro_meta", KR(ret));
  }
  inner_reset_persist_range_arr();
  return ret;
}

int ObSSPersistMicroMetaOp::build_sub_range_bloom_filter(
    ObSSMicroSubRangeInfo *sub_range,
    const ObIArray<uint32_t> &ghost_hval_arr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(sub_range)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sub_range));
  } else if (enable_save_meta_memory_) {
    const int64_t ghost_hval_cnt = ghost_hval_arr.count();
    if (OB_UNLIKELY(sub_range->exist_bloom_filter())) {
      // for logic check
      LOG_WARN("sub_range already has bloom_filter", KP(sub_range), KPC(sub_range));
    } else if (ghost_hval_cnt > 0) {
      if (OB_FAIL(micro_range_mgr()->create_sub_range_bloom_filter(sub_range, ghost_hval_cnt, false))) {
        LOG_WARN("fail to create sub_range bloom filter", KR(ret), K(ghost_hval_cnt), KP(sub_range), KPC(sub_range));
      } else {
        for (int64_t i = 0; i < ghost_hval_cnt; ++i) {
          if (OB_TMP_FAIL(sub_range->add_hval_into_bloom_filter(ghost_hval_arr.at(i)))) {
            LOG_WARN("fail to add hval into bloom_filter", KR(tmp_ret), K(i), K(ghost_hval_cnt), K(ghost_hval_arr.at(i)),
              KPC(sub_range));
            tmp_ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::update_sub_ranges_persisted_info(const int64_t blk_idx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t rng_cnt = persist_range_arr_.count();
  if (OB_UNLIKELY(blk_idx < SS_SUPER_BLK_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(blk_idx));
  } else if (rng_cnt > 0) {
    ObSSPhyBlockHandle phy_blk_handle;
    uint64_t reuse_version = 0;
    if (OB_FAIL(phy_blk_mgr()->get_block_handle(blk_idx, phy_blk_handle))) {
      LOG_WARN("fail to get phy_block handle", KR(ret), K(blk_idx));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle should be valid", KR(ret), K(blk_idx), K(phy_blk_handle));
    } else {
      reuse_version = phy_blk_handle()->reuse_version_;
    }
    
    ObSSMicroInitRangeInfo *init_range = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && (i < rng_cnt); ++i) {
      ObSSPersistedSubRangeMicroInfo &p_range_info = persist_range_arr_.at(i);
      ObSSMicroSubRangeInfo *sub_range = p_range_info.get_sub_range();
      init_range = nullptr;
      uint32_t rng_hval = 0;
      if (OB_UNLIKELY(!p_range_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("persisted_rng_info must be valid", KR(ret), K(i), K(p_range_info), K(rng_cnt));
      } else if (FALSE_IT(rng_hval = sub_range->get_end_hval())) {
      } else if (OB_FAIL(micro_range_mgr()->get_init_range_by_hval(rng_hval, init_range))) {
        LOG_WARN("fail to get init_range by hval", KR(ret), K(i), K(rng_hval), KPC(sub_range));
      } else if (OB_ISNULL(init_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("init_range should not be null", KR(ret), K(i), K(rng_hval), KP(init_range));
      } else if (OB_FAIL(init_range->update_sub_range_persisted_info(sub_range, blk_idx, reuse_version,
                 p_range_info.offset_, p_range_info.length_, p_range_info.all_pmeta_info_))) {
        LOG_WARN("fail to update sub_range persisted info", KR(ret), K(i), K(blk_idx), K(reuse_version),
          K(p_range_info), KP(sub_range));
      } else if (OB_TMP_FAIL(build_sub_range_bloom_filter(sub_range, p_range_info.ghost_hval_arr_))) {
        LOG_WARN("fail to build sub_range bloom filter", KR(tmp_ret), K(i), KP(sub_range), K(p_range_info), KPC(sub_range));
        tmp_ret = OB_SUCCESS;
      }
      ret = OB_SUCCESS; // Continue to handle next sub_range
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::handle_persisted_micro_meta()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t rng_cnt = persist_range_arr_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < rng_cnt); ++i) {
    ObSSPersistedSubRangeMicroInfo &persisted_rng_info = persist_range_arr_.at(i);
    ObSSMicroSubRangeInfo *sub_range = persisted_rng_info.get_sub_range();
    const int64_t ser_micro_cnt = persisted_rng_info.get_serialized_micro_cnt();

    if (OB_UNLIKELY(!persisted_rng_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("persisted_rng_info must be valid", KR(ret), K(i), K(persisted_rng_info), K(rng_cnt));
    } else if (OB_LIKELY(ser_micro_cnt > 0)) {
      if (enable_save_meta_memory_) {
        int64_t force_erase_cnt = 0;
        const uint32_t rng_hval = sub_range->get_end_hval();
        ObSSMicroSubRangeInfo sub_range_snapshot; // Use snapshot to read read-only fields
        ObSSMicroInitRangeInfo *init_range = nullptr;
        if (OB_FAIL(micro_range_mgr()->get_init_range_by_hval(rng_hval, init_range))) {
          LOG_WARN("fail to get init_range by hval", KR(ret), K(i), K(rng_hval), KPC(sub_range));
        } else if (OB_ISNULL(init_range)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("init_range should not be null", KR(ret), K(i), K(rng_hval), KP(init_range));
        } else if (OB_FAIL(init_range->get_sub_range_snapshot_info(sub_range, sub_range_snapshot))) {
          LOG_WARN("fail to get sub_range snapshot", KR(ret), KPC(sub_range));
        } else if (OB_UNLIKELY(!sub_range_snapshot.exist_persisted_meta())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sub_range must have persisted micro_meta", KR(ret), K(sub_range_snapshot), KPC(sub_range));
        } else if (OB_TMP_FAIL(micro_meta_mgr()->try_erase_persisted_micro_metas(sub_range,
                   persisted_rng_info.serialized_micro_keys_, force_erase_cnt))) {
          LOG_WARN("fail to try force erase persisted micro_metas", KR(tmp_ret), K(i), K(persisted_rng_info),
            KPC(persisted_rng_info.sub_range_));
          tmp_ret = OB_SUCCESS;
        }

        if (OB_FAIL(ret)) {
        } else {
          if (force_erase_cnt > 0) {
            cache_stat()->task_stat().update_erase_persisted_cnt(force_erase_cnt);
          }

          const bool all_mem_meta = (0 == force_erase_cnt);
          if (OB_FAIL(init_range->update_sub_range_all_mem_meta(sub_range, all_mem_meta))) {
            LOG_ERROR("fail to update sub_range all_mem_meta", KR(ret), K(i), K(persisted_rng_info), K(all_mem_meta),
              KPC(persisted_rng_info.sub_range_));
          } else if (!all_mem_meta) {
            cache_stat()->range_stat().update_fe_sub_range_cnt(1);
          }
        }
      } else {
        if (OB_TMP_FAIL(micro_meta_mgr()->try_update_persisted_micro_meta(persisted_rng_info.serialized_micro_keys_))) {
          LOG_WARN("fail to try update persisted micro_meta", KR(tmp_ret), K(i), K(persisted_rng_info));
        }
      }
    } else {
      LOG_TRACE("no persisted micro_meta", KR(ret), K(i), K(persisted_rng_info));
    }
    ret = OB_SUCCESS; // Continue to handle next sub_range
  }
  return ret;
}

int ObSSPersistMicroMetaOp::update_super_block(bool &succ_ckpt)
{
  int ret = OB_SUCCESS;
  int64_t cur_exe_idx = 0;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (FALSE_IT(cur_exe_idx = micro_ckpt_ctx_.get_cur_ckpt_exe_idx())) {
  } else if (OB_LIKELY(micro_ckpt_ctx_.cur_super_blk_.micro_ckpt_used_blk_list_by_idx(cur_exe_idx).count() > 0)) {
    const int64_t cur_exe_idx = micro_ckpt_ctx_.get_cur_ckpt_exe_idx();
    int64_t start_time_us = ObTimeUtility::current_time_us();
    do {
      ret = OB_SUCCESS;
      if (OB_FAIL(phy_blk_mgr()->get_micro_ckpt_info(micro_ckpt_ctx_.prev_super_blk_.micro_ckpt_info_))) {
        LOG_WARN("fail to get micro_ckpt info", KR(ret), K_(micro_ckpt_ctx));
        ob_usleep(500 * 1000);
      }
    } while (OB_FAIL(ret) && (ObTimeUtility::current_time_us() - start_time_us <= retry_duration_us_));

    if (OB_SUCC(ret)) {
      start_time_us = ObTimeUtility::current_time_us();
      do {
        ret = OB_SUCCESS;
        if (OB_FAIL(phy_blk_mgr()->update_ss_super_block_for_micro_ckpt(cur_exe_idx, micro_ckpt_ctx_.cur_super_blk_))) {
          LOG_WARN("fail to update ss_super_blk for micro_ckpt", KR(ret), K(cur_exe_idx), K_(micro_ckpt_ctx));
          ob_usleep(500 * 1000);
        } else {
          succ_ckpt = true;
        }
      } while (OB_FAIL(ret) && (ObTimeUtility::current_time_us() - start_time_us <= retry_duration_us_));
    }
  }
  return ret;
}

int ObSSPersistMicroMetaOp::try_free_phy_blocks(const bool succ_ckpt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    const int64_t failed_blk_cnt = micro_ckpt_ctx_.failed_blk_id_list_.count();
    if (failed_blk_cnt > 0) {
      // May failed cuz timeout, thus need to add into reusable_set
      if (OB_TMP_FAIL(phy_blk_mgr()->add_batch_reusable_blocks(micro_ckpt_ctx_.failed_blk_id_list_))) {
        LOG_WARN("fail to add batch reusable blocks", KR(tmp_ret), K_(micro_ckpt_ctx));
        tmp_ret = OB_SUCCESS;
      }
      FLOG_INFO("will add micro_ckpt reusable blocks", KR(tmp_ret), K(failed_blk_cnt),
        K(micro_ckpt_ctx_.failed_blk_id_list_));
    }

    ObSSMicroCacheSuperBlk &super_blk = (succ_ckpt ? micro_ckpt_ctx_.prev_super_blk_ : micro_ckpt_ctx_.cur_super_blk_);
    const int64_t cur_exe_idx = micro_ckpt_ctx_.get_cur_ckpt_exe_idx();
    if (OB_UNLIKELY(cur_exe_idx >= super_blk.get_micro_ckpt_used_blk_list_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur exe_idx is invalid", KR(ret), K(cur_exe_idx), K(succ_ckpt), K(super_blk));
    } else if (OB_FAIL(phy_blk_mgr()->add_batch_reusable_blocks(super_blk.micro_ckpt_used_blk_list_by_idx(cur_exe_idx)))) {
      LOG_ERROR("fail to add batch reusable phy_blocks", KR(ret), K(cur_exe_idx), K(succ_ckpt), K_(micro_ckpt_ctx));
    }
  }
  return ret;
}

void ObSSPersistMicroMetaOp::handle_meta_ckpt_failure()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // that means, cur micro_meta checkpoint failed, and the persisted ghost micro_meta has been lost
  FLOG_INFO("this persist_micro_meta_op failed", K_(micro_ckpt_ctx));

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    // If we want to clear init_ranges' sub_range persisted micro_meta info, we need to save the detail firstly, 
    // for updating arc_info later;
    ObSSAllSegMetaInfo all_seg_meta_info;
    const int64_t handled_range_cnt = handled_range_idx_arr_.count();
    for (int64_t i = 0; i < handled_range_cnt; ++i) {
      const int64_t init_rng_idx = handled_range_idx_arr_.at(i);
      if (OB_TMP_FAIL(micro_range_mgr()->clear_init_range_persisted_meta_info(init_rng_idx, all_seg_meta_info))) {
        LOG_WARN("fail to clear init_range persisted micro_meta info", KR(tmp_ret), K(i), K(init_rng_idx), K(handled_range_cnt));
        tmp_ret = OB_SUCCESS;
      }
    }

    // handle B1/B2 in arc_info
    const int64_t erase_b1_cnt = all_seg_meta_info.get_cnt_by_idx(SS_ARC_B1);
    const int64_t erase_b1_size = all_seg_meta_info.get_size_by_idx(SS_ARC_B1);
    const int64_t erase_b2_cnt = all_seg_meta_info.get_cnt_by_idx(SS_ARC_B2);
    const int64_t erase_b2_size = all_seg_meta_info.get_size_by_idx(SS_ARC_B2);
    const ObSSMicroArcOpType op_type = ObSSMicroArcOpType::SS_FORCE_ERASE;
    if (erase_b1_cnt > 0 && erase_b1_size > 0) {
      if (OB_TMP_FAIL(micro_meta_mgr()->adjust_arc_seg_info(true/*is_in_l1*/, true/*is_in_ghost*/, op_type,
          erase_b1_size, erase_b1_cnt))) {
        LOG_ERROR("fail to adjust arc_seg_info", KR(tmp_ret), K(all_seg_meta_info));
        tmp_ret = OB_SUCCESS;
      } else {
        cache_stat()->task_stat().update_dropped_persisted_cnt(erase_b1_cnt);
      }
    }

    if (erase_b2_cnt > 0 && erase_b2_size > 0) {
      if (OB_TMP_FAIL(micro_meta_mgr()->adjust_arc_seg_info(false/*is_in_l1*/, true/*is_in_ghost*/, op_type,
          erase_b2_size, erase_b2_cnt))) {
        LOG_ERROR("fail to adjust arc_seg_info", KR(tmp_ret), K(all_seg_meta_info));
        tmp_ret = OB_SUCCESS;
      } else {
        cache_stat()->task_stat().update_dropped_persisted_cnt(erase_b2_cnt);
      }
    }
  }
}

void ObSSPersistMicroMetaOp::handle_extra_func()
{
  int ret = OB_SUCCESS;

  if (micro_ckpt_ctx_.need_replay_ckpt()) {
    replay_cache_ckpt_async();
    micro_ckpt_ctx_.disable_replay_ckpt();
  }

  if (micro_ckpt_ctx_.need_update_expiration()) {
    update_latest_expiration_time();
    micro_ckpt_ctx_.finish_update_expiration();
  }
}

int ObSSPersistMicroMetaOp::replay_cache_ckpt_async()
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret), KP(micro_cache));
  } else {
    FLOG_INFO("start replay micro_cache ckpt async");
    if (OB_FAIL(micro_cache->read_checkpoint())) {
      LOG_ERROR("fail to read ss_micro_cache checkpoint", KR(ret), KPC(cache_stat()));
    } else {
      ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_REPLAY_CKPT_ERR;
    }

    if (OB_FAIL(ret)) {
      // NOTICE: before clear_micro_cache, execute these two steps
      LOG_INFO("start to clear ss_micro_cache cuz fail to replay ckpt", KPC(cache_stat()));
      micro_cache->enable_cache();
      is_closed_ = true;
      IGNORE_RETURN micro_cache->clear_micro_cache(true/* for_replay */);
      FLOG_INFO("finish to clear ss_micro_cache cuz fail to replay ckpt", KPC(cache_stat()));
    } else {
      micro_cache->get_task_runner().enable_extra_task_for_ckpt();
      micro_cache->enable_cache();
      FLOG_INFO("succ to replay ss_micro_cache ckpt, will enable ss_micro_cache");
    }
  }
  return ret;
}

void ObSSPersistMicroMetaOp::update_latest_expiration_time()
{
  const int64_t expiration_time_us = ObSSMicroCacheUtil::calc_micro_cache_expiration_time(tenant_id_) * 1000000L;
  if (expiration_time_us > SS_PERSIST_META_INTERVAL_US) {
    micro_ckpt_ctx_.handle_expire_interval_us_ = SS_PERSIST_META_INTERVAL_US;
  } else if (expiration_time_us < SS_HANDLE_EXPIRATION_MICRO_MIN_INTERVAL_US) {
    micro_ckpt_ctx_.handle_expire_interval_us_ = SS_HANDLE_EXPIRATION_MICRO_MIN_INTERVAL_US;
  } else {
    micro_ckpt_ctx_.handle_expire_interval_us_ = expiration_time_us;
  }
}

void ObSSPersistMicroMetaOp::inner_reset_persist_range_arr()
{
  persist_range_arr_.reset();
  persist_range_arr_.set_attr(ObMemAttr(tenant_id_, "CkptPerRngs"));
}

int ObSSPersistMicroMetaOp::check_split_cnt_changed()
{
  int ret = OB_SUCCESS;
  uint32_t new_ckpt_split_cnt = 0;
  const uint32_t old_ckpt_split_cnt = micro_ckpt_ctx_.get_cur_ckpt_split_cnt(); // old value from micro_ckpt_ctx_
  
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(phy_blk_mgr())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(new_ckpt_split_cnt))) {
    LOG_WARN("fail to get micro_ckpt_split_cnt", KR(ret));
  } else if (OB_UNLIKELY((old_ckpt_split_cnt < 1) || (new_ckpt_split_cnt < 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_ckpt_split_cnt is invalid", KR(ret), K(old_ckpt_split_cnt), K(new_ckpt_split_cnt));
  } else if (OB_UNLIKELY(new_ckpt_split_cnt < old_ckpt_split_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_ckpt_split_cnt should be greater than old_ckpt_split_cnt", KR(ret), K(old_ckpt_split_cnt), K(new_ckpt_split_cnt));
  } else if (new_ckpt_split_cnt > old_ckpt_split_cnt) {
    LOG_INFO("meta_ckpt_split_cnt changed, need update super_blk", 
             K(old_ckpt_split_cnt), K(new_ckpt_split_cnt));
    // after parameters changed, the following operations will be performed:
    // 1. Reset super_blk and reuse all meta_blks
    // 2. Reset meta_ckpt_ctx and other parameters
    if (OB_FAIL(phy_blk_mgr()->reuse_meta_blks_on_meta_ckpt_reset(new_ckpt_split_cnt))) {
      LOG_WARN("fail to reuse meta_blks on ckpt reset", KR(ret), K(new_ckpt_split_cnt));  
    } else {
      // Reset and reinit micro_ckpt_ctx_ with new split_cnt
      micro_ckpt_ctx_.reset();
      if (OB_FAIL(micro_ckpt_ctx_.handle_init(tenant_id_, new_ckpt_split_cnt))) {
        LOG_WARN("fail to handle init", KR(ret), K_(tenant_id), K(new_ckpt_split_cnt));
      } else {
        tablet_info_map_.clear();
        full_ckpt_item_cnt_ = 0;
        cache_stat()->task_stat().set_erase_persisted_cnt(0);
        
        LOG_INFO("succ to update params for split_cnt changed", K(old_ckpt_split_cnt), K(new_ckpt_split_cnt), K_(micro_ckpt_ctx));
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
