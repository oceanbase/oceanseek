/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_do_blk_checkpoint_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSDoBlkCheckpointOp------------------------------------------*/
ObSSDoBlkCheckpointOp::ObSSDoBlkCheckpointOp()
  : ObSSDoCheckpointBaseOp(), blk_ckpt_ctx_()
{
}

void ObSSDoBlkCheckpointOp::destroy()
{
  blk_ckpt_ctx_.reset();
  ObSSDoCheckpointBaseOp::destroy();
}

int ObSSDoBlkCheckpointOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  ObMemAttr buf_attr(tenant_id, "SSBlkItem");
  uint32_t ckpt_split_cnt = 0;
  if (OB_FAIL(ObSSDoCheckpointBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_ctx should be valid", KR(ret), KP_(task_ctx));
  } else if (FALSE_IT(item_buf_size_ = sizeof(ObSSPhyBlockInfoCkptItem))) {
  } else if (OB_ISNULL(item_buf_ = static_cast<char *>(task_allocator()->alloc(item_buf_size_, buf_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(item_buf_size), K(buf_attr));
  } else if (OB_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(ckpt_split_cnt))) {
    LOG_WARN("fail to get micro_ckpt_split_cnt", KR(ret));
  } else if (OB_FAIL(blk_ckpt_ctx_.handle_init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to init blk_ckpt_ctx", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    op_type_ = ObSSMicroCacheTaskOpType::SS_DO_BLK_CKPT_OP;
    ckpt_ctx_ = &blk_ckpt_ctx_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::start_op()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSDoCheckpointBaseOp::start_op())) {
    LOG_WARN("fail to start ckpt operation", KR(ret), K_(blk_ckpt_ctx));
  } else if ((phy_blk_mgr()->get_reusable_blocks_cnt() >= SS_MIN_REORGAN_BLK_CNT) ||
             (blk_ckpt_ctx_.need_exe_full_ckpt())) {
    // To reduce the frequency of blk_ckpt, it is only executed when the number of reusable blocks exceeds 
    // the minimum number of sparse blocks required to trigger a reorganization task.
    blk_ckpt_ctx_.need_ckpt_ = true;
  }
  return ret;
}

bool ObSSDoBlkCheckpointOp::need_scan_reusable_blocks()
{
  int ret = OB_SUCCESS;
  bool need_scan = blk_ckpt_ctx_.need_scan_reusable_blk();
  if (!need_scan && nullptr != phy_blk_mgr()) {
    const int64_t reusable_blk_cnt = phy_blk_mgr()->get_reusable_blocks_cnt();
    const bool exist_free_data_blk = phy_blk_mgr()->exist_free_block(ObSSPhyBlockType::SS_MICRO_DATA_BLK);
    need_scan = (((reusable_blk_cnt < SS_MIN_REORGAN_BLK_CNT) && (!exist_free_data_blk)));
    if (need_scan) {
      LOG_INFO("need to scan reusable blocks", K(reusable_blk_cnt), K(exist_free_data_blk));
    }
  }
  return need_scan;
}

int ObSSDoBlkCheckpointOp::gen_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // For example, if phy_blk_ckpt blocks count = 2, and we already executed once blk_checkpoint.
  // When we execute the second time, it occured write timeout. Thus, the second phy_blk_ckpt block
  // will be added into reusable_set. So, we will execute the third time blk_checkpoint. And before
  // executing checkpoint, we need to scan the reusable_set to free the phy_blk_ckpt blocks.
  // To avoid the freed phy_blocks are used by other, like common io, we will forbid these types block alloc.
  bool exist_reusable_ckpt_blks = false;
  bool exist_free_ckpt_blks = false;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    exist_free_ckpt_blks = has_free_ckpt_blk();
    if (!exist_free_ckpt_blks) {
      // In case there exist none blk_ckpt block to use
      if (OB_TMP_FAIL(phy_blk_mgr()->scan_reusable_ckpt_blocks_to_free(exist_reusable_ckpt_blks))) {
        LOG_WARN("fail to scan reusable ckpt blocks to free", KR(tmp_ret));
        tmp_ret = OB_SUCCESS;
      } else {
        // after scan reusable ckpt_blks, we need to update 'exist_free_ckpt_blks'
        exist_free_ckpt_blks = has_free_ckpt_blk();
      }
    }

    if (need_scan_reusable_blocks()) {
      if (OB_TMP_FAIL(phy_blk_mgr()->scan_blocks_to_reuse())) {
        LOG_WARN("fail to scan phy_blocks to reuse", KR(tmp_ret));
        tmp_ret = OB_SUCCESS;
      } else {
        blk_ckpt_ctx_.finish_scan_phy_blk();
      }
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (is_op_enabled() && exist_free_ckpt_blks && (blk_ckpt_ctx_.need_ckpt())) {
    LOG_TRACE("ss_micro_cache: start phy_blk ckpt", "blk_ckpt_cnt_", cache_stat()->task_stat_.blk_ckpt_cnt_);
    blk_ckpt_ctx_.start_ckpt();

    bool succ_ckpt = false;
    if (OB_FAIL(gen_phy_block_checkpoint())) {
      LOG_WARN("fail to gen phy_block checkpoint", KR(ret));
    } else if (OB_FAIL(update_super_block(succ_ckpt))) {
      LOG_WARN("fail to update super_block", KR(ret));
    }

    if (OB_TMP_FAIL(try_free_phy_blocks(succ_ckpt))) {
      LOG_WARN("fail to try free phy_blocks", KR(tmp_ret), K(succ_ckpt));
      ret = tmp_ret;
    }

    blk_ckpt_ctx_.finish_ckpt();
    LOG_TRACE("ss_micro_cache: finish phy_blk ckpt", KR(ret), K(blk_ckpt_ctx_));
  }

  if (exist_reusable_ckpt_blks) {
    phy_blk_mgr()->allow_alloc_all_blk();
    FLOG_INFO("allow all blk alloc", KR(ret));
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::gen_phy_block_checkpoint()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (is_op_enabled()) {
    ObSEArray<ObSSPhyBlockReuseInfo, DEFAULT_BLK_CNT> all_blk_info_arr;
    if (OB_FAIL(phy_blk_mgr()->scan_blocks_to_ckpt(all_blk_info_arr))) {
      LOG_WARN("fail to scan all phy_blocks' reuse_info", KR(ret));
    } else if (OB_FAIL(mark_reusable_blocks(all_blk_info_arr))) {
      LOG_WARN("fail to mark reusable blocks", KR(ret));
    } else if (OB_FAIL(build_blk_ckpt_item(all_blk_info_arr))) {
      LOG_WARN("fail to build blk ckpt item", KR(ret));
    } else {
      cache_stat()->task_stat_.update_blk_ckpt_cnt(1, blk_ckpt_ctx_.ckpt_item_cnt_);
    }
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::mark_reusable_blocks(ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr)
{
  int ret = OB_SUCCESS;
  int64_t total_blk_cnt = 0;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(total_blk_cnt = phy_blk_mgr()->get_total_blk_cnt())) {
  } else if (OB_UNLIKELY(total_blk_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total_blk_cnt is invalid", KR(ret), K(total_blk_cnt));
  } else if (OB_FAIL(phy_blk_mgr()->get_blk_ckpt_info(blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_info_))) {
    LOG_WARN("fail to get blk_ckpt primary info in super_blk", KR(ret), K_(blk_ckpt_ctx));
  } else if (blk_ckpt_ctx_.prev_super_blk_.exist_blk_checkpoint()) {
    ObIArray<int64_t> &prev_ckpt_used_blk_list = blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_used_blk_list();
    const int64_t phy_ckpt_cnt = prev_ckpt_used_blk_list.count();
    // 1. If previous ckpt is valid, get these phy_blocks reuse info, try to reuse it later.
    for (int64_t i = 0; OB_SUCC(ret) && (i < phy_ckpt_cnt); ++i) {
      const int64_t blk_idx = prev_ckpt_used_blk_list.at(i);
      if (OB_LIKELY((blk_idx >= SS_SUPER_BLK_COUNT) && (blk_idx < total_blk_cnt) && 
                    (all_blk_info_arr.at(blk_idx - SS_SUPER_BLK_COUNT).blk_idx_ == blk_idx))) {
        all_blk_info_arr.at(blk_idx - SS_SUPER_BLK_COUNT).need_reuse_ = 1;
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    ObSEArray<int64_t, DEFAULT_BLK_CNT> reusable_blk_arr;
    // 2. Select some phy_blocks from reusable_set, try to reuse it later.
    if (OB_FAIL(phy_blk_mgr()->get_batch_reusable_blocks(reusable_blk_arr, MIN_REUSE_PHY_BLOCK_CNT))) {
      LOG_WARN("fail to get reusable blocks", KR(ret));
    } else if (reusable_blk_arr.count() > 0) {
      const int64_t reusable_blk_cnt = reusable_blk_arr.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < reusable_blk_cnt); ++i) {
        const int64_t blk_idx = reusable_blk_arr.at(i);
        if (OB_LIKELY((blk_idx >= SS_SUPER_BLK_COUNT) && (blk_idx < total_blk_cnt) && 
                      (all_blk_info_arr.at(blk_idx - SS_SUPER_BLK_COUNT).blk_idx_ == blk_idx))) {
          all_blk_info_arr.at(blk_idx - SS_SUPER_BLK_COUNT).need_reuse_ = 1;
        }
      }
    }
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::build_blk_ckpt_item(ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  ObSSCkptPhyBlockItemWriter item_writer;
  ObCompressorType compressor_type = ObCompressorType::INVALID_COMPRESSOR;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(item_buf_) || OB_UNLIKELY(item_buf_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item buf/size is invalid", KR(ret), KP_(item_buf), K_(item_buf_size));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret), KP(micro_cache));
  } else if (OB_FAIL(item_writer.init(tenant_id_, *(phy_blk_mgr()), ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK,
             compressor_type))) {
    LOG_WARN("fail to init ckpt phy_block item writer", KR(ret), K_(tenant_id), K(compressor_type));
  } else {
    const int64_t total_blk_cnt = all_blk_info_arr.count();
    // use prev_ckpt_used_blk_list to save all blk_idx of phy_blks which need to be reused
    ObIArray<int64_t> &prev_ckpt_used_blk_list = blk_ckpt_ctx_.prev_super_blk_.blk_ckpt_used_blk_list();
    prev_ckpt_used_blk_list.reuse();
    prev_ckpt_used_blk_list.reserve(total_blk_cnt);

    for (int64_t i = 0; OB_SUCC(ret) && (i < total_blk_cnt) && is_op_enabled(); ++i) {
      const int64_t blk_idx = all_blk_info_arr.at(i).blk_idx_;
      ObSSPhyBlockInfoCkptItem ckpt_item;
      ckpt_item.blk_idx_ = blk_idx;
      // For one phy_block, if it was the prev ckpt phy_block or it was choosen from reusable_set, 
      // it can be considered to reuse, and its reuse_version need to increase 1.
      if (all_blk_info_arr.at(i).need_reuse_) {
        ckpt_item.reuse_version_ = all_blk_info_arr.at(i).reuse_version_ + 1;
        if (OB_FAIL(prev_ckpt_used_blk_list.push_back(blk_idx))) {
          LOG_ERROR("fail to push back", KR(ret), K(blk_idx), K(total_blk_cnt), K_(blk_ckpt_ctx));
        }
      } else {
        ckpt_item.reuse_version_ = all_blk_info_arr.at(i).reuse_version_;
      }
      
      if (OB_SUCC(ret)) {
        int64_t pos = 0;
        if (OB_UNLIKELY(!ckpt_item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block ckpt_item is invalid", KR(ret), K(ckpt_item), K(i), K(total_blk_cnt));
        } else if (OB_FAIL(ckpt_item.serialize(item_buf_, item_buf_size_, pos))) {
          LOG_WARN("fail to serialize phy_blk ckpt_item", KR(ret), K(i), KP_(item_buf), K_(item_buf_size), K(ckpt_item));
        } else if (OB_FAIL(item_writer.write_item(item_buf_, pos))) {
          LOG_WARN("fail to write item", KR(ret), K(i), K(pos), KP_(item_buf), K_(item_buf_size));
        } else {
          ++blk_ckpt_ctx_.ser_item_cnt_;
        }
      }
    }

    if (FAILEDx(item_writer.close())) {
      LOG_WARN("fail to close item writer", KR(ret));
    } else {
      blk_ckpt_ctx_.ckpt_item_cnt_ = blk_ckpt_ctx_.ser_item_cnt();
    }

    if (OB_TMP_FAIL(handle_block_id_list(item_writer))) {
      LOG_WARN("fail to handle block_id list", KR(ret), KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSSDoBlkCheckpointOp::handle_block_id_list(ObSSCkptPhyBlockItemWriter &item_writer)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(get_ckpt_block_id_list(item_writer, blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_used_blk_list(), false))) {
    LOG_ERROR("fail to get block_id list", KR(ret), K_(blk_ckpt_ctx));
  } else if (!blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_used_blk_list().empty()) {
    const int64_t blk_ckpt_entry = blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_used_blk_list().at(0);
    blk_ckpt_ctx_.cur_super_blk_.blk_ckpt_info_.blk_ckpt_entry_ = blk_ckpt_entry;
  }

  if (OB_TMP_FAIL(get_ckpt_block_id_list(item_writer, blk_ckpt_ctx_.failed_blk_id_list_, true))) {
    LOG_ERROR("fail to get failed block_id list", KR(tmp_ret), K_(blk_ckpt_ctx));
    tmp_ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::update_super_block(bool &succ_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (OB_FAIL(phy_blk_mgr()->update_ss_super_block_for_blk_ckpt(blk_ckpt_ctx_.cur_super_blk_))) {
    LOG_WARN("fail to update ss_super_blk for blk_ckpt", KR(ret), K_(blk_ckpt_ctx));
  } else {
    succ_ckpt = true;
  }
  return ret;
}

int ObSSDoBlkCheckpointOp::try_free_phy_blocks(const bool succ_ckpt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    const int64_t failed_blk_cnt = blk_ckpt_ctx_.failed_blk_id_list_.count();
    if (failed_blk_cnt > 0) {
      // May failed cuz timeout, thus need to add into reusable_set
      if (OB_TMP_FAIL(phy_blk_mgr()->add_batch_reusable_blocks(blk_ckpt_ctx_.failed_blk_id_list_))) {
        LOG_WARN("fail to add batch reusable blocks", KR(tmp_ret), K_(blk_ckpt_ctx));
        tmp_ret = OB_SUCCESS;
      }
      FLOG_INFO("will add blk_ckpt reusable blocks", KR(tmp_ret), K(failed_blk_cnt),
        K(blk_ckpt_ctx_.failed_blk_id_list_));
    }

    ObSSMicroCacheSuperBlk &super_blk = (succ_ckpt ? blk_ckpt_ctx_.prev_super_blk_ : blk_ckpt_ctx_.cur_super_blk_);
    ObIArray<int64_t> &reusable_blk_list = super_blk.blk_ckpt_used_blk_list();
    check_before_free_phy_blocks(reusable_blk_list);

    if (OB_FAIL(phy_blk_mgr()->try_free_batch_blocks(reusable_blk_list, blk_ckpt_ctx_.free_blk_cnt_))) {
      LOG_WARN("fail to free reusable phy_blocks", KR(ret), K(succ_ckpt), K_(blk_ckpt_ctx));
    }

    check_after_free_phy_blocks(succ_ckpt);
  }
  return ret;
}

void ObSSDoBlkCheckpointOp::check_before_free_phy_blocks(ObIArray<int64_t> &reusable_blk_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t reusable_blk_cnt = reusable_blk_list.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (OB_LIKELY(reusable_blk_cnt > 0)) {
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    ObArray<int64_t> abnormal_blk_list;
    ObArray<int64_t> normal_blk_list;
    if (OB_FAIL(normal_blk_list.reserve(reusable_blk_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(reusable_blk_cnt));
    }

    for (int64_t i = 0; OB_SUCC(ret) && (i < reusable_blk_cnt); ++i) {
      bool is_exist = false;
      const int64_t blk_idx = reusable_blk_list.at(i);
      // for one phy_block which will be free soon, it should not exist in ss_super_block checkpoint list.
      if (OB_TMP_FAIL(phy_blk_mgr()->exist_in_ss_super_block_ckpt_list(blk_idx, is_exist))) {
        LOG_WARN("fail to check exist in ss_super_blk ckpt list", KR(tmp_ret), K(i), K(reusable_blk_cnt), K(blk_idx));
        tmp_ret = OB_SUCCESS;
      }

      if (OB_UNLIKELY(is_exist)) {
        if (OB_TMP_FAIL(abnormal_blk_list.push_back(blk_idx))) {
          LOG_WARN("fail to push back", KR(tmp_ret), K(blk_idx));
          tmp_ret = OB_SUCCESS;
        }
      } else if (OB_TMP_FAIL(normal_blk_list.push_back(blk_idx))) {
        LOG_WARN("fail to push back", KR(tmp_ret), K(blk_idx));
        tmp_ret = OB_SUCCESS;
      }
    }

    const int64_t abnormal_blk_cnt = abnormal_blk_list.count();
    if (OB_SUCC(ret) && (abnormal_blk_cnt > 0)) {
      LOG_ERROR("NOTICE: exist abnormal block_idx to free", K(abnormal_blk_cnt));
      phy_blk_mgr()->check_blocks_info(abnormal_blk_list);

      reusable_blk_list.reuse();
      if (OB_TMP_FAIL(reusable_blk_list.assign(normal_blk_list))) {
        LOG_ERROR("fail to assign", KR(tmp_ret), K(normal_blk_list));
        tmp_ret = OB_SUCCESS;
      }
    }

    if (TC_REACH_TIME_INTERVAL(SS_LOOP_PRINT_LOG_INTERVAL_US)) {
      const int64_t cost_time_us = ObTimeUtility::current_time_us() - start_time_us;
      if (cost_time_us > 500 * 1000) {
        LOG_INFO("check_before_free_phy_blocks cost too much time", K(cost_time_us), K(reusable_blk_cnt));
      }
    }
  }
}

void ObSSDoBlkCheckpointOp::check_after_free_phy_blocks(const bool succ_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    ObSSMicroCacheSuperBlk super_blk;
    if (OB_FAIL(phy_blk_mgr()->get_ss_super_block(super_blk))) {
      LOG_WARN("fail to get ss_super_blk", KR(ret));
    } else {
      // We need to check that, can't use more than half phy_blk_ckpt blocks. Otherwise,
      // there may exist some unexpected situation, thus we need to print some info.
      int64_t blk_ckpt_cnt = 0;
      int64_t blk_ckpt_used_cnt = 0;
      phy_blk_mgr()->get_blk_ckpt_block_info(blk_ckpt_cnt, blk_ckpt_used_cnt);
      if ((blk_ckpt_cnt > 0) && (blk_ckpt_used_cnt > blk_ckpt_cnt / 2)) {
        const int64_t ckpt_used_blk_cnt = super_blk.blk_ckpt_used_blk_list().count();
        const int64_t failed_blk_cnt = blk_ckpt_ctx_.failed_blk_id_list_.count();
        FLOG_INFO("phy_blk_ckpt blocks may be recycled", KR(ret), K(blk_ckpt_cnt), K(blk_ckpt_used_cnt),
          K(succ_ckpt), K(blk_ckpt_ctx_.need_ckpt_), K(blk_ckpt_ctx_.free_blk_cnt_), K(ckpt_used_blk_cnt),
          K(failed_blk_cnt), K(blk_ckpt_ctx_.failed_blk_id_list_));
        if (failed_blk_cnt > 0) {
          phy_blk_mgr()->check_blocks_info(blk_ckpt_ctx_.failed_blk_id_list_);
        }
        if (ckpt_used_blk_cnt > 0) {
          phy_blk_mgr()->check_blocks_info(super_blk.blk_ckpt_used_blk_list());
        }
      }
    }
  }
}

} // storage
} // oceanbase
