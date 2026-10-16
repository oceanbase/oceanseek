/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_reorganize_micro_op.h"
#include "lib/utility/ob_sort.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------SSReorganizeBlkCtx------------------------------------------*/
void SSReorganizeBlkCtx::reset()
{
  reuse();
  has_scanned_ = false;
  block_size_ = 0;
  last_scan_time_ = 0;
  last_reorgan_time_ = 0;
}

void SSReorganizeBlkCtx::reuse()
{
  last_reorgan_time_ = ObTimeUtility::current_time_us();
  candidate_blk_cnt_ = 0;
  candidate_micro_cnt_ = 0;
  reorgan_micro_cnt_ = 0;
  reorgan_total_size_ = 0;
  reorgan_used_blk_cnt_ = 0;
  reorgan_free_blk_cnt_ = 0;
}

int SSReorganizeBlkCtx::init(const uint64_t tenant_id, const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size));
  } else {
    reuse();
    has_scanned_ = false;
    last_scan_time_ = 0;
    block_size_ = block_size;
  }
  return ret;
}

/*-----------------------------------------ObSSReorganizeMicroOp------------------------------------------*/
ObSSReorganizeMicroOp::ObSSReorganizeMicroOp()
  : ObSSMicroCacheBaseOp(), reorgan_ctx_(), read_buf_arr_(), candidate_phy_blks_(), candidate_micro_blks_(),
    candidate_key_map_()
{}

void ObSSReorganizeMicroOp::destroy()
{
  reorgan_ctx_.reset();
  candidate_phy_blks_.reset();
  candidate_micro_blks_.reset();
  candidate_key_map_.destroy();
  free_all_read_buf();
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSReorganizeMicroOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_NUM = 256;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(reorgan_ctx_.init(tenant_id, task_ctx.phy_blk_mgr_->get_block_size()))) {
    LOG_WARN("fail to init reorgan_ctx", KR(ret), K(tenant_id));
  } else if (OB_FAIL(candidate_key_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSReorgKeyMap")))) {
    LOG_WARN("fail to create map", KR(ret), K(tenant_id));
  } else {
    op_type_ = ObSSMicroCacheTaskOpType::SS_REORGANIZE_MICRO_OP;
    is_inited_ = true;
  }
  return ret;
}

int ObSSReorganizeMicroOp::execute_reorganization()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(cache_stat()->task_stat().update_reorgan_invoke_time(ObTimeUtility::current_time_s()))) {
  } else if (check_need_execute()) {
    reorganize_phy_block();
  }
  return ret;
}

void ObSSReorganizeMicroOp::clear_for_next_round()
{
  candidate_phy_blks_.reset(); // not use 'ObArray.reuse()', cuz phy_block_handle needs to dec ref_cnt
  candidate_micro_blks_.reset();
  candidate_key_map_.clear();
  free_all_read_buf();
  reorgan_ctx_.reuse();
}

bool ObSSReorganizeMicroOp::reach_scan_interval()
{
  return (ObTimeUtility::current_time_us() - reorgan_ctx_.last_scan_time_ > SS_SCAN_SPARSE_BLK_INTERVAL_US);
}

bool ObSSReorganizeMicroOp::can_trigger_reorganize()
{
  // To aggregate hot micro data, a single reorgan_task must be able to reorganize 
  // at least SS_MIN_REORGAN_BLK_CNT sparse blocks.
  return phy_blk_mgr()->get_sparse_block_cnt() >= SS_MIN_REORGAN_BLK_CNT;
}

bool ObSSReorganizeMicroOp::need_scan_sparse_blk()
{
  return ((!can_trigger_reorganize() && reach_scan_interval()) || !reorgan_ctx_.has_scanned_);
}

int ObSSReorganizeMicroOp::reorganize_phy_block()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    // There are two situations that trigger a global scan:
    // 1. Collect all sparse phy_blocks after observer restart.
    // 2. Not enough phy_block in sparse_blk_map and reach scan interval.
    if (need_scan_sparse_blk()) {
      if (OB_TMP_FAIL(phy_blk_mgr()->scan_blocks_to_reorgan())) {
        LOG_WARN("fail to scan sparse_block to reorganize", KR(tmp_ret));
        tmp_ret = OB_SUCCESS;
      } else {
        reorgan_ctx_.last_scan_time_ = ObTimeUtility::current_time_us();
        reorgan_ctx_.has_scanned_ = true;
      }
    }

    if (OB_SUCC(ret) && can_trigger_reorganize()) {
      int64_t reserved_blk_cnt = 0;
      phy_blk_mgr()->reserve_reorganize_block(reserved_blk_cnt);
      if (OB_UNLIKELY(reserved_blk_cnt < SS_MIN_REORGAN_BLK_CNT)) {
        const int64_t free_bg_mem_blk_cnt = mem_blk_mgr()->get_free_mem_blk_cnt(false/*is_fg*/);
        if (free_bg_mem_blk_cnt + reserved_blk_cnt >= SS_MIN_REORGAN_BLK_CNT) {
          reserved_blk_cnt = SS_MIN_REORGAN_BLK_CNT;
          FLOG_INFO("can't reserve enough reorgan_blk, will use mem_blk", K(free_bg_mem_blk_cnt), K(reserved_blk_cnt));
        } else {
          FLOG_WARN("can't reserve enough reorgan_blk/mem_blk", K(free_bg_mem_blk_cnt), K(reserved_blk_cnt));
        }
      }

      if (OB_FAIL(phy_blk_mgr()->get_batch_sparse_blocks(candidate_phy_blks_, reserved_blk_cnt))) {
        LOG_WARN("fail to get batch sparse blocks", KR(ret), K(reserved_blk_cnt));
      } else if (OB_LIKELY(candidate_phy_blks_.count() >= SS_MIN_REORGAN_BLK_CNT)) {
        reorgan_ctx_.candidate_blk_cnt_ = candidate_phy_blks_.count();
        LOG_TRACE("ss_micro_cache: start reorgan task", K(reserved_blk_cnt), "candidate_phy_blk_cnt", candidate_phy_blks_.count());

        if (OB_FAIL(read_sparse_phy_blocks())) {
          LOG_WARN("fail to read sparse phy_blocks", KR(ret));
        } else if (OB_FAIL(handle_sparse_phy_blocks())) {
          LOG_WARN("fail to handle sparse phy_blocks", KR(ret));
        } else if (OB_FAIL(reaggregate_micro_blocks())) {
          LOG_WARN("fail to reaggregate micro_blocks", KR(ret));
        }

        if (reorgan_ctx_.new_free_blk_cnt() < 0) {
          LOG_WARN("ss_micro_cache reorgan task not free phy_block", K_(reorgan_ctx));
        }
        const int64_t cost_us = get_cost_us();
        LOG_TRACE("ss_micro_cache: finish reorgan task", KR(ret), K(cost_us), K_(reorgan_ctx));
      } else {
        LOG_TRACE("not exist enough sparse phy_blk", K(reserved_blk_cnt), K(candidate_phy_blks_.count()));
      }
    }
  }
  
  return ret;
}

int ObSSReorganizeMicroOp::read_sparse_phy_blocks()
{
  int ret = OB_SUCCESS;
  const int64_t candidate_blk_cnt = candidate_phy_blks_.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_FAIL(alloc_all_read_buf())) {
    LOG_WARN("fail to alloc read_buf", KR(ret), K(candidate_blk_cnt));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);

    for (int64_t i = 0; OB_SUCC(ret) && (i < candidate_blk_cnt); ++i) {
      ObSSPhyBlkReorganEntry &blk_reorgan_entry = candidate_phy_blks_[i];
      if (OB_UNLIKELY(!blk_reorgan_entry.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block entry should be valid", KR(ret), K(i), K(blk_reorgan_entry), K_(reorgan_ctx));
      // this phy_blk may be not valid, cuz if its micro_blocks were evicted, its valid_len may be equal to 0
      } else if (OB_LIKELY(blk_reorgan_entry.phy_blk_handle_.get_ptr()->is_valid_micro_data_block())) {
        const int32_t phy_blk_size = task_ctx_->block_size_;
        const int64_t phy_blk_offset = blk_reorgan_entry.phy_blk_idx_ * phy_blk_size;
        ObSSPhyBlockHandle &phy_blk_handle = blk_reorgan_entry.phy_blk_handle_;
        ObIOHandle &io_handle = blk_reorgan_entry.io_handle_;
        char *buf = static_cast<char *>(read_buf_arr_.at(i));
        if (OB_ISNULL(buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read buf should not be null", KR(ret), K(i), K_(reorgan_ctx), K(read_buf_arr_.count()));
        } else if (OB_FAIL(ObSSMicroCacheIOHelper::async_read_block(phy_blk_offset, phy_blk_size, buf, phy_blk_handle,
                   io_handle))) {
          LOG_WARN("fail to read micro_cache block", KR(ret), K(i), K(phy_blk_size), K(phy_blk_offset), KP(buf), 
            K(phy_blk_handle), K_(reorgan_ctx));
        } else {
          blk_reorgan_entry.need_read_blk_ = true;
        }
      }
    }

    // It is safe if io_handle not execute wait here.
    for (int64_t i = 0; OB_SUCC(ret) && (i < candidate_blk_cnt); ++i) {
      ObSSPhyBlkReorganEntry &blk_reorgan_entry = candidate_phy_blks_[i];
      if (blk_reorgan_entry.need_read_blk_ && OB_FAIL(blk_reorgan_entry.io_handle_.wait())) {
        LOG_WARN("fail to wait read phy_block", KR(ret), K(i), K(blk_reorgan_entry), K_(reorgan_ctx));
      }
    }
  }

  return ret;
}

int ObSSReorganizeMicroOp::handle_sparse_phy_blocks()
{
  int ret = OB_SUCCESS;
  const int64_t candidate_blk_cnt = candidate_phy_blks_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < candidate_blk_cnt); ++i) {
    ObSSPhyBlkReorganEntry &blk_reorgan_entry = candidate_phy_blks_[i];
    char *blk_data_buf = static_cast<char *>(read_buf_arr_.at(i));
    if (OB_ISNULL(blk_data_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block data buf should not be null", KR(ret), K(i), K(read_buf_arr_.count()), K(candidate_blk_cnt));
    } else if (OB_FAIL(select_valid_micro_blocks(i, blk_reorgan_entry, blk_data_buf))) {
      LOG_WARN("fail to select valid micro_block of one phy_block", KR(ret), K(i), K(candidate_blk_cnt), 
        K(blk_reorgan_entry));
    }
    ret = OB_SUCCESS; // ignore ret, continue to handle next phy_block
  }
  return ret;
}

int ObSSReorganizeMicroOp::select_valid_micro_blocks(
    const int64_t candidate_blk_idx,
    ObSSPhyBlkReorganEntry &blk_reorgan_entry,
    char *data_buf)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_blk_cnt = candidate_phy_blks_.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(data_buf) || OB_UNLIKELY(!blk_reorgan_entry.is_valid() || candidate_blk_idx < 0 ||
             candidate_blk_idx >= candidate_blk_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_buf), K(candidate_blk_idx), K(blk_reorgan_entry), K(candidate_blk_cnt));
  } else {
    const int32_t phy_blk_size = task_ctx_->block_size_;
    const int64_t phy_blk_idx = blk_reorgan_entry.phy_blk_idx_;
    ObSSPhyBlockCommonHeader common_header;
    ObSSMicroDataBlockHeader blk_header;
    int64_t pos = 0;
    if (OB_FAIL(common_header.deserialize(data_buf, phy_blk_size, pos))) {
      LOG_WARN("fail to deserialize phy_blk common header", KR(ret), KP(data_buf), K(phy_blk_size));
    } else if (OB_UNLIKELY(!common_header.is_valid() || !common_header.is_micro_data_blk() || 
               pos != common_header.header_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is invalid or wrong type or deserialize error", KR(ret), K(common_header), K(pos));
    } else if (OB_FAIL(common_header.check_payload_checksum(data_buf + pos, common_header.payload_size_))) {
      LOG_WARN("fail to check payload checksum", KR(ret), K(common_header), K(phy_blk_size), K(pos), KP(data_buf));
    } else if (OB_FAIL(blk_header.deserialize(data_buf, phy_blk_size, pos))) {
      LOG_WARN("fail to deserialize phy_blk header", KR(ret), KP(data_buf), K(phy_blk_size));
    } else if (OB_UNLIKELY(!blk_header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block_header is invalid", KR(ret), K(blk_header), K(candidate_blk_idx), K(blk_reorgan_entry));
    } else if (OB_LIKELY(blk_header.micro_count_ > 0)) {
      ObSSMicroBlockIndex micro_index;
      const int32_t payload_offset = blk_header.payload_offset_;
      const int32_t micro_index_offset = blk_header.micro_index_offset_;
      int64_t index_pos = micro_index_offset;
      for (int64_t i = 0; OB_SUCC(ret) && (i < blk_header.micro_count_); ++i) {
        micro_index.reset();
        ObSSMicroReorganEntry micro_reorgan_entry;
        micro_reorgan_entry.idx_ = candidate_blk_idx;

        if (OB_FAIL(micro_index.deserialize(data_buf, phy_blk_size, index_pos))) {
          LOG_WARN("fail to deserialize phy_block micro index", KR(ret), K(i), K(phy_blk_size));
        } else if (OB_UNLIKELY(!micro_index.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block micro index should be valid", KR(ret), K(micro_index), K(i));
        } else {
          bool need_reorganize = false;
          const ObSSMicroBlockCacheKey &micro_key = micro_index.get_micro_key();
          
          if (OB_FAIL(micro_meta_mgr()->get_micro_block_meta(micro_key, micro_reorgan_entry.micro_meta_handle_, 
              ObTabletID::INVALID_TABLET_ID, false/*update_arc*/))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS; // skip it, try to reorganize next micro_block
            } else {
              LOG_WARN("fail to try mark reorganizing", KR(ret), K(i), K(micro_key), K(phy_blk_idx));
            }
          } else if (OB_FAIL(check_micro_need_reorganize(micro_reorgan_entry, phy_blk_idx, phy_blk_size, need_reorganize))) {
            LOG_WARN("fail to check micro need reorganize", KR(ret), K(micro_key), K(micro_reorgan_entry), K(phy_blk_idx));
          } else if (need_reorganize) {
            const int32_t data_buf_offset = micro_reorgan_entry.data_loc() - phy_blk_idx * phy_blk_size;
            if (OB_UNLIKELY(data_buf_offset <= 0 || data_buf_offset >= phy_blk_size)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("data_buf_offset should be valid", KR(ret), K(micro_reorgan_entry), K(phy_blk_idx), K(phy_blk_size),
                K(data_buf_offset));
            } else {
              micro_reorgan_entry.data_ptr_ = data_buf + data_buf_offset;
              if (OB_FAIL(candidate_micro_blks_.push_back(micro_reorgan_entry))) {
                LOG_WARN("fail to push_back", KR(ret), K(i), K(micro_key), K(micro_reorgan_entry));
              } else {
                ATOMIC_INC(&reorgan_ctx_.candidate_micro_cnt_);
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY((index_pos - micro_index_offset) != blk_header.micro_index_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_block index size mismatch", KR(ret), K(blk_header), K(index_pos));
        }
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::check_micro_need_reorganize(
    ObSSMicroReorganEntry &micro_reorgan_entry,
    const int64_t exp_phy_blk_idx,
    const int32_t phy_blk_size,
    bool &need_reorganize)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_reorganize = false;
  if (OB_UNLIKELY(exp_phy_blk_idx < SS_SUPER_BLK_COUNT || phy_blk_size <= 0 ||
      !micro_reorgan_entry.micro_meta_handle_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exp_phy_blk_idx), K(phy_blk_size), K(micro_reorgan_entry));
  } else {
    const ObSSMicroBlockCacheKey &micro_key = micro_reorgan_entry.micro_meta_handle_()->get_micro_key();
    // Cuz the candidate micro_block may be force deleted or expired, and then readded into cache again.
    // Thus, we need to save the original 'data_loc' and 'crc', when we update micro_meta after reorganizing
    // micro_block data, we need to compare these two values.
    ObSSMicroBlockMetaInfo micro_meta_info;
    if (OB_FAIL(micro_meta_mgr()->get_micro_meta_info(micro_key, micro_meta_info))) {
      LOG_WARN("fail to get micro_meta info", KR(ret), K(micro_key), K(micro_reorgan_entry));
    } else {
      bool already_exist = false;
      bool map_val = false;
      if (OB_TMP_FAIL(candidate_key_map_.get_refactored(micro_key, map_val))) {
        if (OB_HASH_NOT_EXIST != tmp_ret) {
          LOG_WARN("fail to get refactored", KR(tmp_ret), K(micro_meta_info), K(exp_phy_blk_idx));
        }
        tmp_ret = OB_SUCCESS;
      } else {
        already_exist = true;
      }

      if ((!already_exist) && micro_meta_info.is_valid_field() && micro_meta_info.is_data_persisted()) {
        const int64_t cur_phy_blk_idx = micro_meta_info.get_phy_blk_idx(phy_blk_size);
        if (cur_phy_blk_idx == exp_phy_blk_idx) {
          micro_reorgan_entry.exp_data_loc_ = micro_meta_info.data_loc();
          micro_reorgan_entry.exp_micro_size_ = micro_meta_info.size();
          micro_reorgan_entry.exp_crc_ = micro_meta_info.crc();
          need_reorganize = true;

          if (OB_TMP_FAIL(candidate_key_map_.set_refactored(micro_key, true))) {
            LOG_WARN("fail to set refactored", KR(tmp_ret), K(micro_meta_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::reaggregate_micro_blocks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (!candidate_micro_blks_.empty()) {
    ObSEArray<ObLSHandle, 16> ls_handles;
    if (OB_TMP_FAIL(ObSSMicroCacheUtil::get_ls_handles(ls_handles))) {
      LOG_WARN("fail to get ls_handle", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }

    ObArray<SSCandidateMicroInfo> micro_info_arr;
    const int64_t candidate_micro_cnt = candidate_micro_blks_.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < candidate_micro_cnt); ++i) {
      const ObSSMicroReorganEntry &micro_reorgan_entry = candidate_micro_blks_.at(i);
      // If this micro_block was evicted after it was put into candidate_micro_blks, this micro_reorgan_entry would be invalid
      if (OB_LIKELY(micro_reorgan_entry.is_valid())) {
        ObTabletID tablet_id(micro_reorgan_entry.effective_tablet_id());
        ObLSID ls_id;
        ObSSMicroCacheUtil::get_ls_id_by_tablet_id(tenant_id_, tablet_id, ls_handles, ls_id);
        if (!ls_id.is_valid()) {
          ls_id = SYS_LS;
        }

        const bool is_in_l1 = micro_reorgan_entry.is_in_l1();
        ObSSAggregateMicroInfo aggr_info(tablet_id, ls_id, is_in_l1);
        SSCandidateMicroInfo candidate_micro_info(i, aggr_info);
        if (OB_FAIL(micro_info_arr.push_back(candidate_micro_info))) {
          LOG_WARN("fail to push back", KR(ret), K(i), K(candidate_micro_cnt), K(micro_reorgan_entry));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (micro_info_arr.count() > 0) {
      lib::ob_sort(&micro_info_arr.at(0), &micro_info_arr.at(0) + micro_info_arr.count(), SSCandidateMicroInfoCmp());
      
      if (OB_FAIL(write_micro_blocks(micro_info_arr))) {
        LOG_WARN("fail to write micro_blocks", KR(ret), K(micro_info_arr.count()));
      } else {
        cache_stat()->task_stat().update_reorgan_cnt(1);
      }
    }

    ls_handles.reset();
  }
  return ret;
}

int ObSSReorganizeMicroOp::write_micro_blocks(const ObIArray<SSCandidateMicroInfo> &micro_info_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_UNLIKELY(0 == micro_info_arr.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_info_arr));
  } else {
    const int64_t micro_cnt = micro_info_arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
      const int64_t candidate_micro_idx = micro_info_arr.at(i).idx_;
      if (OB_FAIL(write_micro_block(candidate_micro_idx))) {
        LOG_WARN("fail to write micro_block", KR(ret), K(i), K(candidate_micro_idx), K(micro_cnt));
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::write_micro_block(const int64_t candidate_micro_idx)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_blk_cnt = candidate_phy_blks_.count();
  const int64_t candidate_micro_cnt = candidate_micro_blks_.count();
  int64_t candidate_blk_idx = -1;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_UNLIKELY(candidate_micro_idx < 0 || candidate_micro_idx >= candidate_micro_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(candidate_micro_idx), K(candidate_micro_cnt));
  } else if (OB_UNLIKELY(!candidate_micro_blks_.at(candidate_micro_idx).is_valid())) {
    // skip it, in case this micro is evicted, deleted, etc.
  } else if (FALSE_IT(candidate_blk_idx = candidate_micro_blks_.at(candidate_micro_idx).idx_)) {
  } else if (OB_UNLIKELY(candidate_blk_idx >= candidate_blk_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected micro_reorgan_extry or candidate phy_blks", KR(ret), K(candidate_micro_idx), 
      K(candidate_blk_idx), K(candidate_blk_cnt), "micro_entry", candidate_micro_blks_.at(candidate_micro_idx));
  } else {
    const bool is_fg = false;
    ObSSPhyBlkReorganEntry &blk_reorgan_entry = candidate_phy_blks_.at(candidate_blk_idx);
    const ObSSMicroReorganEntry &micro_reorgan_entry = candidate_micro_blks_.at(candidate_micro_idx);
    const ObSSMicroBlockCacheKey &micro_key = micro_reorgan_entry.micro_key();
    const uint64_t micro_data_loc = micro_reorgan_entry.data_loc();
    const uint32_t micro_crc = micro_reorgan_entry.crc();
    const uint64_t reuse_version = blk_reorgan_entry.reuse_version_;

    ObSSMemBlockHandle mem_blk_handle;
    char *content = micro_reorgan_entry.data_ptr_;
    const int32_t micro_size = micro_reorgan_entry.size();
    uint32_t crc = 0;
    bool alloc_new = false;

    if (OB_FAIL(mem_blk_mgr()->add_micro_block_data(micro_key, content, micro_size, mem_blk_handle, crc, 
        alloc_new, is_fg))) {
      LOG_WARN("fail to add bg micro_block data", KR(ret), K(micro_reorgan_entry));
    } else {
      if (OB_UNLIKELY(crc != micro_reorgan_entry.crc())) {
        ret = OB_CHECKSUM_ERROR;
        LOG_WARN("micro_block meta crc should not change", KR(ret), K(crc), K(micro_reorgan_entry));
      } else {
        const int32_t added_mem_blk = alloc_new ? 1 : 0;
        reorgan_ctx_.update_reorgan_used_blk_cnt(added_mem_blk);

        // 'is_persisted': true -> false; 'data_loc': disk -> memory
        // NOTICE: we need to check 'data_loc'. suppose that, this micro_meta is being reorganized,
        //         but it is deleted by 'clear_tablet_micro_block_meta'. then, it is added into micro_cache
        //         again and persisted into another phy_blk. Btw, we can also check 'crc'.
        bool succ_update = false;
        if (OB_FAIL(micro_meta_mgr()->try_update_reorganizing_micro_block_meta(micro_key, micro_data_loc, micro_crc, 
            reuse_version, mem_blk_handle, succ_update))) {
          LOG_WARN( "fail to update reorganizing micro_block meta", KR(ret), K(candidate_micro_idx), K(mem_blk_handle),
            K(blk_reorgan_entry), K(micro_reorgan_entry));
        } else if (!succ_update) {
        } else {
          ATOMIC_INC(&reorgan_ctx_.reorgan_micro_cnt_);
          reorgan_ctx_.update_reorgan_total_size(micro_size);

          const int64_t phy_blk_idx = blk_reorgan_entry.phy_blk_idx_;
          ObSSPhyBlockHandle &phy_blk_handle = blk_reorgan_entry.phy_blk_handle_;
          const int64_t delta_micro_len = micro_size * -1;
          if (OB_FAIL(phy_blk_handle()->dec_valid_len(phy_blk_idx, delta_micro_len))) {
            LOG_WARN("fail to dec valid_len", KR(ret), K(phy_blk_idx), K(delta_micro_len), K(micro_key));
          } else if (phy_blk_handle()->is_empty()) {
            phy_blk_handle.reset();  // dec phy_blk ref_cnt in advance
            cache_stat()->task_stat().update_reorgan_free_blk_cnt(1);
            reorgan_ctx_.update_reorgan_free_blk_cnt(1);
          }
          LOG_TRACE("ss_micro_cache: reorganize micro", KR(ret), K(micro_key), K(micro_size), K(phy_blk_idx), K(mem_blk_handle));
        }
      }

      // If succeed to add_micro_block_data into mem_block, must call mark_micro_complete() to decrease incomplete_cnt_
      if (OB_LIKELY(mem_blk_handle()->is_valid())) {
        mem_blk_handle()->mark_micro_complete();
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::alloc_all_read_buf()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (read_buf_arr_.empty()) {
    ObMemAttr attr(tenant_id_, "SSReorgBuf");
    const int64_t buf_cnt = candidate_phy_blks_.count();
    const int32_t block_size = task_ctx_->block_size_;
    for (int64_t i = 0; OB_SUCC(ret) && (i < buf_cnt); ++i) {
      void *read_buf = nullptr;
      if (OB_ISNULL(read_buf = ob_malloc_align(SS_MEM_BUF_ALIGNMENT, block_size, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(block_size), K(attr));
      } else if (OB_FAIL(read_buf_arr_.push_back(read_buf))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(buf_cnt), KP(read_buf));
      }
    }
  }
  return ret;
}

void ObSSReorganizeMicroOp::free_all_read_buf()
{
  const int64_t buf_cnt = read_buf_arr_.count();
  for (int64_t i = 0; i < buf_cnt; ++i) {
    if (nullptr != read_buf_arr_.at(i)) {
      ob_free_align(read_buf_arr_.at(i));
      read_buf_arr_.at(i) = nullptr;
    }
  }
  read_buf_arr_.reset();
}

} // storage
} // oceanbase
