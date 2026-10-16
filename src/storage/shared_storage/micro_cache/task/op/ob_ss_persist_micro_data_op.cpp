/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_data_op.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"


namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------SSPersistOPEntry-----------------------------------------*/
SSPersistOPEntry::SSPersistOPEntry()
    : phy_blk_idx_(-1), updated_micro_cnt_(0), phy_blk_handle_(), mem_blk_handle_(), io_handle_()
{}

int SSPersistOPEntry::init(
    const int64_t phy_blk_idx, 
    ObSSPhysicalBlock *phy_blk,
    ObSSMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(phy_blk_handle_.is_valid() || mem_blk_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(phy_blk_idx < 0) || OB_ISNULL(phy_blk) || OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx), KP(phy_blk), KP(mem_blk));
  } else {
    phy_blk_idx_ = phy_blk_idx;
    phy_blk_handle_.set_ptr(phy_blk);
    mem_blk_handle_.set_ptr(mem_blk);
  }
  return ret;
}

void SSPersistOPEntry::reset()
{
  phy_blk_idx_ = -1;
  updated_micro_cnt_ = 0;
  phy_blk_handle_.reset();
  mem_blk_handle_.reset();
  io_handle_.reset();
}

/*-----------------------------------------ObSSPersistMicroDataOp-----------------------------------------*/
ObSSPersistMicroDataOp::ObSSPersistMicroDataOp()
    : ObSSMicroCacheBaseOp(),
      max_mem_blk_cnt_(0),
      alloc_blk_fail_cnt_(0),
      entry_arr_(nullptr),
      free_list_(),
      in_use_list_()
{}

void ObSSPersistMicroDataOp::destroy()
{
  if (OB_NOT_NULL(entry_arr_)) {
    for (int64_t i = 0; i < max_mem_blk_cnt_; ++i) {
      entry_arr_[i].reset();
    }
    task_ctx_->allocator_->free(entry_arr_);
    entry_arr_ = nullptr;
  }
  free_list_.destroy();
  in_use_list_.destroy();
  max_mem_blk_cnt_ = 0;
  alloc_blk_fail_cnt_ = 0;
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSPersistMicroDataOp::init(
    const uint64_t tenant_id, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FALSE_IT(max_mem_blk_cnt_ = mem_blk_mgr()->max_mem_block_cnt())) {
  } else if (OB_FAIL(free_list_.init(max_mem_blk_cnt_))) {
    LOG_WARN("fail to init free_list", KR(ret), K_(max_mem_blk_cnt));
  } else if (OB_FAIL(in_use_list_.init(max_mem_blk_cnt_))) {
    LOG_WARN("fail to init in_use_list", KR(ret), K_(max_mem_blk_cnt));
  } else if (OB_FAIL(pre_alloc_persist_entry())) {
    LOG_WARN("fail to pre alloc persist entrt", KR(ret));
  } else {
    op_type_ = ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_DATA_OP;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObSSPersistMicroDataOp::execute()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (check_need_execute()) {
    persist_sealed_mem_blocks();
  }
  return ret;
}

int ObSSPersistMicroDataOp::pre_alloc_persist_entry()
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = sizeof(SSPersistOPEntry) * max_mem_blk_cnt_;
  if (OB_ISNULL(entry_arr_ = static_cast<SSPersistOPEntry *>(task_ctx_->allocator_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(max_mem_blk_cnt), K(alloc_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_mem_blk_cnt_; ++i) {
      SSPersistOPEntry *persist_entry = nullptr;
      persist_entry = new (entry_arr_ + i) SSPersistOPEntry();
      if (OB_FAIL(free_list_.push(persist_entry))) {
        LOG_WARN("fail to push persist_entry", K(ret), K(i), KP(persist_entry));
      }
    }
  }

  if (OB_FAIL(ret) && (nullptr != entry_arr_)) {
    task_ctx_->allocator_->free(entry_arr_);
    entry_arr_ = nullptr;
  }
  return ret;
}

int ObSSPersistMicroDataOp::persist_sealed_mem_blocks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObSSMemBlockHandle> sealed_mem_blk_handles;
  while (OB_SUCC(ret)) {
    alloc_blk_fail_cnt_ = 0;
    const int64_t max_cnt = free_list_.get_curr_total();
    if (OB_FAIL(mem_blk_mgr()->get_sealed_mem_blocks(sealed_mem_blk_handles, max_cnt))) {
      LOG_WARN("fail to get sealed_mem_blocks", KR(ret), K(max_cnt));
    } else if (sealed_mem_blk_handles.count() == 0 && in_use_list_.get_curr_total() == 0) {
      break;  // All sealed_mem_blocks have been handled, directly exit the loop.
    } else {
      const int64_t sealed_mem_blk_cnt = sealed_mem_blk_handles.count();
      if (OB_FAIL(async_write_batch_mem_blocks(sealed_mem_blk_handles))) {
        LOG_WARN("fail to write sealed_mem_blocks", KR(ret), K_(alloc_blk_fail_cnt));
      } else if (OB_FALSE_IT(sealed_mem_blk_handles.reset())) {
      } else if (OB_FAIL(update_micro_meta_for_batch_mem_blocks())) {
        LOG_WARN("fail to update micro_meta for mem_blocks", KR(ret));
      }

      // Fail to alloc any phy_block, exit the loop.
      if ((sealed_mem_blk_cnt > 0) && (alloc_blk_fail_cnt_ >= sealed_mem_blk_cnt)) {
        break;
      }
    }

    if (OB_SUCC(ret) && in_use_list_.get_curr_total() > 0) {
      ob_usleep(10);
    }
  }

  const int64_t cost_us = get_cost_us();
  LOG_TRACE("finish to persist micro_data task", KR(ret), K(cost_us));
  return ret;
}

int ObSSPersistMicroDataOp::async_write_mem_block(
    SSPersistOPEntry &persist_entry, 
    const ObSSMemBlockHandle &sealed_mem_blk_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(persist_entry.is_valid() || !sealed_mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(persist_entry), K(sealed_mem_blk_handle));
  } else {
    // SSMicroDataBlock layout:                                                   
    // |--common header--|--blk header--|--micro data--|--micro index--|--empty--|
    //                                  ^              ^               ^   
    //                                first_data    last_data     first_index
    //                                              last_index
    const ObSSPhyBlockType block_type = sealed_mem_blk_handle()->is_fg_mem_blk() ? ObSSPhyBlockType::SS_MICRO_DATA_BLK
                                                                                  : ObSSPhyBlockType::SS_REORGAN_BLK;      
    int64_t phy_blk_idx = -1;
    ObSSPhysicalBlock *phy_blk = nullptr;
    ObSSPhyBlockHandle phy_blk_handle;
    if (OB_FAIL(phy_blk_mgr()->alloc_block(phy_blk_idx, phy_blk_handle, block_type))) {
      if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
        LOG_WARN("fail to alloc free phy_block", KR(ret), K(block_type));
      }
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block_handle should be valid", KR(ret), K(phy_blk_idx), K(block_type), K(phy_blk_handle));
    } else if (OB_FAIL(persist_entry.init(phy_blk_idx, phy_blk_handle.get_ptr(), sealed_mem_blk_handle.get_ptr()))) {
      LOG_WARN("fail to init persist entry", KR(ret), K(phy_blk_idx), K(phy_blk_handle), K(sealed_mem_blk_handle));
    } else {
      CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
      
      const uint32_t block_size = sealed_mem_blk_handle()->mem_blk_size_;
      const uint32_t payload_offset = sealed_mem_blk_handle()->reserved_size_;
      const uint32_t payload_size = sealed_mem_blk_handle()->get_payload_size();
      char *cur_buf = sealed_mem_blk_handle()->mem_blk_buf_; // promise that it is aligned

      ObSSMicroDataBlockHeader blk_header;
      blk_header.payload_offset_ = payload_offset;
      blk_header.payload_size_ = payload_size;
      blk_header.micro_count_ = sealed_mem_blk_handle()->micro_cnt_;
      blk_header.micro_index_size_ = sealed_mem_blk_handle()->total_index_size_;
      blk_header.micro_index_offset_ = payload_offset + sealed_mem_blk_handle()->total_data_size_;

      ObSSPhyBlockCommonHeader common_header;
      const uint32_t data_blk_header_size = blk_header.get_serialize_size();
      const uint32_t common_header_size = common_header.header_size_;
      common_header.set_payload_size(payload_size + data_blk_header_size);
      common_header.set_block_type(ObSSPhyBlockType::SS_MICRO_DATA_BLK);

      int64_t phy_blk_offset = -1;
      int64_t pos = common_header_size;
      if (OB_ISNULL(cur_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_buf should not be null", KR(ret), KP(cur_buf), KPC(sealed_mem_blk_handle.get_ptr()));
      } else if (OB_FAIL(sealed_mem_blk_handle()->memmove_micro_index())) {
        LOG_WARN("fail to move micro_index", KR(ret));
      } else if (OB_UNLIKELY(!blk_header.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("blk_header is invalid", KR(ret), K(blk_header));
      } else if (OB_FAIL(blk_header.serialize(cur_buf, block_size, pos))) {
        LOG_WARN("fail to serialize blk_header", KR(ret), K(block_size), K(blk_header), KP(cur_buf));
      } else if (FALSE_IT(common_header.calc_payload_checksum(cur_buf + common_header_size, 
                                                              common_header.payload_size_))) {
      } else if (OB_UNLIKELY(!common_header.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("common_header is invalid", KR(ret), K(common_header));
      } else if (FALSE_IT(pos = 0)) {
      } else if (OB_FAIL(common_header.serialize(cur_buf, block_size, pos))) {
        LOG_WARN("fail to serialize common_header", KR(ret), K(block_size), K(common_header), KP(cur_buf));
      } else if (FALSE_IT(phy_blk_offset = phy_blk_idx * block_size)) {
      } else if (OB_FAIL(ObSSMicroCacheIOHelper::async_write_block(phy_blk_offset, block_size, cur_buf, 
                                                                   phy_blk_handle, persist_entry.io_handle_))) {
        LOG_WARN("fail to write phy_block", KR(ret), K(phy_blk_offset), K(block_size), KP(cur_buf));
      }
    }

    if (OB_FAIL(ret) && (-1 != phy_blk_idx)) {
      // can't set this phy_block sealed right now, cuz if write timeout, there may exist a flying write_request,
      // if we reuse this phy_block and write new data into it, the flying write may overwrite it.
      if (OB_TMP_FAIL(phy_blk_mgr()->add_reusable_block(phy_blk_idx))) {
        LOG_WARN("fail to add reusable block", KR(ret), KR(tmp_ret), K(phy_blk_idx));
      }
    }
  }
  return ret;
}

int ObSSPersistMicroDataOp::async_write_batch_mem_blocks(ObArray<ObSSMemBlockHandle> &sealed_mem_blk_handles)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sealed_mem_blk_handles.count(); ++i) {
    SSPersistOPEntry *persist_entry = nullptr;
    ObSSMemBlockHandle &sealed_mem_blk_handle = sealed_mem_blk_handles[i];
    if (OB_FAIL(free_list_.pop(persist_entry))) {
      LOG_WARN("fail to pop persist_entry from free_list", KR(ret));
    } else if (OB_ISNULL(persist_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persist_entry is null", KR(ret), KP(persist_entry));
    } else if (OB_FAIL(async_write_mem_block(*persist_entry, sealed_mem_blk_handle))) {
      if (OB_EAGAIN == ret) {
        ++alloc_blk_fail_cnt_;
      } else {
        LOG_WARN("fail to async write sealed_mem_block", KR(ret), KPC(persist_entry));
      }
    } else if (OB_FAIL(in_use_list_.push(persist_entry))) {
      LOG_ERROR("fail to push into in_use_list", KR(ret), K(i), K(in_use_list_.get_curr_total()));
    }

    // If fail, push mem_block into sealed_mem_blk_list and recycle persist_entry.
    if (OB_FAIL(ret)) {
      int64_t updated_micro_cnt = 0;
      bool valid_phy_blk = false;
      if (OB_NOT_NULL(persist_entry)) {
        updated_micro_cnt = persist_entry->updated_micro_cnt_;
        valid_phy_blk = persist_entry->is_valid();
        persist_entry->reset();
        if (OB_TMP_FAIL(free_list_.push(persist_entry))) {
          LOG_ERROR("fail to push into free_list", KR(tmp_ret), K(free_list_.get_curr_total()));
        }
      }

      if (OB_TMP_FAIL(handle_sealed_mem_block(updated_micro_cnt, valid_phy_blk, sealed_mem_blk_handle))) {
        LOG_ERROR("fail to handle sealed_mem_block", KR(tmp_ret), K(updated_micro_cnt), 
                                                     K(valid_phy_blk), K(sealed_mem_blk_handle));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next sealed_mem_blk
  }
  return ret;
}

int ObSSPersistMicroDataOp::update_micro_meta_for_mem_block(SSPersistOPEntry &persist_entry)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!persist_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("persist_entry should be valid", KR(ret), K(persist_entry));
  } else {
    ObSSMemBlockHandle &sealed_mem_blk_handle = persist_entry.mem_blk_handle_;
    const ObSSPhyBlockHandle &phy_blk_handle = persist_entry.phy_blk_handle_;
    // must update valid_len before update meta
    const int64_t blk_valid_len = sealed_mem_blk_handle()->valid_val_;
    phy_blk_handle()->set_sealed(blk_valid_len);

    int64_t updated_micro_size = 0;
    const uint32_t block_size = phy_blk_mgr()->get_block_size();
    const int64_t phy_blk_idx = persist_entry.phy_blk_idx_;
    const int64_t phy_blk_offset = phy_blk_idx * block_size;
    if (OB_FAIL(micro_meta_mgr()->update_persisted_micro_block_meta(sealed_mem_blk_handle, phy_blk_offset,
        phy_blk_handle()->reuse_version_, updated_micro_size, persist_entry.updated_micro_cnt_))) {
      LOG_ERROR("fail to update meta for persisted micro_block", KR(ret), K(phy_blk_idx), KPC(phy_blk_handle.get_ptr()),
        KPC(sealed_mem_blk_handle.get_ptr()), K(updated_micro_size), K(blk_valid_len));
    } else {
      LOG_TRACE("ss_micro_cache: succ to persist mem_blk", K(phy_blk_idx), K(sealed_mem_blk_handle), 
          KPC(sealed_mem_blk_handle.get_ptr()), K(updated_micro_size), K(blk_valid_len),
          "updated_micro_cnt", persist_entry.updated_micro_cnt_);
    }

    const int32_t delta_len = blk_valid_len - updated_micro_size;
    if (OB_UNLIKELY(delta_len > 0)) {
      if (OB_TMP_FAIL(phy_blk_handle()->dec_valid_len(phy_blk_idx, delta_len * -1))) {
        LOG_WARN("fail to update valid_len by delta", KR(ret), KR(tmp_ret), K(delta_len), 
                                                      K(phy_blk_idx), K(sealed_mem_blk_handle));
      }
    } else if (OB_UNLIKELY(delta_len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("updated_micro_size is abnormal", KR(ret), K(blk_valid_len), K(updated_micro_size), K(phy_blk_idx),
        KPC(sealed_mem_blk_handle.get_ptr()), KPC(phy_blk_handle.get_ptr()));
    }

    if (OB_FAIL(ret)) {
    } else if (phy_blk_handle()->can_reorganize()) {
      if (OB_TMP_FAIL(phy_blk_mgr()->add_sparse_block(ObSSPhyBlockIdx(phy_blk_idx)))) {
        LOG_WARN("fail to add sparse_block after persist micro_data", KR(tmp_ret), K(phy_blk_idx));
        tmp_ret = OB_SUCCESS;
      } else if (TC_REACH_TIME_INTERVAL(SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US)) {
        LOG_INFO("mem_blk is sparse, add sparse_block after persist micro_data", KR(ret), K(phy_blk_idx));
      }
    }
  }
  return ret;
}

int ObSSPersistMicroDataOp::update_micro_meta_for_batch_mem_blocks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t in_use_list_cnt = in_use_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && i < in_use_list_cnt; ++i) {
    SSPersistOPEntry *persist_entry = nullptr;
    bool io_finished = false;
    if (OB_FAIL(in_use_list_.pop(persist_entry))) {
      LOG_WARN("fail to pop persist_entry from in_use_list", KR(ret));
    } else if (OB_ISNULL(persist_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persist_entry should not be nullptr", KR(ret), KP(persist_entry));
    } else if (OB_UNLIKELY(!persist_entry->io_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io_handle should be valid", KR(ret), KPC(persist_entry));
    } else if (OB_FAIL(persist_entry->io_handle_.check_is_finished(io_finished))) {
      LOG_WARN("fail to check io_handle is_finished", KR(ret), KPC(persist_entry));
    } else {
      if (io_finished) {
        if (OB_FAIL(persist_entry->io_handle_.wait())) {
          LOG_WARN("fail to wait", KR(ret), KPC(persist_entry));
        } else if (OB_FAIL(update_micro_meta_for_mem_block(*persist_entry))) {
          LOG_WARN("fail to update micro_meta of phy_block", KR(ret), KPC(persist_entry));
        }
      } else { // io is not finished yet, push it back into in_use_list.
        if (OB_FAIL(in_use_list_.push(persist_entry))) {
          LOG_ERROR("fail to push into in_use_list", KR(ret), KPC(persist_entry), K(in_use_list_.get_curr_total()));
        } else {
          persist_entry = nullptr;
        }
      }
    }

    // If io is not finished and persist_entry is pushed back into in_use_list, persist_entry and mem_block 
    // don't need to be recycled. In other cases, no matter succ or fail, persist_entry and mem_block must be recycled.
    if (OB_NOT_NULL(persist_entry)) {
      const int64_t updated_micro_cnt = persist_entry->updated_micro_cnt_;
      const bool valid_phy_blk = persist_entry->is_valid();
      ObSSMemBlockHandle &mem_blk_handle = persist_entry->mem_blk_handle_;
      if (OB_TMP_FAIL(handle_sealed_mem_block(updated_micro_cnt, valid_phy_blk, mem_blk_handle))) {
        LOG_ERROR("fail to handle sealed mem_block", KR(tmp_ret), KPC(persist_entry));
      }

      persist_entry->reset();
      if (OB_TMP_FAIL(free_list_.push(persist_entry))) {
        LOG_ERROR("fail to push into free_list", KR(tmp_ret), KPC(persist_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next sealed_mem_blk
  }
  return ret;
}

int ObSSPersistMicroDataOp::handle_sealed_mem_block(
    const int64_t updated_micro_cnt,
    const bool valid_phy_blk, 
    ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else {
    // If fail to alloc phy_block, re-add it into sealed_mem_blk_list
    if (!valid_phy_blk) {
      if (OB_FAIL(mem_blk_mgr()->add_into_sealed_block_list(mem_blk_handle))) {
        LOG_WARN("fail to add into sealed_mem_block_list", KR(ret), K(mem_blk_handle));
      }
    } else {
      // If exist micro_block fail to update meta, it's data_dest_ still points to the memory address of mem_block and
      // access this micro_block will cause memory core. So it is necessary to delete these micro_block's meta from
      // meta_map.
      if (OB_UNLIKELY(updated_micro_cnt < mem_blk_handle()->valid_cnt_) &&
          OB_FAIL(micro_meta_mgr()->try_delete_micro_block_meta(mem_blk_handle))) {
        LOG_ERROR("fail to try delete micro_block meta", KR(ret), K(updated_micro_cnt));
      } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("mem_blk_handle is invalid", KR(ret), K(mem_blk_handle));
      } else {
        ObSSMemBlock *mem_blk = mem_blk_handle.get_ptr();
        mem_blk_handle.reset();
        bool succ_free = false;
        if (OB_FAIL(mem_blk->try_free(succ_free))) {
          LOG_WARN("fail to try free this mem_block", KR(ret), KP(mem_blk));
        } else if (!succ_free) {
          LOG_INFO("this mem_blk may be refered, can't free right now", KP(mem_blk));
        }
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
