/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_do_checkpoint_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------SSCkptBaseCtx------------------------------------------*/
int SSCkptBaseCtx::handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prev_super_blk_.init_non_basic_info(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to init non_basic info", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else if (OB_FAIL(cur_super_blk_.init_non_basic_info(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to init non_basic info", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    failed_blk_id_list_.set_attr(ObMemAttr(tenant_id, "SSCkptFBlks"));
    prev_ckpt_us_ = ObTimeUtility::current_time_us();
  }
  return ret;
}

void SSCkptBaseCtx::reuse()
{
  need_ckpt_ = false;
  ckpt_start_us_ = 0;
  ckpt_cost_us_ = 0;
  free_blk_cnt_ = 0;
  failed_blk_id_list_.reuse();
  prev_super_blk_.reuse();
  cur_super_blk_.reuse();
}

void SSCkptBaseCtx::reset() 
{
  ckpt_type_ = ObSSMicroCacheCkptType::SS_CACHE_MICRO_CKPT;
  need_ckpt_ = false;
  ckpt_item_cnt_ = 0;
  prev_ckpt_us_ = 0;
  ckpt_start_us_ = 0;
  ckpt_cost_us_ = 0;
  free_blk_cnt_ = 0;
  failed_blk_id_list_.reset();
  prev_super_blk_.reset();
  cur_super_blk_.reset();
}

void SSCkptBaseCtx::start_ckpt()
{
  ckpt_start_us_ = ObTimeUtility::current_time_us();
  ckpt_cost_us_ = 0;
  ckpt_item_cnt_ = 0;
  ser_item_cnt_ = 0;
  free_blk_cnt_ = 0;
}

void SSCkptBaseCtx::finish_ckpt()
{
  ckpt_cost_us_ = ObTimeUtility::current_time_us() - ckpt_start_us_;
}

/*-----------------------------------------SSBlkCkptCtx------------------------------------------*/
SSBlkCkptCtx::SSBlkCkptCtx() 
  : SSCkptBaseCtx(), prev_scan_blk_time_us_(0)
{}

void SSBlkCkptCtx::reset()
{
  SSCkptBaseCtx::reset();
  prev_scan_blk_time_us_ = 0;
}

void SSBlkCkptCtx::reuse()
{
  SSCkptBaseCtx::reuse();
}

void SSBlkCkptCtx::start_ckpt()
{
  SSCkptBaseCtx::start_ckpt();
}

void SSBlkCkptCtx::finish_ckpt()
{
  SSCkptBaseCtx::finish_ckpt();
  prev_ckpt_us_ = ObTimeUtility::current_time_us();
}

int SSBlkCkptCtx::handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSCkptBaseCtx::handle_init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to handle init", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    ckpt_type_ = ObSSMicroCacheCkptType::SS_CACHE_BLK_CKPT;
    prev_scan_blk_time_us_ = ObTimeUtility::current_time_us();
  }
  return ret;
}

bool SSBlkCkptCtx::need_exe_full_ckpt() const
{
  return (ObTimeUtility::current_time_us() >= (prev_ckpt_us_ + SS_DO_BLK_CKPT_INTERVAL_US));
}

bool SSBlkCkptCtx::need_scan_reusable_blk() const
{
  return (ObTimeUtility::current_time_us() >= (prev_scan_blk_time_us_ + SS_SCAN_REUSABLE_BLK_INTERVAL_US));
}

void SSBlkCkptCtx::finish_scan_phy_blk()
{
  prev_scan_blk_time_us_ = ObTimeUtility::current_time_us();
}

/*-----------------------------------------SSMicroCkptCtx------------------------------------------*/
SSMicroCkptCtx::SSMicroCkptCtx() 
  : SSCkptBaseCtx(), lack_phy_blk_(false), need_replay_ckpt_(false),
    complete_full_ckpt_(true), prev_update_expiration_us_(0), 
    handle_expire_interval_us_(0), cur_ckpt_exe_idx_(0), cur_ckpt_split_cnt_(0)
{}

void SSMicroCkptCtx::reuse()
{
  SSCkptBaseCtx::reuse();
  lack_phy_blk_ = false;
}

void SSMicroCkptCtx::reset()
{
  SSCkptBaseCtx::reset();
  lack_phy_blk_ = false;
  need_replay_ckpt_ = false;
  complete_full_ckpt_ = true;
  prev_update_expiration_us_ = 0;
  handle_expire_interval_us_ = 0;
  cur_ckpt_exe_idx_ = 0;
  cur_ckpt_split_cnt_ = 0;
}

void SSMicroCkptCtx::start_ckpt()
{ 
  SSCkptBaseCtx::start_ckpt();
  if (complete_full_ckpt_) {
    complete_full_ckpt_ = false;
    cur_ckpt_exe_idx_ = 0;
  } else {
    cur_ckpt_exe_idx_ += 1;
  }
}

void SSMicroCkptCtx::finish_ckpt()
{
  SSCkptBaseCtx::finish_ckpt();
  if (cur_ckpt_exe_idx_ >= cur_ckpt_split_cnt_ - 1) {
    complete_full_ckpt_ = true;
  }
  if (complete_full_ckpt_) {
    prev_ckpt_us_ = ObTimeUtility::current_time_us();
  }
}

int SSMicroCkptCtx::handle_init(const uint64_t tenant_id, const uint32_t ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SSCkptBaseCtx::handle_init(tenant_id, ckpt_split_cnt))) {
    LOG_WARN("fail to handle init", KR(ret), K(tenant_id), K(ckpt_split_cnt));
  } else {
    ckpt_type_ = ObSSMicroCacheCkptType::SS_CACHE_MICRO_CKPT;
    prev_update_expiration_us_ = ObTimeUtility::current_time_us();
    handle_expire_interval_us_ = SS_PERSIST_META_INTERVAL_US;
    cur_ckpt_split_cnt_ = ckpt_split_cnt;
  }
  return ret;
}

bool SSMicroCkptCtx::need_exe_full_ckpt() const
{
  return (ObTimeUtility::current_time_us() >= (prev_ckpt_us_ + SS_PERSIST_META_INTERVAL_US));
}

bool SSMicroCkptCtx::need_exe_part_ckpt() const
{
  return (ObTimeUtility::current_time_us() >= (prev_ckpt_us_ + SS_PERSIST_PARTIAL_META_INTERVAL_US));
}

bool SSMicroCkptCtx::need_handle_expiration() const
{
  return (ObTimeUtility::current_time_us() >= (prev_ckpt_us_ + handle_expire_interval_us_));
}

bool SSMicroCkptCtx::need_update_expiration() const
{
  return (ObTimeUtility::current_time_us() >= (prev_update_expiration_us_ + SS_UPDATE_EXPIRATION_TIME_INTERVAL_US));
}

void SSMicroCkptCtx::enable_replay_ckpt()
{
  need_replay_ckpt_ = true;
}

void SSMicroCkptCtx::disable_replay_ckpt()
{
  need_replay_ckpt_ = false;
}

void SSMicroCkptCtx::finish_update_expiration()
{
  prev_update_expiration_us_ = ObTimeUtility::current_time_us();
}

/*-----------------------------------------ObSSPersistedSubRangeMicroInfo------------------------------------------*/
void ObSSPersistedSubRangeMicroInfo::reset()
{
  offset_ = 0;
  length_ = 0;
  sub_range_ = nullptr;
  all_pmeta_info_.reset();
  serialized_micro_keys_.reset();
  ghost_hval_arr_.reset();
}

int ObSSPersistedSubRangeMicroInfo::assign(const ObSSPersistedSubRangeMicroInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (other.serialized_micro_keys_.count() > 0) {
      if (OB_FAIL(serialized_micro_keys_.assign(other.serialized_micro_keys_))) {
        LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (other.ghost_hval_arr_.count() > 0) {
      if (OB_FAIL(ghost_hval_arr_.assign(other.ghost_hval_arr_))) {
        LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(all_pmeta_info_.assign(other.all_pmeta_info_))) {
      LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
    } else {
      sub_range_ = other.sub_range_;
      offset_ = other.offset_;
      length_ = other.length_;
    }
  }
  return ret;
}

int ObSSPersistedSubRangeMicroInfo::add_serialized_micro_key(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialized_micro_keys_.push_back(micro_key))) {
    LOG_WARN("fail to push back", KR(ret), K(micro_key), KPC(this));
  }
  return ret;
}

bool ObSSPersistedSubRangeMicroInfo::is_valid() const 
{ 
  return ((nullptr != sub_range_) && (offset_ >= 0) && (length_ > 0)); 
}

/*-----------------------------------------ObSSDoCheckpointBaseOp------------------------------------------*/
ObSSDoCheckpointBaseOp::ObSSDoCheckpointBaseOp()
  : ckpt_ctx_(nullptr), item_buf_(nullptr), item_buf_size_(0)
{}

int ObSSDoCheckpointBaseOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheBaseOp::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  }
  return ret;
}

void ObSSDoCheckpointBaseOp::destroy()
{
  ckpt_ctx_ = nullptr;
  if (nullptr != item_buf_ && nullptr != task_ctx_ && nullptr != task_allocator()) {
    task_allocator()->free(item_buf_);
    item_buf_ = nullptr;
  }
  item_buf_size_ = 0;
  ObSSMicroCacheBaseOp::destroy();
}

int ObSSDoCheckpointBaseOp::execute_checkpoint()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (check_need_execute()) {
    execute_checkpoint_op();
    const int64_t cost_us = get_cost_us();
    LOG_TRACE("ss_micro_cache: finish ss_micro_cache ckpt", KR(ret), K(cost_us), KPC_(ckpt_ctx));
  }
  return ret;
}

int ObSSDoCheckpointBaseOp::execute_checkpoint_op()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_op())) {
    LOG_WARN("fail to start ckpt operation", KR(ret));
  } else if (OB_FAIL(gen_checkpoint())) {
    LOG_WARN("fail to gen checkpoint", KR(ret));
  }
  finish_op();
  return ret;
}

int ObSSDoCheckpointBaseOp::start_op()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_) || OB_UNLIKELY(!task_ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (FALSE_IT(ckpt_ctx_->reuse())) {
  } else if (OB_FAIL(inner_ensure_ckpt_ctx_valid())) {
    LOG_WARN("fail to ensure ckpt_ctx valid", KR(ret), KPC_(ckpt_ctx));
  }
  return ret;
}

void ObSSDoCheckpointBaseOp::finish_op()
{
  if (is_op_enabled()) {
    handle_extra_func();
  }
}

bool ObSSDoCheckpointBaseOp::has_free_ckpt_blk()
{
  bool exist_free_blk = false;
  if (nullptr != phy_blk_mgr()) {
    if (ObSSMicroCacheTaskOpType::SS_DO_BLK_CKPT_OP == op_type_) {
      exist_free_blk = phy_blk_mgr()->exist_free_block(ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK);
    } else if (ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_META_OP == op_type_) {
      exist_free_blk = phy_blk_mgr()->exist_free_block(ObSSPhyBlockType::SS_MICRO_META_BLK);
    }
  }
  return exist_free_blk;
}

int ObSSDoCheckpointBaseOp::get_ckpt_block_id_list(
    ObSSCkptPhyBlockItemWriter &item_writer,
    common::ObIArray<int64_t> &block_id_list,
    const bool is_failed_blk_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    // if fail to get_block_id_list(cuzed by memory not enough), we may leak these phy_blocks, thus need to retry
    int64_t max_retry_cnt = 600;
    do {
      ret = OB_SUCCESS;
      if (OB_FAIL(item_writer.get_block_id_list(block_id_list, is_failed_blk_list))) {
        LOG_ERROR("fail to get block_id list", KR(ret), K(is_failed_blk_list), KPC_(ckpt_ctx));
        ob_usleep(1000 * 1000);
      }
    } while (OB_FAIL(ret) && (--max_retry_cnt > 0));
  }
  return ret;
}

int ObSSDoCheckpointBaseOp::inner_ensure_ckpt_ctx_valid()
{
  int ret = OB_SUCCESS;
  bool is_prev_super_blk_valid = ckpt_ctx_->prev_super_blk_.is_ckpt_info_valid();
  bool is_cur_super_blk_valid = ckpt_ctx_->cur_super_blk_.is_ckpt_info_valid();
  bool is_valid = (is_prev_super_blk_valid && is_cur_super_blk_valid);
  if (OB_LIKELY(is_valid)) {
    // skip
  } else {
    if (!is_prev_super_blk_valid && !is_cur_super_blk_valid) {
      LOG_ERROR("NOTICE: please check micro_meta ckpt_ctx", KPC_(ckpt_ctx));
    }

    uint32_t ckpt_split_cnt = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(ckpt_split_cnt))) {
      LOG_WARN("fail to get micro_ckpt_split_cnt", KR(ret));
    } else if (!is_prev_super_blk_valid) {
      ckpt_ctx_->prev_super_blk_.reset();
      if (OB_FAIL(ckpt_ctx_->prev_super_blk_.init_non_basic_info(tenant_id_, ckpt_split_cnt))) {
        LOG_WARN("fail to init non_basic info", KR(ret), K_(tenant_id), K(ckpt_split_cnt), KPC_(ckpt_ctx));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_cur_super_blk_valid) {
      ckpt_ctx_->cur_super_blk_.reset();
      if (OB_FAIL(ckpt_ctx_->cur_super_blk_.init_non_basic_info(tenant_id_, ckpt_split_cnt))) {
        LOG_WARN("fail to init non_basic info", KR(ret), K_(tenant_id), K(ckpt_split_cnt), KPC_(ckpt_ctx));
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
