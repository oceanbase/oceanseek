/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_base_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

void ObSSMicroCacheBaseOp::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  start_time_us_ = 0;
  task_ctx_ = nullptr;
  is_enabled_ = false;
  is_closed_ = false;
  is_inited_ = false;
}

int ObSSMicroCacheBaseOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!task_ctx.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "valid_task_ctx", task_ctx.is_valid());
  } else {
    tenant_id_ = tenant_id;
    task_ctx_ = &task_ctx;
    is_enabled_ = true;
    is_closed_ = false;
  }
  return ret;
}

bool ObSSMicroCacheBaseOp::check_need_execute()
{
  bool need_execute = false;
  is_closed_ = (!is_op_enabled());
  if (is_op_enabled()) {
    cache_stat()->task_stat().update_task_start_time(op_type_, start_time_us_);
    need_execute = true;
  }
  return need_execute;
}

int64_t ObSSMicroCacheBaseOp::get_cost_us() const
{
  int ret = OB_SUCCESS;
  int64_t cost_us = 0;
  if (start_time_us_ > 0) {
    cost_us = ObTimeUtility::current_time_us() - start_time_us_;
    if (cost_us >= SS_SLOW_TASK_COST_US && ObSSMicroCacheTaskOpType::SS_PERSIST_MICRO_META_OP != op_type_) {
      LOG_WARN("slow micro_cache task", K_(op_type), K(cost_us), K_(start_time_us));
    }
  }
  return cost_us;
}

// Upper level should promise task_ctx is valid
ObIAllocator *ObSSMicroCacheBaseOp::task_allocator() { return task_ctx_->allocator_; }
ObSSMemBlockManager *ObSSMicroCacheBaseOp::mem_blk_mgr() { return task_ctx_->mem_blk_mgr_; }
ObSSMicroMetaManager *ObSSMicroCacheBaseOp::micro_meta_mgr() { return task_ctx_->micro_meta_mgr_; }
ObSSMicroRangeManager *ObSSMicroCacheBaseOp::micro_range_mgr() { return task_ctx_->micro_range_mgr_; }
ObSSPhysicalBlockManager *ObSSMicroCacheBaseOp::phy_blk_mgr() { return task_ctx_->phy_blk_mgr_; }
ObTenantFileManager *ObSSMicroCacheBaseOp::tnt_file_mgr() { return task_ctx_->tnt_file_mgr_; }
ObSSMicroCacheStat *ObSSMicroCacheBaseOp::cache_stat() { return task_ctx_->cache_stat_; }
ObSSLAMicroKeyManager *ObSSMicroCacheBaseOp::la_micro_mgr() { return task_ctx_->la_micro_mgr_; }

} // storage
} // oceanbase
