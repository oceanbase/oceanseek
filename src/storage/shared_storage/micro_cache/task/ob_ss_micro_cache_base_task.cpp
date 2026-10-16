/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_base_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSMicroCacheBaseTask------------------------------------------*/
int ObSSMicroCacheBaseTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (interval_us < 0) || (!task_ctx.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(interval_us), "valid_task_ctx", task_ctx.is_valid());
  } else {
    tenant_id_ = tenant_id;
    ori_interval_us_ = interval_us;
    cur_interval_us_ = interval_us;
    task_ctx_ = &task_ctx;
  }
  return ret;
}

void ObSSMicroCacheBaseTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ori_interval_us_ = 0;
  cur_interval_us_ = 0;
  task_ctx_ = nullptr;
  is_inited_ = false;
}

// Upper level should promise task_ctx is valid
ObSSMemBlockManager *ObSSMicroCacheBaseTask::mem_blk_mgr() { return task_ctx_->mem_blk_mgr_; }
ObSSMicroMetaManager *ObSSMicroCacheBaseTask::micro_meta_mgr() { return task_ctx_->micro_meta_mgr_; }
ObSSPhysicalBlockManager *ObSSMicroCacheBaseTask::phy_blk_mgr() { return task_ctx_->phy_blk_mgr_; }
ObTenantFileManager *ObSSMicroCacheBaseTask::tnt_file_mgr() { return task_ctx_->tnt_file_mgr_; }
ObSSMicroCacheStat *ObSSMicroCacheBaseTask::cache_stat() { return task_ctx_->cache_stat_; }
ObSSLAMicroKeyManager *ObSSMicroCacheBaseTask::la_micro_mgr() { return task_ctx_->la_micro_mgr_; }

} // storage
} // oceanbase
