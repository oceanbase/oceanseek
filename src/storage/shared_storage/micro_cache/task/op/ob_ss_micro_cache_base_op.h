/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_OP_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_task_info.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
class ObSSMemBlockManager;
class ObSSMicroMetaManager;
class ObSSPhysicalBlockManager;
class ObTenantFileManager;
class ObSSLAMicroKeyManager;

class ObSSMicroCacheBaseOp
{
public:
  ObSSMicroCacheBaseOp() 
    : is_inited_(false), is_enabled_(false), is_closed_(false), op_type_(ObSSMicroCacheTaskOpType::SS_INVALID_OP),
      tenant_id_(OB_INVALID_TENANT_ID), start_time_us_(0), task_ctx_(nullptr)
  {}
  virtual ~ObSSMicroCacheBaseOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  
  bool is_closed() const { return is_closed_; }
  bool is_op_enabled() const { return is_enabled_; }
  void enable_op() { is_enabled_ = true; }
  void disable_op() { is_enabled_ = false; }

protected:
  int64_t get_cost_us() const;
  // Update is_closed and task start time. Call after init checks, before op logic.
  // Returns true if op is enabled (caller should proceed with main logic).
  bool check_need_execute();

  common::ObIAllocator *task_allocator();
  ObSSMemBlockManager *mem_blk_mgr();
  ObSSMicroMetaManager *micro_meta_mgr();
  ObSSMicroRangeManager *micro_range_mgr();
  ObSSPhysicalBlockManager *phy_blk_mgr();
  ObTenantFileManager *tnt_file_mgr();
  ObSSMicroCacheStat *cache_stat();
  ObSSLAMicroKeyManager *la_micro_mgr();

protected:
  bool is_inited_;
  volatile bool is_enabled_; // If set as true, the task_op main processing logic won't be invoked in next round
  bool is_closed_; // When is_enabled=false and finish current round execution, is_closed will be set as true
  ObSSMicroCacheTaskOpType op_type_;
  uint64_t tenant_id_;
  int64_t start_time_us_; // the start time of task in each round
  ObSSMicroCacheTaskCtx *task_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASE_OP_H_ */