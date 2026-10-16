/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "ob_mds_allocator.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase {
namespace share {

void *ObTenantBufferCtxAllocator::alloc(const int64_t size)
{
  void *obj = share::mtl_malloc(size, ObMemAttr(MTL_ID(), "MDS_CTX_DEFAULT", ObCtxIds::MDS_CTX_ID));
  return obj;
}

void *ObTenantBufferCtxAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  void *obj = share::mtl_malloc(size, attr);
  return obj;
}

void ObTenantBufferCtxAllocator::free(void *ptr)
{
  share::mtl_free(ptr);
}

ObMdsThrottleGuard::ObMdsThrottleGuard(const share::ObLSID ls_id, const bool for_replay, const int64_t abs_expire_time)
    : ls_id_(ls_id), for_replay_(for_replay), abs_expire_time_(abs_expire_time)
{
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  if (0 == abs_expire_time) {
    abs_expire_time_ =
        ObClockGenerator::getClock() + ObThrottleUnit<ObMdsMemstoreAllocator>::DEFAULT_MAX_THROTTLE_TIME;
  }
  share::mds_memstore_throttled_alloc() = 0;
}

ObMdsThrottleGuard::~ObMdsThrottleGuard()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObThrottleInfoGuard share_ti_guard;
  ObThrottleInfoGuard module_ti_guard;

  if (OB_ISNULL(throttle_tool_)) {
    SHARE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "throttle tool is unexpected nullptr", KP(throttle_tool_));
  } else if (throttle_tool_->is_throttling<ObMdsMemstoreAllocator>(share_ti_guard, module_ti_guard)) {

    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      STORAGE_LOG(WARN, "get ls handle failed", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "get ls handle failed", KR(ret), K(ls_id_));
    } else {
      (void)TxShareMemThrottleUtil::do_throttle<ObMdsMemstoreAllocator>(for_replay_,
                                                                        abs_expire_time_,
                                                                        share::mds_memstore_throttled_alloc(),
                                                                        *(ls_handle.get_ls()),
                                                                        *throttle_tool_,
                                                                        share_ti_guard,
                                                                        module_ti_guard);
    }

    if (throttle_tool_->still_throttling<ObMdsMemstoreAllocator>(share_ti_guard, module_ti_guard)) {
      (void)throttle_tool_->skip_throttle<ObMdsMemstoreAllocator>(
          share::mds_memstore_throttled_alloc(), share_ti_guard, module_ti_guard);

      if (module_ti_guard.is_valid()) {
        module_ti_guard.throttle_info()->reset();
      }
    }

    share::mds_memstore_throttled_alloc() = 0;
  } else {
    // do not need throttle, exit directly
  }
}

void ObMdsMemstoreAllocator::init_throttle_config(int64_t &resource_limit,
                                                  int64_t &trigger_percentage,
                                                  int64_t &max_duration)
{
  const int64_t MDS_LIMIT_PERCENTAGE = 5;
  const int64_t THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  const int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    resource_limit = total_memory * MDS_LIMIT_PERCENTAGE / 100;
    trigger_percentage = THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = THROTTLE_MAX_DURATION;
  } else {
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
    resource_limit = total_memory * tenant_config->_mds_memory_limit_percentage / 100LL;
  }
}

}  // namespace share
}  // namespace oceanbase
