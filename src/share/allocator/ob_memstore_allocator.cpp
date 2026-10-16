/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_memstore_allocator.h"
#include "ob_shared_memory_allocator_mgr.h"

namespace oceanbase
{
using namespace share;
namespace share 
{
int FrozenMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_frozen()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ActiveMemstoreInfoLogger::operator()(ObDLink* link)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator::AllocHandle* handle = CONTAINER_OF(link, typeof(*handle), total_list_);
  memtable::ObMemtable& mt = handle->mt_;
  if (handle->is_active()) {
    if (OB_FAIL(databuff_print_obj(buf_, limit_, pos_, mt))) {
    } else {
      ret = databuff_printf(buf_, limit_, pos_, ",");
    }
  }
  return ret;
}

int ObMemstoreAllocator::AllocHandle::bind(const ObMemstoreResourceKind kind)
{
  int ret = OB_SUCCESS;
  ObMemstoreAllocator &host = MTL(ObSharedMemAllocMgr *)->get_memstore_allocator_by_kind(kind);
  (void)host.register_handle(*this);
  return ret;
}

void* ObMemstoreAllocator::AllocHandle::alloc(const int64_t size)
{
  void *ptr = NULL;
  // host_ is NULL only before bind() or after destroy(). Alloc on an unbound
  // handle is not a legal idle state; treat it as a bug rather than OOM.
  if (OB_ISNULL(host_)) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "alloc on unbound memstore allocator handle",
                   KP(this), K(size), K(mt_));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  } else {
    ptr = host_->alloc(*this, size);
  }
  return ptr;
}

int ObMemstoreAllocator::init(const int64_t ctx_id, const ObMemstoreResourceKind kind)
{
  throttle_tool_ = &(MTL(ObSharedMemAllocMgr *)->share_resource_throttle_tool());
  kind_ = kind;
  return arena_.init(ctx_id, get_label_by_kind(kind));
}

void ObMemstoreAllocator::register_handle(AllocHandle& handle)
{
  handle.do_reset();
  handle.set_host(this);
  {
    int64_t nway = nway_per_group();
    LockGuard guard(lock_);
    hlist_.init_handle(handle);
    arena_.update_nway_per_group(nway);
  }
  COMMON_LOG(TRACE, "MTALLOC.init", KP(&handle.mt_));
}

void ObMemstoreAllocator::unregister_handle(AllocHandle& handle)
{
  ObTimeGuard time_guard("ObMemstoreAllocator::unregister_handle", 100 * 1000);
  COMMON_LOG(TRACE, "MTALLOC.destroy", KP(&handle.mt_));
  arena_.free(handle.arena_handle_);
  time_guard.click();
  {
    LockGuard guard(lock_);
    time_guard.click();
    hlist_.destroy_handle(handle);
    time_guard.click();
    if (hlist_.is_empty()) {
      arena_.reset();
    }
    time_guard.click();
  }
  handle.do_reset();
}

void* ObMemstoreAllocator::alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts)
{
  int ret = OB_SUCCESS;
  int64_t align_size = upper_align(size, sizeof(int64_t));
  uint64_t tenant_id = arena_.get_tenant_id();

  bool is_out_of_mem = false;
  if (!handle.is_id_valid()) {
    COMMON_LOG(TRACE, "MTALLOC.first_alloc", KP(&handle.mt_));
    LockGuard guard(lock_);
    if (handle.is_frozen()) {
      ret = OB_EAGAIN;
      if (!handle.mt_.get_offlined()) {
        COMMON_LOG(ERROR, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      } else {
        COMMON_LOG(WARN, "cannot alloc because allocator is frozen", K(ret), K(handle.mt_));
      }
    } else if (!handle.is_id_valid()) {
      handle.set_clock(arena_.retired());
      hlist_.set_active(handle);
    }
  }

  if (OB_SUCC(ret)) {
    ret = check_memory_limit_(tenant_id, align_size, is_out_of_mem);
  }

  void *res = nullptr;
  if (OB_FAIL(ret) || is_out_of_mem) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "this tenant is already out of memstore limit or some thing wrong.", K(tenant_id));
    }
    res = nullptr;
  } else {
    bool is_throttled = false;
    // Record on the throttle bucket matching this arena's resource kind.
    (void)throttle_tool_->memstore_alloc_resource(kind_, align_size, expire_ts, is_throttled);
    if (is_throttled) {
      acc_tl_memstore_by_kind(kind_, align_size);
    }
    res = arena_.alloc(handle.id_, handle.arena_handle_, align_size);
  }
  return res;
}

void ObMemstoreAllocator::set_frozen(AllocHandle& handle)
{
  COMMON_LOG(TRACE, "MTALLOC.set_frozen", KP(&handle.mt_));
  LockGuard guard(lock_);
  hlist_.set_frozen(handle);
}

static int64_t calc_nway(int64_t cpu, int64_t mem)
{
  return std::min(cpu, mem/20/ObFifoArena::PAGE_SIZE);
}

int64_t ObMemstoreAllocator::nway_per_group()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arena_.get_tenant_id();
  double min_cpu = 0;
  double max_cpu = 0;
  int64_t max_memory = 0;
  int64_t min_memory = 0;
  omt::ObMultiTenant *omt = GCTX.omt_;

  MTL_SWITCH(tenant_id) {
    storage::ObTenantFreezer *freezer = nullptr;
    if (NULL == omt) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "omt should not be null", K(tenant_id), K(ret));
    } else if (OB_FAIL(omt->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
      COMMON_LOG(WARN, "get tenant cpu failed", K(tenant_id), K(ret));
    } else if (FALSE_IT(freezer = MTL(storage::ObTenantFreezer *))) {
    } else if (OB_FAIL(freezer->get_tenant_mem_limit(min_memory, max_memory))) {
      COMMON_LOG(WARN, "get tenant mem limit failed", K(tenant_id), K(ret));
    }
  }
  return OB_SUCCESS == ret? calc_nway((int64_t)max_cpu, min_memory): 0;
}

int64_t ObMemstoreAllocator::resource_unit_size()
{
  static const int64_t MEMSTORE_RESOURCE_UNIT_SIZE = 2LL * 1024LL * 1024LL; /* 2MB */
  return MEMSTORE_RESOURCE_UNIT_SIZE;
}

void ObMemstoreAllocator::init_throttle_config(int64_t &resource_limit,
                                               int64_t &trigger_percentage,
                                               int64_t &max_duration)
{
  // define some default value
  const int64_t MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE = 60;
  const int64_t MEMSTORE_THROTTLE_MAX_DURATION = 2LL * 60LL * 60LL * 1000LL * 1000LL;  // 2 hours

  int64_t total_memory = lib::get_tenant_memory_limit(MTL_ID());

  // Use tenant config to init throttle config
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    trigger_percentage = tenant_config->writing_throttling_trigger_percentage;
    max_duration = tenant_config->writing_throttling_maximum_duration;
  } else {
    COMMON_LOG_RET(WARN, OB_INVALID_CONFIG, "init throttle config with default value");
    trigger_percentage = MEMSTORE_THROTTLE_TRIGGER_PERCENTAGE;
    max_duration = MEMSTORE_THROTTLE_MAX_DURATION;
  }
  resource_limit = total_memory * MTL(storage::ObTenantFreezer *)->get_memstore_limit_percentage() / 100;
}

void ObMemstoreAllocator::adaptive_update_limit(const int64_t tenant_id,
                                                const int64_t holding_size,
                                                const int64_t config_specify_resource_limit,
                                                int64_t &resource_limit,
                                                int64_t &last_update_limit_ts,
                                                bool &is_updated)
{
  // do nothing
}

int ObMemstoreAllocator::check_memory_limit_(const uint64_t tenant_id,
                                             const int64_t alloc_size,
                                             bool &is_out_of_mem) const
{
  int ret = OB_SUCCESS;
  is_out_of_mem = false;
  if (is_virtual_tenant_id(tenant_id)) {
    // Virtual tenants should not have memstores.
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "virtual tenant should not have memstore", K(ret), K(tenant_id));
  } else if (ObMemstoreResourceKind::DATA == kind_) {
    storage::ObTenantFreezer *freezer = MTL(storage::ObTenantFreezer *);
    if (OB_ISNULL(freezer)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "tenant freezer is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(freezer->check_memstore_full_internal(is_out_of_mem))) {
      COMMON_LOG(ERROR, "fail to check data memstore limit", K(ret), K(tenant_id));
    }
  } else if (ObMemstoreResourceKind::MDS == kind_) {
    // The MDS throttle unit is a rate model, not a cap: once hold is above the
    // throttle trigger the virtual release clock advances proportionally to
    // hold, so queued allocations keep being admitted however far above the
    // limit hold grows. Enforce a hard limit here as the last-resort backstop
    // for the cases where the MDS self-freeze path cannot reclaim the memory
    // (a stuck redo replay keeps writing into LS_MDS_TABLET).
    // The check is on the MDS module bucket only: the DATA hard-limit check
    // likewise accounts for the DATA memstore alone, and a share-bucket check
    // here would couple MDS availability to DATA/tx-data/vector usage (when
    // the share quota is exhausted by the other modules, MDS allocations
    // would be hard-rejected while MDS itself is far below its own limit).
    // The tenant-wide share quota still guards MDS through the rate-model
    // throttle path, which is shared by all four modules.
    // OB_ALLOCATE_MEMORY_FAILED stays in the replay retry whitelist
    // (ObReplayStatus::is_fatal_error), so replay keeps retrying instead of
    // stopping the LS, and the DATA memstore full path behaves the same way.
    if (OB_ISNULL(throttle_tool_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "throttle tool is null", K(ret), K(tenant_id));
    } else if (throttle_tool_->exceeded_module_resource_limit<ObMdsMemstoreAllocator>(alloc_size)) {
      is_out_of_mem = true;
      COMMON_LOG(WARN,
                 "mds memstore has reached its memory limit",
                 K(tenant_id),
                 K(alloc_size),
                 K(hold()));
    }
  }
  return ret;
}

};  // namespace share
};  // namespace oceanbase
