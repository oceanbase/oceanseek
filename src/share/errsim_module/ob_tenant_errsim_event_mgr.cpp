/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "ob_tenant_errsim_event_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace lib;

namespace share
{

ObTenantErrsimEventMgr::ObTenantErrsimEventMgr()
    : is_inited_(false),
      lock_(common::ObLatchIds::ERRSIM_LOCK),
      event_array_(OB_MALLOC_NORMAL_BLOCK_SIZE,
          ModulePageAllocator("TErrsimEvent", OB_SERVER_TENANT_ID)),
      next_pos_(0),
      next_event_id_(1)
{
}

ObTenantErrsimEventMgr::~ObTenantErrsimEventMgr()
{
  destroy();
}

int ObTenantErrsimEventMgr::mtl_init(ObTenantErrsimEventMgr *&errsim_event_mgr)
{
  return errsim_event_mgr->init(MTL_ID());
}

void ObTenantErrsimEventMgr::destroy()
{
  common::SpinWLockGuard guard(lock_);
  is_inited_ = false;
  event_array_.destroy();
  next_pos_ = 0;
  next_event_id_ = 1;
}

int ObTenantErrsimEventMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::ErrsimModuleGuard errsim_guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    event_array_.set_tenant_id(tenant_id);
    if (OB_FAIL(event_array_.reserve(MAX_EVENT_COUNT))) {
      LOG_WARN("failed to reserve errsim event buffer", K(ret), K(tenant_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTenantErrsimEventMgr::add_tenant_event(const ObTenantErrsimEvent &event)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!event.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Capacity was reserved at init; this path must never allocate or log.
    if (event_array_.count() < MAX_EVENT_COUNT) {
      ret = event_array_.push_back(event);
    } else {
      event_array_.at(next_pos_) = event;
    }
    if (OB_SUCC(ret)) {
      event_array_.at(next_pos_).event_id_ = next_event_id_++;
      next_pos_ = (next_pos_ + 1) % MAX_EVENT_COUNT;
    }
  }
  return ret;
}

int ObTenantErrsimEventMgr::get_events(common::ObArray<ObTenantErrsimEvent> &events)
{
  int ret = OB_SUCCESS;
  lib::ErrsimModuleGuard errsim_guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
  events.reuse();
  // Never allocate while holding lock_: allocation can itself visit errsim hooks.
  if (OB_FAIL(events.reserve(MAX_EVENT_COUNT))) {
  } else {
    common::SpinRLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else {
      const int64_t count = event_array_.count();
      const int64_t oldest = (MAX_EVENT_COUNT == count) ? next_pos_ : 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ret = events.push_back(event_array_.at((oldest + i) % MAX_EVENT_COUNT));
      }
    }
  }
  return ret;
}


} //share
} //oceanbase
