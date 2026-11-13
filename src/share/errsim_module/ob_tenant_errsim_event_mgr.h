/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SHARE_ERRSIM_OB_TENANT_ERRSIM_EVENT_MGR_H_
#define SRC_SHARE_ERRSIM_OB_TENANT_ERRSIM_EVENT_MGR_H_

#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "common/errsim_module/ob_tenant_errsim_event.h"

namespace oceanbase
{
namespace share
{

class ObTenantErrsimEventMgr
{
public:
  static constexpr int64_t MAX_EVENT_COUNT = 2000;
  ObTenantErrsimEventMgr();
  virtual ~ObTenantErrsimEventMgr();
  static int mtl_init(ObTenantErrsimEventMgr *&errsim_event_mgr);
  int init(const uint64_t tenant_id);
  int add_tenant_event(
      const ObTenantErrsimEvent &event);
  // Copy a stable snapshot, oldest first. Reserve memory before taking the read lock.
  int get_events(common::ObArray<common::ObTenantErrsimEvent> &events);
  void destroy();
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObArray<common::ObTenantErrsimEvent> event_array_;
  int64_t next_pos_;
  uint64_t next_event_id_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantErrsimEventMgr);
};

} // namespace share
} // namespace oceanbase
#endif
