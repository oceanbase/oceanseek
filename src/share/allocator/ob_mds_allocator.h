/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ALLOCATOR_OB_MDS_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_MDS_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "share/throttle/ob_share_throttle_define.h"
#include "share/ob_ls_id.h"

namespace oceanbase {
namespace share {

struct ObTenantBufferCtxAllocator : public common::ObIAllocator// for now, it is just a wrapper of mtl_malloc
{
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const common::ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const common::ObMemAttr &) override {}
};

// Two-phase throttling for the MDS memstore bucket.
//
// LS_MDS_TABLET memtable writes happen inside the part_ctx lock (register /
// replay of multi source data), so ObMemstoreAllocator::alloc only records the
// throttled size into mds_memstore_throttled_alloc() there, and
// ObStorageTableGuard deliberately skips MDS-kind memtables. This guard is
// placed around register_multi_data_source / replay_multi_data_source and
// performs the real throttle sleep in its destructor, after the tx ctx lock
// has been released.
class ObMdsThrottleGuard
{
public:
  ObMdsThrottleGuard(const share::ObLSID ls_id, const bool for_replay, const int64_t abs_expire_time);
  ~ObMdsThrottleGuard();

private:
  const share::ObLSID ls_id_;
  bool for_replay_;
  int64_t abs_expire_time_;
  share::TxShareThrottleTool *throttle_tool_;
};

}  // namespace share
}  // namespace oceanbase

#endif
