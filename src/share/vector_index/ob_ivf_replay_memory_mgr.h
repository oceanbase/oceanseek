/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_IVF_REPLAY_MEMORY_MGR_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_IVF_REPLAY_MEMORY_MGR_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace share
{
class ObIVFReplayMemoryMgr;

// One reservation per INITIAL_BUILD or merge, shared by all of its capture slices.
// The owning build must release its replay stores before resetting this guard.
class ObIVFReplayMemoryReservation
{
public:
  ObIVFReplayMemoryReservation()
    : mgr_(nullptr), reserved_bytes_(0), slice_memory_limit_(0),
      slice_count_(0), configured_limit_(0)
  {}
  ~ObIVFReplayMemoryReservation() { reset(); }

  void reset();
  bool is_reserved() const { return nullptr != mgr_; }
  int64_t get_reserved_bytes() const { return reserved_bytes_; }
  // Zero means no retained-data quota, NOT ObRADatumStore's zero-limit mode.
  // The caller must use a positive minimal spill threshold in that case.
  int64_t get_slice_memory_limit() const { return slice_memory_limit_; }
  int64_t get_slice_count() const { return slice_count_; }
  int64_t get_configured_limit() const { return configured_limit_; }

private:
  friend class ObIVFReplayMemoryMgr;
  ObIVFReplayMemoryMgr *mgr_;
  int64_t reserved_bytes_;
  int64_t slice_memory_limit_;
  int64_t slice_count_;
  int64_t configured_limit_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFReplayMemoryReservation);
};

// A tenant-local admission budget on one observer. This accounts for retained
// replay/sample-data quotas, not all IVF allocations: the current write block,
// file indexes, reader buffers, training workspaces and allocator overhead are
// extra. After capture seals, successive cluster/refinement sample phases can
// borrow its unused quota; those phases must never retain samples concurrently.
// There is no waiting, redistribution or retroactive revocation of a quota.
class ObIVFReplayMemoryMgr
{
public:
  ObIVFReplayMemoryMgr()
    : lock_(common::ObLatchIds::OB_IVF_BUILD_HELPER_LOCK),
      reserved_bytes_(0), active_build_count_(0)
  {}
  ~ObIVFReplayMemoryMgr() = default;

  static int mtl_init(ObIVFReplayMemoryMgr *&mgr)
  {
    return nullptr == mgr ? common::OB_INVALID_ARGUMENT : common::OB_SUCCESS;
  }
  // The MTL registration precedes the DDL owners so all reservation guards
  // have been released before the manager is destroyed during tenant teardown.
  void destroy() {}

  int reserve(const int64_t configured_limit, const int64_t slice_count,
              ObIVFReplayMemoryReservation &reservation)
  {
    int ret = common::OB_SUCCESS;
    if (configured_limit < 0 || slice_count <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (reservation.is_reserved()) {
      ret = common::OB_INIT_TWICE;
    } else {
      common::ObSpinLockGuard guard(lock_);
      // Zero grants no retained-data quota. A decreased configuration can
      // be below live reservations; new builds spill until quota returns.
      const int64_t available = configured_limit > reserved_bytes_
          ? configured_limit - reserved_bytes_ : 0;
      const int64_t slice_limit = available / slice_count;
      if (slice_limit > (INT64_MAX - reserved_bytes_) / slice_count
          || INT64_MAX == active_build_count_) {
        ret = common::OB_SIZE_OVERFLOW;
      } else {
        reservation.mgr_ = this;
        reservation.reserved_bytes_ = slice_limit * slice_count;
        reservation.slice_memory_limit_ = slice_limit;
        reservation.slice_count_ = slice_count;
        reservation.configured_limit_ = configured_limit;
        reserved_bytes_ += reservation.reserved_bytes_;
        ++active_build_count_;
      }
    }
    return ret;
  }

  int64_t get_reserved_bytes() const
  {
    common::ObSpinLockGuard guard(lock_);
    return reserved_bytes_;
  }
  int64_t get_active_build_count() const
  {
    common::ObSpinLockGuard guard(lock_);
    return active_build_count_;
  }

private:
  friend class ObIVFReplayMemoryReservation;
  void release_(const int64_t bytes)
  {
    common::ObSpinLockGuard guard(lock_);
    OB_ASSERT(bytes >= 0 && bytes <= reserved_bytes_ && active_build_count_ > 0);
    reserved_bytes_ -= bytes;
    --active_build_count_;
  }

  mutable common::ObSpinLock lock_;
  int64_t reserved_bytes_;
  int64_t active_build_count_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFReplayMemoryMgr);
};

inline void ObIVFReplayMemoryReservation::reset()
{
  if (nullptr != mgr_) {
    mgr_->release_(reserved_bytes_);
    mgr_ = nullptr;
  }
  reserved_bytes_ = 0;
  slice_memory_limit_ = 0;
  slice_count_ = 0;
  configured_limit_ = 0;
}

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_IVF_REPLAY_MEMORY_MGR_H_
