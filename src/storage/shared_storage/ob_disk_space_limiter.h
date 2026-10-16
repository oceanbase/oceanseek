/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_LIMITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_LIMITER_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

class ObDiskSpaceLimiter
{
public:
  ObDiskSpaceLimiter(const int64_t max);
  ~ObDiskSpaceLimiter();

  void reset();
  int add_child(ObDiskSpaceLimiter *node);
  void del_child(ObDiskSpaceLimiter *node);
  bool has_child();
  // Returns true when there are at least two children in the linked list.
  // (Used by share-tenant limiter to check whether meta/user tenant limiters are both attached.)
  bool has_children();
  int64_t get_used() const;
  int64_t get_max() const;
  int set_max(const int64_t max);
  int update_max(const int64_t delta_max);
  // Force update used_ without checking max_ (i.e. may temporarily make used_ > max_).
  //
  // Motivation:
  // - During restart / replay / calibration, the in-memory quota hierarchy (especially the
  //   share-tenant limiter) can be temporarily incomplete before the meta/user tenant pair is
  //   fully created and attached.
  // - In that window, updating used_ via acquire() may fail with OB_SERVER_OUTOF_DISK_SPACE even
  //   though the "calibrated" used_ reflects the real on-disk usage.
  //
  // Contract:
  // - This method is intended for calibration/restore paths to reconcile counters.
  // - It propagates positive deltas to parent_ so the hierarchy stays consistent.
  // - It may cause subsequent allocations to fail until max_ is updated to cover the real usage,
  //   which is acceptable during the restart window.
  void force_update_used(const int64_t delta_used);
  int acquire(const int64_t quota);
  void release(const int64_t quota);

  // 首次校准完成前，used_ 由可能过期的 disk_space_meta 播种、并不可信，此时 over-release 属预期现象，
  // 应降级为 WARN，避免刷 ERROR 误报。默认 true，由 ObTenantDiskSpaceManager 在 init 后到首次校准
  // 成功前置为 false、校准成功后置回 true。
  void set_first_calibration_done(const bool is_first_calibration_done)
  {
    ATOMIC_STORE(&is_first_calibration_done_, is_first_calibration_done);
  }

  TO_STRING_KV(K(max_), K(used_), KP(parent_), KP(child_), KP(next_), K(is_first_calibration_done_));

private:
  bool is_quota_valid_() const;

private:
  int64_t max_;
  int64_t used_;
  bool is_first_calibration_done_; // 默认 true；为 false 时 over-release 打 WARN 而非 ERROR

  common::SpinRWLock lock_; // serializes structural modifications
  ObDiskSpaceLimiter *parent_;
  ObDiskSpaceLimiter *child_;
  ObDiskSpaceLimiter *next_;
};

class ObShareTenantDiskSpaceLimiterManager;
class ObShareTenantDiskSpaceLimiter
{
public:
  ObShareTenantDiskSpaceLimiter(const uint64_t tenant_id)
      : tenant_id_(tenant_id), limiter_(0), next_(nullptr),
        lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK),
        tenant_hot_macro_used_{0, 0}
  {
  }
  ~ObShareTenantDiskSpaceLimiter() {}

  bool has_child() { return limiter_.has_child(); }
  bool has_children() { return limiter_.has_children(); }
  int64_t get_used() const { return limiter_.get_used(); }
  int64_t get_free() const { return get_max() - get_used(); }
  int64_t get_max() const { return limiter_.get_max(); }
  int set_max(const int64_t max) { return limiter_.set_max(max); }
  int update_max(const int64_t delta_max) { return limiter_.update_max(delta_max); }
  int add_child(ObDiskSpaceLimiter *node) { return limiter_.add_child(node); }
  void del_child(ObDiskSpaceLimiter *node) { limiter_.del_child(node); }

  int set_tenant_hot_macro_used(const uint64_t tenant_id, const int64_t hot_macro_used);
  int64_t get_total_hot_macro_used();

private:
  friend class ObShareTenantDiskSpaceLimiterManager;

private:
  uint64_t tenant_id_;
  ObDiskSpaceLimiter limiter_;
  ObShareTenantDiskSpaceLimiter *next_;
  
  common::SpinRWLock lock_; // protects tenant_hot_macro_used_
  int64_t tenant_hot_macro_used_[2];
};

class ObShareTenantDiskSpaceLimiterManager
{
public:
  ObShareTenantDiskSpaceLimiterManager();
  virtual ~ObShareTenantDiskSpaceLimiterManager();
  virtual void destroy();
  int init();

  int add_child_tenant_limiter(const uint64_t tenant_id, ObDiskSpaceLimiter *node);
  void del_child_tenant_limiter(const uint64_t tenant_id, ObDiskSpaceLimiter *node);
  void try_del_share_tenant_limiter(const uint64_t tenant_id);

  int64_t get_share_tenant_total_size(const uint64_t tenant_id);
  int update_share_tenant_total_size(const uint64_t tenant_id, const int64_t delta_disk_size);
  int64_t get_share_tenant_free_size(const uint64_t tenant_id);
  int set_tenant_hot_macro_used(const uint64_t tenant_id, const int64_t hot_macro_used);
  int64_t get_share_tenant_total_hot_macro_used(const uint64_t tenant_id);

  // Used during restart: meta-tenant and its paired user-tenant may not be created/restored in
  // lockstep, so the share-tenant limiter can be temporarily incomplete (share quota not stable).
  // Some best-effort tasks (persist/calibrate, and restoring macro cache stats) should
  // be gated on this to avoid transient OB_SERVER_OUTOF_DISK_SPACE.
  bool is_share_tenant_disk_space_quota_initialized(const uint64_t tenant_id);

private:
  int create_share_tenant_limiter_(const uint64_t tenant_id, ObShareTenantDiskSpaceLimiter *&limiter);

protected:
  bool is_inited_;

private:
  using ShareTenantLimiterMap = common::hash::ObHashMap<uint64_t, ObShareTenantDiskSpaceLimiter *>;
  static constexpr int64_t MAP_BUCKET_NUM = 64;
  static constexpr const char MALLOC_LABEL[] = "SSTntLimiterMgr";

  // mete_tenant_id -> ObShareTenantDiskSpaceLimiter *
  ShareTenantLimiterMap share_tenant_limiter_map_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_LIMITER_H_ */
