/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_
#include "ob_handle_list.h"
#include "ob_fifo_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/throttle/ob_share_throttle_define.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
};

namespace share
{

// record the throttled alloc size of memstore in this thread
OB_INLINE int64_t &memstore_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, throttled_alloc);
  return throttled_alloc;
}

// Throttled alloc size of the MDS memstore bucket in this thread. Kept
// separate from memstore_throttled_alloc() because MDS allocation happens
// under the tx ctx lock and only records here; the sleep is compensated by
// ObMdsThrottleGuard outside the lock, and must not be disturbed by DATA
// write guards on the same thread.
OB_INLINE int64_t &mds_memstore_throttled_alloc()
{
  RLOCAL_INLINE(int64_t, mds_throttled_alloc);
  return mds_throttled_alloc;
}

struct FrozenMemstoreInfoLogger
{
  FrozenMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~FrozenMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};

struct ActiveMemstoreInfoLogger
{
  ActiveMemstoreInfoLogger(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  ~ActiveMemstoreInfoLogger() {}
  int operator()(ObDLink* link);
  char* buf_;
  int64_t limit_;
  int64_t pos_;
};


// Resource kind of a memstore allocator instance. LS_MDS_TABLET memtables use a
// dedicated ObMemstoreAllocator instance whose memory must be throttled on an
// independent bucket rather than sharing the user-data memstore bucket.
enum class ObMemstoreResourceKind : int64_t
{
  DATA = 0,
  MDS  = 1,
};

// Accumulate thread-local throttled alloc size on the bucket selected by
// `kind`. Kept strict (no fallback to DATA) so it stays consistent with
// ObShareResourceThrottleTool::memstore_alloc_resource.
OB_INLINE void acc_tl_memstore_by_kind(const ObMemstoreResourceKind kind, const int64_t size)
{
  switch (kind) {
    case ObMemstoreResourceKind::DATA:
      memstore_throttled_alloc() += size;
      break;
    case ObMemstoreResourceKind::MDS:
      mds_memstore_throttled_alloc() += size;
      break;
    default:
      COMMON_LOG_RET(ERROR, OB_NOT_SUPPORTED, "unsupported memstore resource kind",
                     "kind", static_cast<int64_t>(kind), K(size));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
      break;
  }
}


class ObMemstoreAllocator
{
public:
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Memstore);

  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  typedef ObMemstoreAllocator GAlloc;
  typedef ObFifoArena Arena;
  typedef ObHandleList HandleList;
  typedef HandleList::Handle ListHandle;
  typedef Arena::Handle ArenaHandle;

  class AllocHandle: public ListHandle, public ObIAllocator
  {
  public:
    memtable::ObMemtable& mt_;
    GAlloc* host_;
    ArenaHandle arena_handle_;
    AllocHandle(memtable::ObMemtable& mt): mt_(mt), host_(NULL) {
      do_reset();
    }
    void do_reset() {
      ListHandle::reset();
      arena_handle_.reset();
      host_ = NULL;
    }
    int64_t get_group_id() const { return id_ < 0? INT64_MAX: (id_ % Arena::MAX_CACHED_GROUP_COUNT); }
    // Bind once to the arena selected by the memtable's resource policy. The
    // kind is only a routing argument and is not duplicated in this handle.
    int bind(const ObMemstoreResourceKind kind);
    bool is_bound() const { return NULL != host_; }
    ObMemstoreResourceKind get_resource_kind() const;
    void set_host(GAlloc* host) { host_ = host; }
    void destroy() {
      if (NULL != host_) {
        host_->unregister_handle(*this);
        host_ = NULL;
      }
    }
    int64_t get_protection_clock() const { return get_clock(); }
    int64_t get_retire_clock() const
    {
      int64_t retire_clock = INT64_MAX;
      if (NULL != host_) {
        retire_clock = host_->get_retire_clock();
      }
      return retire_clock;
    }
    int64_t get_size() const { return arena_handle_.get_allocated(); }
    int64_t get_occupied_size() const { return get_size(); }
    // Defined in ob_memstore_allocator.cpp. host_ knows its own resource kind
    // (DATA / MDS) and records throttle resource on the matching bucket inside
    // alloc(); the sleep is executed by ObStorageTableGuard (DATA) or
    // ObMdsThrottleGuard (MDS, outside the tx ctx lock).
    void* alloc(const int64_t size);
    void* alloc(const int64_t size, const ObMemAttr &attr)
    {
      UNUSEDx(attr);
      return alloc(size);
    }
    void free(void* ptr) {
      UNUSED(ptr);
    }
    void set_frozen() {
      if (NULL != host_) {
        host_->set_frozen(*this);
      }
    }
    INHERIT_TO_STRING_KV("ListHandle", ListHandle, KP_(host), K_(arena_handle));
  };

public:
  ObMemstoreAllocator()
      : throttle_tool_(nullptr), lock_(common::ObLatchIds::MEMSTORE_ALLOCATOR_LOCK), hlist_(), arena_(),
        kind_(ObMemstoreResourceKind::DATA) {}
  ~ObMemstoreAllocator() {}
public:
  int init(const int64_t ctx_id, const ObMemstoreResourceKind kind);
  static lib::ObLabel get_label_by_kind(const ObMemstoreResourceKind kind);
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy() {}
  void register_handle(AllocHandle& handle);
  void unregister_handle(AllocHandle& handle);
  void* alloc(AllocHandle& handle, int64_t size, const int64_t expire_ts = 0);
  void set_frozen(AllocHandle& handle);
  template<typename Func>
  int for_each(Func& f, const bool reverse=false) {
    int ret = common::OB_SUCCESS;
    ObDLink* iter = NULL;
    LockGuard guard(lock_);
    while(OB_SUCC(ret) && NULL != (iter = (reverse ? hlist_.prev(iter) : hlist_.next(iter)))) {
      ret = f(iter);
    }
    return ret;
  }
public:
  int64_t get_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.allocated() - hazard);
  }
  int64_t get_freezable_active_memstore_used() {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: (arena_.retired() - hazard);
  }
  int64_t get_max_cached_memstore_size() const {
    return arena_.get_max_cached_memstore_size();
  }
  int64_t hold() const { return arena_.hold(); }
  int64_t get_total_memstore_used() const { return arena_.hold(); }
  int64_t get_frozen_memstore_pos() const {
    int64_t hazard = hlist_.hazard();
    return  hazard == INT64_MAX? 0: hazard;
  }
  int64_t get_memstore_reclaimed_pos() const { return arena_.reclaimed(); }
  int64_t get_memstore_allocated_pos() const { return arena_.allocated(); }
  int64_t get_retire_clock() const { return arena_.retired(); }
  ObMemstoreResourceKind get_resource_kind() const { return kind_; }
  void log_frozen_memstore_info(char* buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      FrozenMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse  */);
    }
  }
  void log_active_memstore_info(char *buf, int64_t limit) {
    if (NULL != buf && limit > 0) {
      ActiveMemstoreInfoLogger logger(buf, limit);
      buf[0] = 0;
      (void)for_each(logger, true /* reverse */);
    }
  }

private:
  int check_memory_limit_(const uint64_t tenant_id,
                          const int64_t alloc_size,
                          bool &is_out_of_mem) const;
  int64_t nway_per_group();
private:
  share::TxShareThrottleTool *throttle_tool_;
  Lock lock_;
  HandleList hlist_;
  Arena arena_;
  // Resource kind of this arena instance (DATA / MDS); set at init() and used to
  // throttle on the matching bucket.
  ObMemstoreResourceKind kind_;
};

inline ObMemstoreResourceKind ObMemstoreAllocator::AllocHandle::get_resource_kind() const
{
  return NULL == host_ ? ObMemstoreResourceKind::DATA : host_->get_resource_kind();
}

// The throttle framework keys buckets by allocator type. Keep DATA and MDS as
// explicit types so they own independent ObThrottleUnit instances. DATA uses
// the default memstore throttle policy from ObMemstoreAllocator, while MDS only
// overrides the policy that is different.
class ObDataMemstoreAllocator final : public ObMemstoreAllocator {};

class ObMdsMemstoreAllocator final : public ObMemstoreAllocator
{
public:
  static const lib::ObLabel throttle_unit_name()
  {
    static lib::ObLabel label("MdsMemstore");
    return label;
  }
  static void init_throttle_config(int64_t &resource_limit,
                                   int64_t &trigger_percentage,
                                   int64_t &max_duration);
};

inline lib::ObLabel ObMemstoreAllocator::get_label_by_kind(const ObMemstoreResourceKind kind)
{
  return (ObMemstoreResourceKind::MDS == kind)
      ? ObMdsMemstoreAllocator::throttle_unit_name()
      : lib::ObLabel(ObNewModIds::OB_MEMSTORE);
}

};     // namespace share
};     // namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_GMEMSTORE_ALLOCATOR_H_ */
