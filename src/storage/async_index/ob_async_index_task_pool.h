/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TASK_POOL_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TASK_POOL_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace storage
{

// Fixed-size, thread-safe object pool that also tracks which objects are
// currently in use, so callers can iterate the in-use set (e.g. for dedup).
//
// Why a free list (rather than a ring buffer or a bare counter)?
//   The intended usage is "borrow object -> hand to a FIFO work queue ->
//   return it on completion". A single consumer drains that queue in FIFO
//   order while acquire() hands out slots in LIFO order, so a completed object
//   is generally NOT at the slot a counter would free. The free list lets
//   release() return the EXACT slot the object occupies, in any order.
//
// Concurrency:
//   acquire()/release() are writers; in_use_count()/for_each_in_use() are
//   readers. All operations are serialized by a single spin lock. A spin lock
//   (not a read-write lock) is intentional: release() is always a writer and
//   the readers are effectively single-threaded, so read-read concurrency
//   would buy nothing.
//
// Requirements on T:
//   - default-constructible (the pool holds an inline array of T)
//   - has a reset() method (called under the lock on release, so concurrent
//     for_each_in_use() never observes a half-reset object)
template <typename T, int64_t CAPACITY>
class ObInUseObjectPool
{
public:
  explicit ObInUseObjectPool(const uint32_t latch_id)
    : free_count_(0), lock_(latch_id)
  {
  }
  ~ObInUseObjectPool() {}

  // Build the free list (all slots free). init_fn is invoked once per slot so
  // the owner can perform one-time per-object setup (e.g. stamp a back-pointer).
  template <typename InitFn>
  void init(InitFn &&init_fn)
  {
    for (int64_t i = 0; i < CAPACITY; ++i) {
      init_fn(objs_[i]);
      free_stack_[i] = static_cast<int32_t>(i);
      in_use_[i] = false;
    }
    free_count_ = static_cast<int32_t>(CAPACITY);
  }

  // Borrow an object from the pool. Returns nullptr if the pool is exhausted.
  T *acquire()
  {
    T *obj = nullptr;
    common::ObSpinLockGuard guard(lock_);
    if (free_count_ > 0) {
      const int32_t idx = free_stack_[--free_count_];
      in_use_[idx] = true;
      obj = &objs_[idx];
    }
    return obj;
  }

  // Return an object to the pool. The slot is derived from the object's
  // pointer, so objects may be returned in any order. Calls obj->reset()
  // under the lock. Double-free / foreign pointers are detected and skipped to
  // avoid corrupting the free list.
  void release(T *obj)
  {
    if (OB_NOT_NULL(obj)) {
      const int64_t idx = obj - objs_;
      if (OB_UNLIKELY(idx < 0 || idx >= CAPACITY)) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pointer out of pool range, skip return",
                      KP(obj), KP(objs_), K(idx));
      } else {
        common::ObSpinLockGuard guard(lock_);
        if (OB_UNLIKELY(!in_use_[idx])) {
          // Not borrowed: double-free or foreign pointer. Skip so we never hand
          // out the same slot twice.
          STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "returning a slot that is not in use, skip",
                        K(idx));
        } else {
          obj->reset();
          in_use_[idx] = false;
          free_stack_[free_count_++] = static_cast<int32_t>(idx);
        }
      }
    }
  }

  // Number of objects currently borrowed.
  int64_t in_use_count() const
  {
    common::ObSpinLockGuard guard(lock_);
    return CAPACITY - free_count_;
  }

  // Visit each in-use object under the lock. pred(const T &) returns true to
  // stop early; the return value is whether pred matched any in-use object.
  template <typename Pred>
  bool find_in_use(Pred &&pred) const
  {
    bool found = false;
    common::ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; !found && i < CAPACITY; ++i) {
      if (in_use_[i] && pred(objs_[i])) {
        found = true;
      }
    }
    return found;
  }

private:
  // objs_[i] is the i-th poolable object. free_stack_[0..free_count_-1] holds
  // the indices of currently-free slots; in_use_[i] marks slot i as borrowed.
  T objs_[CAPACITY];
  int32_t free_stack_[CAPACITY];
  bool in_use_[CAPACITY];
  int32_t free_count_;
  mutable common::ObSpinLock lock_;

  DISALLOW_COPY_AND_ASSIGN(ObInUseObjectPool);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_TASK_POOL_H_
