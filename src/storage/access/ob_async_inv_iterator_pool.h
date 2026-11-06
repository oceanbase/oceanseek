/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_OB_ASYNC_INV_ITERATOR_POOL_H_
#define OCEANBASE_STORAGE_OB_ASYNC_INV_ITERATOR_POOL_H_
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace blocksstable
{
class ObInvSSTablePostingIter;

enum class ObInvIteratorType
{
  POSTING
};

// Only explicitly adapted iterator types can be pooled.
template <typename Iter>
class ObInvIteratorTraits;

class ObInvIteratorEntryBase
{
public:
  typedef void (*DestroyFunc)(ObInvIteratorEntryBase *);
  const DestroyFunc destroy_func_;
  const ObInvIteratorType type_;
  ObInvIteratorEntryBase *next_global_entry_; // Null while leased.
  ObInvIteratorEntryBase *prev_global_entry_;
  int64_t accounted_bytes_; // Pool charge while leased or idle.
  bool is_idle_in_global_pool_; // Return claimed; linked only after scan cleanup completes.
protected:
  ObInvIteratorEntryBase(const ObInvIteratorType type, const int64_t bytes, DestroyFunc destroy_func)
    : destroy_func_(destroy_func), type_(type), next_global_entry_(nullptr), prev_global_entry_(nullptr),
      accounted_bytes_(bytes), is_idle_in_global_pool_(false)
  {}
  // Pool destruction must dispatch to the concrete Entry type.
  ~ObInvIteratorEntryBase() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObInvIteratorEntryBase);
};

template <typename Iter>
class ObInvIteratorEntry final : public ObInvIteratorEntryBase
{
public:
  typedef ObInvIteratorTraits<Iter> Traits;
  typedef typename Traits::ReuseKey ReuseKey;
  ObInvIteratorEntry()
    : ObInvIteratorEntryBase(Traits::TYPE, sizeof(ObInvIteratorEntry), destroy_entry),
      iter_(), reuse_key_()
  {}
  void reuse()
  {
    // Detach query resources before publication; the caller then resets its allocator.
    iter_.reset();
  }
  bool matches(const ReuseKey &key) const { return Traits::matches(reuse_key_, key); }
  int64_t get_cached_mem_size() const { return sizeof(ObInvIteratorEntry); }
  Iter iter_;
  ReuseKey reuse_key_;
  TO_STRING_KV(K_(iter));
private:
  static void destroy_entry(ObInvIteratorEntryBase *base)
  {
    ObInvIteratorEntry *entry = static_cast<ObInvIteratorEntry *>(base);
    entry->~ObInvIteratorEntry();
    common::ob_free(entry);
  }
  DISALLOW_COPY_AND_ASSIGN(ObInvIteratorEntry);
};

// Posting-specific compatibility for the generic Entry.
template <>
class ObInvIteratorTraits<ObInvSSTablePostingIter>
{
public:
  static constexpr ObInvIteratorType TYPE = ObInvIteratorType::POSTING;
  // reset() discards all rowkey-dependent scan state.
  struct ReuseKey {};
  static bool matches(const ReuseKey &, const ReuseKey &) { return true; }
};

typedef ObInvIteratorEntry<ObInvSSTablePostingIter> ObInvPostingIterEntry;

} // namespace blocksstable


namespace storage
{
// Query bindings and leases keep the pool alive until their cleanup completes.
class ObAsyncInvIteratorPool final
{
public:
  ObAsyncInvIteratorPool();
  ~ObAsyncInvIteratorPool();
  static int mtl_init(ObAsyncInvIteratorPool *&pool);
  static void mtl_destroy(ObAsyncInvIteratorPool *&pool);
  int init();
  void stop();
  void wash();
  bool is_valid() const { return is_inited_; }
  // The caller must protect the initial lookup with the tenant lifecycle.
  // Pair each successful pin() with unpin(), which may destroy the pool.
  bool pin();
  void unpin();
  template <typename Iter>
  int get_inv_entry(const typename blocksstable::ObInvIteratorTraits<Iter>::ReuseKey &key,
      blocksstable::ObInvIteratorEntry<Iter> *&entry);
  template <typename Iter>
  void release_inv_entry(blocksstable::ObInvIteratorEntry<Iter> *&entry);
private:
  bool is_disabled() const { return ATOMIC_LOAD(&is_disabled_); }
  void drain_idle_inv_entries();
  void shrink_idle_inv_entries();
  // Caller holds inv_lock_ and has verified that the idle list is nonempty.
  void evict_idle_tail(blocksstable::ObInvIteratorEntryBase *&evicted);
  static void destroy_inv_entry(blocksstable::ObInvIteratorEntryBase *entry);
  bool is_inited_;
  bool stopped_;
  bool is_disabled_;
  bool is_washing_;
  int64_t refs_; // MTL ownership, query bindings, leases and in-flight operations.
  uint64_t tenant_id_;
  static const int64_t INV_SHRINK_INTERVAL = 60L * 1000 * 1000;
  static const int64_t INV_SHRINK_RETAIN_BYTES = 16L << 20;
  static const int64_t WASH_HIGH_THRESHOLD = 96;
  static const int64_t WASH_LOW_THRESHOLD = 90;
  common::ObSpinLock inv_lock_;
  blocksstable::ObInvIteratorEntryBase *inv_idle_head_; // Most recently returned first.
  blocksstable::ObInvIteratorEntryBase *inv_idle_tail_; // Least recently returned.
  int64_t inv_idle_count_;
  int64_t inv_idle_bytes_;
  int64_t inv_total_bytes_; // Idle retained memory plus leased Entry charges.
  int64_t inv_idle_bytes_low_water_; // Window minimum in bytes; protected by inv_lock_.
  int64_t inv_shrink_window_start_; // Monotonic time; zero means no window yet.
  DISALLOW_COPY_AND_ASSIGN(ObAsyncInvIteratorPool);
};
}
}
#endif
