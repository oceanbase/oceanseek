/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
namespace oceanbase
{
namespace storage
{
ObAsyncInvIteratorPool::ObAsyncInvIteratorPool()
  : is_inited_(false), stopped_(false), is_disabled_(false), is_washing_(false),
    refs_(1), tenant_id_(MTL_ID()), shard_count_(INV_SHARD_COUNT), shards_()
{}
ObAsyncInvIteratorPool::~ObAsyncInvIteratorPool()
{
  drain_idle_inv_entries();
}
int ObAsyncInvIteratorPool::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}
int ObAsyncInvIteratorPool::mtl_init(ObAsyncInvIteratorPool *&pool)
{
  int ret = OB_SUCCESS;
  if (nullptr == pool) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_not_virtual_tenant_id(MTL_ID()) && !is_meta_tenant(MTL_ID())) {
    ret = pool->init();
  }
  return ret;
}
bool ObAsyncInvIteratorPool::pin()
{
  // The caller's tenant guard or existing reference protects this initial increment.
  ATOMIC_INC(&refs_);
  const bool accepted = is_inited_ && !ATOMIC_LOAD(&stopped_);
  if (!accepted) {
    unpin();
  }
  return accepted;
}
void ObAsyncInvIteratorPool::unpin()
{
  if (0 == ATOMIC_SAF(&refs_, 1)) {
    ObAsyncInvIteratorPool *pool = this;
    common::ob_delete(pool);
  }
}
void ObAsyncInvIteratorPool::stop()
{
  ATOMIC_STORE(&stopped_, true);
  // Locking each shard during drain also synchronizes with in-flight publishers.
  drain_idle_inv_entries();
}
void ObAsyncInvIteratorPool::mtl_destroy(ObAsyncInvIteratorPool *&pool)
{
  if (nullptr != pool) {
    ObAsyncInvIteratorPool *released = pool;
    pool = nullptr;
    released->stop();
    {
      const int64_t refs = ATOMIC_LOAD(&released->refs_);
      if (refs > 1) {
        LOG_WARN_RET(OB_EAGAIN, "inverted iterator pool still has leases, query bindings or operations "
            "during MTL teardown",
            K(refs));
      }
    }
    // Late returns keep the management object alive without blocking MTL teardown.
    released->unpin();
  }
}
void ObAsyncInvIteratorPool::wash()
{
  if (pin()) {
    if (false == ATOMIC_VCAS(&is_washing_, false, true)) {
      const int64_t limit = lib::get_tenant_memory_limit(tenant_id_);
      const int64_t hold = lib::get_tenant_memory_hold(tenant_id_);
      if (hold * 100 > limit * WASH_HIGH_THRESHOLD) {
        ATOMIC_STORE(&is_disabled_, true);
        drain_idle_inv_entries();
      } else {
        if (hold * 100 < limit * WASH_LOW_THRESHOLD) {
          ATOMIC_STORE(&is_disabled_, false);
        }
        shrink_idle_inv_entries();
      }
      ATOMIC_STORE(&is_washing_, false);
    }
    unpin();
  }
}
void ObAsyncInvIteratorPool::destroy_inv_entry(blocksstable::ObInvIteratorEntryBase *entry)
{
  if (nullptr != entry) {
    entry->destroy_func_(entry);
  }
}

void ObAsyncInvIteratorPool::drain_idle_inv_entries()
{
  for (int64_t id = 0; id < shard_count_; ++id) {
    InvShard &shard = shards_[id];

    blocksstable::ObInvIteratorEntryBase *entry = nullptr;
    {
      common::ObSpinLockGuard guard(shard.inv_lock_);
      entry = shard.inv_idle_head_;
      shard.inv_idle_head_ = nullptr;
      shard.inv_idle_tail_ = nullptr;
      shard.inv_idle_bytes_low_water_ = 0;
      shard.inv_shrink_window_start_ = 0;
      shard.inv_total_bytes_ -= shard.inv_idle_bytes_;
      shard.inv_idle_bytes_ = 0;
    }
    while (nullptr != entry) {
      blocksstable::ObInvIteratorEntryBase *next = entry->next_global_entry_;
      destroy_inv_entry(entry);
      entry = next;
    }
  }
}

void ObAsyncInvIteratorPool::evict_idle_tail(InvShard &shard, blocksstable::ObInvIteratorEntryBase *&evicted)
{
  blocksstable::ObInvIteratorEntryBase *victim = shard.inv_idle_tail_;
  shard.inv_idle_tail_ = victim->prev_global_entry_;
  if (nullptr != shard.inv_idle_tail_) {
    shard.inv_idle_tail_->next_global_entry_ = nullptr;
  } else {
    shard.inv_idle_head_ = nullptr;
  }
  shard.inv_idle_bytes_ -= victim->accounted_bytes_;
  shard.inv_total_bytes_ -= victim->accounted_bytes_;
  shard.inv_idle_bytes_low_water_ = MIN(shard.inv_idle_bytes_low_water_, shard.inv_idle_bytes_);
  victim->prev_global_entry_ = nullptr;
  victim->next_global_entry_ = evicted;
  evicted = victim;
}

int64_t ObAsyncInvIteratorPool::active_shard_count(const int64_t memory_limit) const
{
  // Avoid fragmenting a small budget below the size of the supported posting entry.
  return memory_limit < shard_count_ * static_cast<int64_t>(sizeof(blocksstable::ObInvPostingIterEntry))
      ? 1 : shard_count_;
}

void ObAsyncInvIteratorPool::shrink_idle_inv_entries()
{
  blocksstable::ObInvIteratorEntryBase *evicted = nullptr;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    const int64_t memory_limit = tenant_config->async_inverted_index_iter_cache_max_size;
    if (memory_limit <= 0) {
      drain_idle_inv_entries();
    } else {
      const int64_t retain_bytes = memory_limit >= INV_SHRINK_RETAIN_BYTES
          ? INV_SHRINK_RETAIN_BYTES : memory_limit / 2;
      const int64_t active_count = active_shard_count(memory_limit);
      for (int64_t id = 0; id < shard_count_; ++id) {
        InvShard &shard = shards_[id];
        const int64_t budget = shard_budget(memory_limit, id, active_count);
        const int64_t retain = shard_budget(retain_bytes, id, active_count);
        common::ObSpinLockGuard guard(shard.inv_lock_);
        // A reduced budget takes precedence over the periodic retention floor.
        while (shard.inv_total_bytes_ > budget && nullptr != shard.inv_idle_tail_) {
          evict_idle_tail(shard, evicted);
        }
        const int64_t now = common::ObTimeUtility::current_monotonic_time();
        if (0 == shard.inv_shrink_window_start_ || now < shard.inv_shrink_window_start_) {
          shard.inv_shrink_window_start_ = now;
          shard.inv_idle_bytes_low_water_ = shard.inv_idle_bytes_;
        } else if (now - shard.inv_shrink_window_start_ >= INV_SHRINK_INTERVAL) {
          const int64_t release_bytes = shard.inv_idle_bytes_low_water_ / 2;
          int64_t released_bytes = 0;
          // Reclaim whole entries without crossing the retained-byte floor.
          while (released_bytes < release_bytes && nullptr != shard.inv_idle_tail_
              && shard.inv_idle_tail_->accounted_bytes_ <= shard.inv_idle_bytes_ - retain) {
            released_bytes += shard.inv_idle_tail_->accounted_bytes_;
            evict_idle_tail(shard, evicted);
          }
          // A delayed wash closes one window only, without repeated catch-up shrinking.
          shard.inv_idle_bytes_low_water_ = shard.inv_idle_bytes_;
          shard.inv_shrink_window_start_ = now;
        }
      }
    }
  }
  while (nullptr != evicted) {
    blocksstable::ObInvIteratorEntryBase *next = evicted->next_global_entry_;
    destroy_inv_entry(evicted);
    evicted = next;
  }
}

template <typename Iter>
int ObAsyncInvIteratorPool::get_inv_entry(
    const typename blocksstable::ObInvIteratorTraits<Iter>::ReuseKey &key,
    blocksstable::ObInvIteratorEntry<Iter> *&entry)
{
  typedef blocksstable::ObInvIteratorEntry<Iter> Entry;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != entry || tenant_id_ != MTL_ID())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid global inverted iterator lease request", K(ret), KP(entry), K(tenant_id_), K(MTL_ID()));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!pin()) {
    // Stopping pools fall back to operator-owned allocation.
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    const int64_t memory_limit = tenant_config.is_valid()
        ? static_cast<int64_t>(tenant_config->async_inverted_index_iter_cache_max_size) : 0;
    if (memory_limit <= 0) {
      unpin();
      return ret;
    }
    const int64_t itid = common::get_itid();
    if (OB_UNLIKELY(itid < 0)) {
      unpin();
      return ret;
    }
    const int64_t shard_id = itid % active_shard_count(memory_limit);
    InvShard &shard = shards_[shard_id];
    {
      common::ObSpinLockGuard guard(shard.inv_lock_);
      if (!ATOMIC_LOAD(&stopped_) && !is_disabled()) {
        blocksstable::ObInvIteratorEntryBase *candidate = shard.inv_idle_head_;
        while (nullptr != candidate
            && (blocksstable::ObInvIteratorTraits<Iter>::TYPE != candidate->type_
                || !static_cast<Entry *>(candidate)->matches(key))) {
          candidate = candidate->next_global_entry_;
        }
        entry = static_cast<Entry *>(candidate);
        if (nullptr != entry) {
          if (nullptr != entry->prev_global_entry_) {
            entry->prev_global_entry_->next_global_entry_ = entry->next_global_entry_;
          } else {
            shard.inv_idle_head_ = entry->next_global_entry_;
          }
          if (nullptr != entry->next_global_entry_) {
            entry->next_global_entry_->prev_global_entry_ = entry->prev_global_entry_;
          } else {
            shard.inv_idle_tail_ = entry->prev_global_entry_;
          }
          entry->next_global_entry_ = nullptr;
          entry->prev_global_entry_ = nullptr;
          ATOMIC_STORE(&entry->is_idle_in_global_pool_, false);
          shard.inv_idle_bytes_ -= entry->accounted_bytes_;
          shard.inv_idle_bytes_low_water_ = MIN(shard.inv_idle_bytes_low_water_, shard.inv_idle_bytes_);
        }
      }
    }
    bool reject = false;
    if (nullptr == entry) {
      void *buf = common::ob_malloc(sizeof(Entry),
          SET_IGNORE_MEM_VERSION(common::ObMemAttr(tenant_id_, "GlobalInvIter")));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate global inverted iterator entry", K(ret));
      } else {
        entry = new (buf) Entry();
        entry->shard_id_ = shard_id;
        {
          common::ObSpinLockGuard guard(shard.inv_lock_);
          reject = ATOMIC_LOAD(&stopped_);
          if (!reject) {
            shard.inv_total_bytes_ += entry->accounted_bytes_;
          }
        }
      }
    }
    if (reject) {
      destroy_inv_entry(entry);
      entry = nullptr;
    }
    if (OB_SUCC(ret) && nullptr != entry) {
      entry->reuse_key_ = key;
    } else {
      unpin();
    }
  }
  return ret;
}

template <typename Iter>
void ObAsyncInvIteratorPool::release_inv_entry(blocksstable::ObInvIteratorEntry<Iter> *&entry)
{
  if (nullptr != entry) {
    blocksstable::ObInvIteratorEntry<Iter> *released = entry;
    blocksstable::ObInvIteratorEntryBase *evicted = nullptr;
    int64_t memory_limit = 0;
    if (!ATOMIC_LOAD(&stopped_)) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (tenant_config.is_valid()) {
        memory_limit = tenant_config->async_inverted_index_iter_cache_max_size;
      }
    }
    InvShard &shard = shards_[released->shard_id_];
    const int64_t budget = shard_budget(memory_limit, released->shard_id_, active_shard_count(memory_limit));
    const bool pool_enabled = memory_limit > 0;
    const bool valid = ATOMIC_BCAS(&released->is_idle_in_global_pool_, false, true);
    if (!valid) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED,
          "returning an already returned inverted iterator entry", KP(released));
    } else {
      // No query-owned handle or datum-utils pointer may enter the tenant-wide free list.
      released->reuse();
      const int64_t entry_bytes = released->get_cached_mem_size();
      bool retain = false;
      {
        common::ObSpinLockGuard guard(shard.inv_lock_);
        retain = pool_enabled && !ATOMIC_LOAD(&stopped_) && !is_disabled() && entry_bytes <= budget;
        if (retain) {
          shard.inv_total_bytes_ += entry_bytes - released->accounted_bytes_;
          released->accounted_bytes_ = entry_bytes;
          released->next_global_entry_ = shard.inv_idle_head_;
          released->prev_global_entry_ = nullptr;
          if (nullptr != shard.inv_idle_head_) {
            shard.inv_idle_head_->prev_global_entry_ = released;
          } else {
            shard.inv_idle_tail_ = released;
          }
          shard.inv_idle_head_ = released;
          shard.inv_idle_bytes_ += entry_bytes;
          // Evict older idle entries first; leased entries are never reclaimed.
          while (shard.inv_total_bytes_ > budget && nullptr != shard.inv_idle_tail_) {
            evict_idle_tail(shard, evicted);
          }
        } else {
          shard.inv_total_bytes_ -= released->accounted_bytes_;
        }
      }
      if (!retain) {
        destroy_inv_entry(released);
      }
      while (nullptr != evicted) {
        blocksstable::ObInvIteratorEntryBase *next = evicted->next_global_entry_;
        destroy_inv_entry(evicted);
        evicted = next;
      }
    }
    entry = nullptr;
    if (valid) {
      unpin();
    }
  }
}

// Instantiate supported types here to keep pool implementation out of the header.
template int ObAsyncInvIteratorPool::get_inv_entry<blocksstable::ObInvSSTablePostingIter>(
    const blocksstable::ObInvPostingIterEntry::ReuseKey &, blocksstable::ObInvPostingIterEntry *&);
template void ObAsyncInvIteratorPool::release_inv_entry<blocksstable::ObInvSSTablePostingIter>(
    blocksstable::ObInvPostingIterEntry *&);


}
}
