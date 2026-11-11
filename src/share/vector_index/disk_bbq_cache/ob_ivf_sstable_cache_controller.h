/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_IVF_SSTABLE_CACHE_CONTROLLER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_IVF_SSTABLE_CACHE_CONTROLLER_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

// Tenant-level accounting and admission controller for Disk-BBQ SSTable
// centroid caches. Cache contents are reconstructible and this state is not
// persisted. The scheduler refreshes the tenant vector memory limit
// periodically; foreground admission also refreshes it before reserving memory.
class ObIvfSSTableCacheController
{
public:
  ObIvfSSTableCacheController();
  ~ObIvfSSTableCacheController() = default;

  int init(uint64_t tenant_id, uint64_t *all_vector_used);
  void reset();
  int refresh_config();
  bool is_cache_enabled() const;

  bool try_reserve(uint64_t bytes);
  void release_reservation(uint64_t bytes);
  // Charge allocated cache buffers until their memory is reclaimed, even when
  // detached from lookup maps or retained privately after a failed publication.
  void on_cache_allocated(uint64_t bytes);
  void on_cache_released(uint64_t bytes);
  // Convert reserved budget and update actual hold in one admission decision.
  void update_cache_hold(uint64_t old_bytes, uint64_t new_bytes, uint64_t released_reservation = 0);

  bool try_start_eviction();
  void finish_eviction();

  uint64_t get_limit() const { return ATOMIC_LOAD(&mem_limit_); }
  uint64_t get_high_watermark() const { return ATOMIC_LOAD(&mem_high_); }
  uint64_t get_low_watermark() const { return ATOMIC_LOAD(&mem_low_); }
  uint64_t get_resident_hold() const { return ATOMIC_LOAD(&resident_hold_); }
  uint64_t get_reserved_hold() const { return ATOMIC_LOAD(&build_reserved_); }
  bool is_admission_closed() const { return ATOMIC_LOAD(&admission_closed_); }
  bool has_eviction_pending() const { return ATOMIC_LOAD(&eviction_pending_); }
  bool is_eviction_running() const { return ATOMIC_LOAD(&eviction_running_); }

  uint64_t get_eviction_count() const { return ATOMIC_LOAD(&eviction_count_); }
  uint64_t get_evicted_entry_count() const { return ATOMIC_LOAD(&evicted_entry_count_); }
  uint64_t get_evicted_bytes() const { return ATOMIC_LOAD(&evicted_bytes_); }
  void record_eviction(uint64_t entry_count, uint64_t bytes);

  void record_cache_access(bool hit)
  {
    if (hit) {
      ATOMIC_INC(&hit_count_);
    } else {
      ATOMIC_INC(&miss_count_);
    }
  }
  uint64_t get_hit_count() const { return ATOMIC_LOAD(&hit_count_); }
  uint64_t get_miss_count() const { return ATOMIC_LOAD(&miss_count_); }

  TO_STRING_KV(K_(tenant_id), "limit", get_limit(), "high", get_high_watermark(),
      "low", get_low_watermark(), "resident", get_resident_hold(),
      "reserved", get_reserved_hold(), "admission_closed", is_admission_closed(),
      "eviction_pending", has_eviction_pending(), "eviction_running", is_eviction_running(),
      "eviction_count", get_eviction_count(), "evicted_entry_count", get_evicted_entry_count(),
      "evicted_bytes", get_evicted_bytes(),
      "hit_count", get_hit_count(), "miss_count", get_miss_count());

private:
  // Cache quota is a fraction of the tenant vector memory limit; watermarks
  // are fractions of that cache quota.
  static const int64_t HIGH_WATERMARK_PERCENT = 90;
  static const int64_t LOW_WATERMARK_PERCENT = 50;

  static uint64_t atomic_saturating_sub_(uint64_t *value, uint64_t delta);
  void update_admission_state_();
  void update_admission_state_locked_();

private:
  common::ObSpinLock state_lock_;
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t *all_vector_used_;
  uint64_t vector_mem_limit_;
  uint64_t mem_limit_;
  uint64_t mem_high_;
  uint64_t mem_low_;
  uint64_t resident_hold_;
  uint64_t build_reserved_;
  bool admission_closed_;
  bool eviction_pending_;
  bool eviction_running_;
  uint64_t eviction_count_;
  uint64_t evicted_entry_count_;
  uint64_t evicted_bytes_;
  uint64_t hit_count_;
  uint64_t miss_count_;

  DISALLOW_COPY_AND_ASSIGN(ObIvfSSTableCacheController);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_IVF_SSTABLE_CACHE_CONTROLLER_H_
