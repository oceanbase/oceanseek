/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/disk_bbq_cache/ob_ivf_sstable_cache_controller.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"

namespace oceanbase
{
namespace share
{

ObIvfSSTableCacheController::ObIvfSSTableCacheController()
  : state_lock_(common::ObLatchIds::OB_PLUGIN_VECTOR_INDEX_SERVICE_LOCK),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    all_vector_used_(nullptr),
    vector_mem_limit_(0),
    mem_limit_(0),
    mem_high_(0),
    mem_low_(0),
    resident_hold_(0),
    build_reserved_(0),
    admission_closed_(true),
    eviction_pending_(false),
    eviction_running_(false),
    eviction_count_(0),
    evicted_entry_count_(0),
    evicted_bytes_(0),
    hit_count_(0),
    miss_count_(0)
{
}

int ObIvfSSTableCacheController::init(const uint64_t tenant_id, uint64_t *all_vector_used)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF SSTable cache controller is initialized twice",
             KR(ret), K(tenant_id), K_(tenant_id));
  } else if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(all_vector_used)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable cache controller argument", KR(ret), K(tenant_id), KP(all_vector_used));
  } else {
    tenant_id_ = tenant_id;
    all_vector_used_ = all_vector_used;
    is_inited_ = true;
    const int config_ret = refresh_config();
    if (OB_SUCCESS != config_ret) {
      // This cache is optional. Keep admission closed and let foreground or
      // periodic maintenance retry after tenant configuration becomes ready.
      LOG_WARN("failed to initialize IVF SSTable cache limit, cache stays disabled",
               K(config_ret), K(tenant_id));
    }
  }
  return ret;
}

void ObIvfSSTableCacheController::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  all_vector_used_ = nullptr;
  ATOMIC_STORE(&vector_mem_limit_, 0);
  ATOMIC_STORE(&mem_limit_, 0);
  ATOMIC_STORE(&mem_high_, 0);
  ATOMIC_STORE(&mem_low_, 0);
  ATOMIC_STORE(&resident_hold_, 0);
  ATOMIC_STORE(&build_reserved_, 0);
  ATOMIC_STORE(&admission_closed_, true);
  ATOMIC_STORE(&eviction_pending_, false);
  ATOMIC_STORE(&eviction_running_, false);
  ATOMIC_STORE(&eviction_count_, 0);
  ATOMIC_STORE(&evicted_entry_count_, 0);
  ATOMIC_STORE(&evicted_bytes_, 0);
  ATOMIC_STORE(&hit_count_, 0);
  ATOMIC_STORE(&miss_count_, 0);
}

bool ObIvfSSTableCacheController::is_cache_enabled() const
{
  omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
  return is_inited_ && config.is_valid()
      && static_cast<int64_t>(config->_disk_bbq_centroid_cache_mem_percentage) > 0;
}

int ObIvfSSTableCacheController::refresh_config()
{
  int ret = OB_SUCCESS;
  int64_t vector_limit = 0;
  omt::ObTenantConfigGuard config(TENANT_CONF(tenant_id_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF SSTable cache controller is not initialized", KR(ret));
  } else if (!config.is_valid()) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("tenant configuration is unavailable for IVF SSTable cache", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(
                 tenant_id_, vector_limit))) {
    LOG_WARN("failed to get tenant vector memory limit", KR(ret), K_(tenant_id));
  } else {
    const uint64_t limit = vector_limit <= 0 ? 0
        : static_cast<uint64_t>(vector_limit)
            * static_cast<int64_t>(config->_disk_bbq_centroid_cache_mem_percentage) / 100;
    const uint64_t high = limit * HIGH_WATERMARK_PERCENT / 100;
    const uint64_t low = limit * LOW_WATERMARK_PERCENT / 100;
    common::ObSpinLockGuard guard(state_lock_);
    ATOMIC_STORE(&vector_mem_limit_,
                 vector_limit > 0 ? static_cast<uint64_t>(vector_limit) : 0);
    ATOMIC_STORE(&mem_limit_, limit);
    ATOMIC_STORE(&mem_high_, high);
    ATOMIC_STORE(&mem_low_, low);
    update_admission_state_locked_();
  }
  return ret;
}

void ObIvfSSTableCacheController::update_admission_state_()
{
  common::ObSpinLockGuard guard(state_lock_);
  update_admission_state_locked_();
}

void ObIvfSSTableCacheController::update_admission_state_locked_()
{
  const uint64_t limit = get_limit();
  const uint64_t high = get_high_watermark();
  const uint64_t low = get_low_watermark();
  const uint64_t resident = get_resident_hold();
  const uint64_t reserved = get_reserved_hold();
  const bool running = is_eviction_running();
  bool pending = has_eviction_pending();

  if (0 == limit) {
    pending = resident > 0;
  } else if (resident >= high) {
    pending = true;
  } else if (!running && pending && resident <= low) {
    // Keep the high/low hysteresis once eviction has started or been marked.
    pending = false;
  }
  ATOMIC_STORE(&eviction_pending_, pending);

  const bool reservation_reaches_high = 0 == high
      || resident >= high
      || reserved >= high - resident;
  ATOMIC_STORE(&admission_closed_,
      0 == limit || running || pending || reservation_reaches_high);
}

bool ObIvfSSTableCacheController::try_reserve(const uint64_t bytes)
{
  bool reserved = false;
  if (is_inited_ && bytes > 0 && OB_SUCCESS == refresh_config()
      && !is_admission_closed()) {
    common::ObSpinLockGuard guard(state_lock_);
    uint64_t old_reserved = ATOMIC_LOAD(&build_reserved_);
    while (!reserved) {
      if (is_admission_closed()) {
        break;
      }
      const uint64_t resident = get_resident_hold();
      const uint64_t limit = get_limit();
      const uint64_t vector_limit = ATOMIC_LOAD(&vector_mem_limit_);
      const uint64_t vector_used = OB_ISNULL(all_vector_used_) ? UINT64_MAX
          : ATOMIC_LOAD(all_vector_used_);
      if (bytes > limit || resident > limit - bytes
          || old_reserved > limit - bytes - resident
          || bytes > vector_limit || vector_used > vector_limit - bytes) {
        break;
      }
      reserved = ATOMIC_BCAS(&build_reserved_, old_reserved, old_reserved + bytes);
      if (!reserved) {
        old_reserved = ATOMIC_LOAD(&build_reserved_);
      }
    }
    if (reserved) {
      update_admission_state_locked_();
    }
  }
  return reserved;
}

uint64_t ObIvfSSTableCacheController::atomic_saturating_sub_(
    uint64_t *value, const uint64_t delta)
{
  uint64_t old_value = ATOMIC_LOAD(value);
  uint64_t new_value = old_value > delta ? old_value - delta : 0;
  while (!ATOMIC_BCAS(value, old_value, new_value)) {
    old_value = ATOMIC_LOAD(value);
    new_value = old_value > delta ? old_value - delta : 0;
  }
  return old_value;
}

void ObIvfSSTableCacheController::release_reservation(const uint64_t bytes)
{
  update_cache_hold(0, 0, bytes);
}

void ObIvfSSTableCacheController::on_cache_allocated(const uint64_t bytes)
{
  update_cache_hold(0, bytes);
}

void ObIvfSSTableCacheController::on_cache_released(const uint64_t bytes)
{
  update_cache_hold(bytes, 0);
}

void ObIvfSSTableCacheController::update_cache_hold(
    const uint64_t old_bytes, const uint64_t new_bytes, const uint64_t released_reservation)
{
  common::ObSpinLockGuard guard(state_lock_);
  // Admission uses the same lock, so no other builder can consume the budget
  // between releasing the reservation and replacing it with actual ownership.
  if (released_reservation > 0) {
    const uint64_t old_reserved = atomic_saturating_sub_(&build_reserved_, released_reservation);
    if (OB_UNLIKELY(released_reservation > old_reserved)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED,
                    "IVF SSTable cache reservation over-release, clamp to zero",
                    K(released_reservation), K(old_reserved), K_(tenant_id));
    }
  }
  if (new_bytes > old_bytes) {
    (void)ATOMIC_AAF(&resident_hold_, new_bytes - old_bytes);
  } else if (old_bytes > new_bytes) {
    const uint64_t released = old_bytes - new_bytes;
    const uint64_t old_resident = atomic_saturating_sub_(&resident_hold_, released);
    if (OB_UNLIKELY(released > old_resident)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED,
                    "IVF SSTable cache resident over-release, clamp to zero",
                    K(released), K(old_resident), K_(tenant_id));
    }
  }
  update_admission_state_locked_();
}

bool ObIvfSSTableCacheController::try_start_eviction()
{
  return has_eviction_pending()
      && ATOMIC_BCAS(&eviction_running_, false, true);
}

void ObIvfSSTableCacheController::finish_eviction()
{
  ATOMIC_STORE(&eviction_running_, false);
  update_admission_state_();
}

void ObIvfSSTableCacheController::record_eviction(
    const uint64_t entry_count, const uint64_t bytes)
{
  (void)ATOMIC_AAF(&eviction_count_, 1);
  (void)ATOMIC_AAF(&evicted_entry_count_, entry_count);
  (void)ATOMIC_AAF(&evicted_bytes_, bytes);
}

} // namespace share
} // namespace oceanbase
