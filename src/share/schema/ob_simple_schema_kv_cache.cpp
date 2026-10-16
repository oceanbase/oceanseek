/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_simple_schema_kv_cache.h"

#include <sched.h>
#include "lib/charset/ob_charset.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_sort.h"
#include "share/cache/ob_cache_name_define.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_resolve_util.h"
#include "share/schema/ob_tablegroup_table_id_cache.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

const int64_t ObSchemaCacheIncrementalRefreshPolicy::MAX_OPERATION_COUNT;
const int64_t ObSchemaCacheIncrementalRefreshPolicy::MAX_DDL_BOUNDARY_COUNT;
const int64_t ObSchemaCacheIncrementalRefreshPolicy::MAX_TARGETED_INVALIDATION_OPERATION_COUNT;
const int64_t ObSchemaCacheIncrementalRefreshPolicy::MAX_TARGETED_INVALIDATION_KEY_COUNT;
const int64_t ObOnDemandSchemaRetryPolicy::DEFAULT_LOAD_TIMEOUT_US;
const int64_t ObOnDemandSchemaRetryPolicy::MAX_STALE_RETRY_COUNT;
const int64_t ObOnDemandSchemaRetryPolicy::STALE_RETRY_BASE_US;
ERRSIM_POINT_DEF(ERRSIM_ON_DEMAND_LOAD_WAIT_TIMEOUT);

namespace
{
void record_schema_resolve_cache_miss(const ObSchemaIDResolveKey &)
{
  EVENT_INC(ObStatEventIds::SCHEMA_ID_RESOLVE_CACHE_MISS);
}

void record_schema_resolve_cache_miss(const ObSchemaNameResolveKey &)
{
  EVENT_INC(ObStatEventIds::SCHEMA_NAME_RESOLVE_CACHE_MISS);
}

void record_schema_resolve_cache_hit(const ObSchemaIDResolveKey &)
{
  EVENT_INC(ObStatEventIds::SCHEMA_ID_RESOLVE_CACHE_HIT);
}

void record_schema_resolve_cache_hit(const ObSchemaNameResolveKey &)
{
  EVENT_INC(ObStatEventIds::SCHEMA_NAME_RESOLVE_CACHE_HIT);
}
}

ObSchemaIDResolveKey::ObSchemaIDResolveKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cache_generation_(0),
    schema_id_(OB_INVALID_ID)
{
}

ObSchemaIDResolveKey::ObSchemaIDResolveKey(
    const uint64_t tenant_id,
    const uint64_t cache_generation,
    const uint64_t schema_id)
  : tenant_id_(tenant_id),
    cache_generation_(cache_generation),
    schema_id_(schema_id)
{
}

bool ObSchemaIDResolveKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != tenant_id_
      && cache_generation_ > 0
      && OB_INVALID_ID != schema_id_;
}

bool ObSchemaIDResolveKey::operator==(const ObIKVCacheKey &other) const
{
  const ObSchemaIDResolveKey &other_key =
      reinterpret_cast<const ObSchemaIDResolveKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && cache_generation_ == other_key.cache_generation_
      && schema_id_ == other_key.schema_id_;
}

uint64_t ObSchemaIDResolveKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&cache_generation_, sizeof(cache_generation_), hash_value);
  hash_value = murmurhash(&schema_id_, sizeof(schema_id_), hash_value);
  return hash_value;
}

int ObSchemaIDResolveKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  key = NULL;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id resolve key buffer", KR(ret), K(buf_len), K(size()));
  } else {
    key = new (buf) ObSchemaIDResolveKey(*this);
  }
  return ret;
}

ObSchemaNameResolveKey::ObSchemaNameResolveKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cache_generation_(0),
    database_id_(OB_INVALID_ID),
    exact_session_id_(0),
    table_namespace_(NS_MAX),
    normalized_name_()
{
}

ObSchemaNameResolveKey::ObSchemaNameResolveKey(
    const uint64_t tenant_id,
    const uint64_t cache_generation,
    const uint64_t database_id,
    const uint64_t exact_session_id,
    const ObTableNameNamespace table_namespace,
    const ObString &normalized_name)
  : tenant_id_(tenant_id),
    cache_generation_(cache_generation),
    database_id_(database_id),
    exact_session_id_(exact_session_id),
    table_namespace_(table_namespace),
    normalized_name_(normalized_name)
{
}

bool ObSchemaNameResolveKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != tenant_id_
      && cache_generation_ > 0
      && OB_INVALID_ID != database_id_
      && table_namespace_ < NS_MAX
      && !normalized_name_.empty()
      && normalized_name_.length() <= OB_MAX_TABLE_NAME_LENGTH;
}

bool ObSchemaNameResolveKey::operator==(const ObIKVCacheKey &other) const
{
  const ObSchemaNameResolveKey &other_key =
      reinterpret_cast<const ObSchemaNameResolveKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && cache_generation_ == other_key.cache_generation_
      && database_id_ == other_key.database_id_
      && exact_session_id_ == other_key.exact_session_id_
      && table_namespace_ == other_key.table_namespace_
      && normalized_name_ == other_key.normalized_name_;
}

uint64_t ObSchemaNameResolveKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&cache_generation_, sizeof(cache_generation_), hash_value);
  hash_value = murmurhash(&database_id_, sizeof(database_id_), hash_value);
  hash_value = murmurhash(&exact_session_id_, sizeof(exact_session_id_), hash_value);
  hash_value = murmurhash(&table_namespace_, sizeof(table_namespace_), hash_value);
  hash_value = murmurhash(normalized_name_.ptr(), normalized_name_.length(), hash_value);
  return hash_value;
}

int64_t ObSchemaNameResolveKey::size() const
{
  return sizeof(*this) + normalized_name_.length();
}

int ObSchemaNameResolveKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  key = NULL;
  if (OB_ISNULL(buf) || buf_len < size() || !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name resolve key buffer", KR(ret), K(buf_len), K(size()), KPC(this));
  } else {
    ObSchemaNameResolveKey *new_key = new (buf) ObSchemaNameResolveKey(*this);
    char *name_buf = buf + sizeof(*this);
    MEMCPY(name_buf, normalized_name_.ptr(), normalized_name_.length());
    new_key->normalized_name_.assign_ptr(name_buf, normalized_name_.length());
    key = new_key;
  }
  return ret;
}

ObSchemaResolveRange::ObSchemaResolveRange()
  : start_version_(OB_INVALID_VERSION),
    end_version_(OB_INVALID_VERSION),
    schema_id_(OB_INVALID_ID),
    precise_schema_version_(OB_INVALID_VERSION),
    admission_seq_(0),
    table_namespace_(NS_MAX),
    is_exist_(false),
    reserved_()
{
  MEMSET(reserved_, 0, sizeof(reserved_));
}

ObSchemaResolveRange::ObSchemaResolveRange(
    const int64_t start_version,
    const int64_t end_version,
    const uint64_t schema_id,
    const int64_t precise_schema_version,
    const uint64_t admission_seq,
    const ObTableNameNamespace table_namespace,
    const bool is_exist)
  : start_version_(start_version),
    end_version_(end_version),
    schema_id_(schema_id),
    precise_schema_version_(precise_schema_version),
    admission_seq_(admission_seq),
    table_namespace_(table_namespace),
    is_exist_(is_exist),
    reserved_()
{
  MEMSET(reserved_, 0, sizeof(reserved_));
}

bool ObSchemaResolveRange::is_valid() const
{
  const bool common_valid = start_version_ >= 0
      && end_version_ >= start_version_
      && table_namespace_ <= NS_MAX;
  return common_valid
      && ((is_exist_
           && OB_INVALID_ID != schema_id_
           && precise_schema_version_ >= 0
           && precise_schema_version_ <= start_version_)
          || (!is_exist_
              && OB_INVALID_ID == schema_id_
              && OB_INVALID_VERSION == precise_schema_version_));
}

bool ObSchemaResolveRange::contains(const int64_t snapshot_version) const
{
  return snapshot_version >= start_version_ && snapshot_version <= end_version_;
}

bool ObSchemaResolveRange::same_result(const ObSchemaResolveRange &other) const
{
  return schema_id_ == other.schema_id_
      && precise_schema_version_ == other.precise_schema_version_
      && table_namespace_ == other.table_namespace_
      && is_exist_ == other.is_exist_;
}

void ObSchemaResolveRange::reset()
{
  *this = ObSchemaResolveRange();
}

void ObSchemaResolveResult::reset()
{
  range_start_ = OB_INVALID_VERSION;
  range_end_ = OB_INVALID_VERSION;
  schema_id_ = OB_INVALID_ID;
  precise_schema_version_ = OB_INVALID_VERSION;
  table_namespace_ = NS_MAX;
  is_exist_ = false;
}

void ObSchemaResolveResult::assign(const ObSchemaResolveRange &range)
{
  range_start_ = range.start_version_;
  range_end_ = range.end_version_;
  schema_id_ = range.schema_id_;
  precise_schema_version_ = range.precise_schema_version_;
  table_namespace_ = range.table_namespace_;
  is_exist_ = range.is_exist_;
}

bool ObSchemaResolveResult::is_valid() const
{
  ObSchemaResolveRange range(range_start_, range_end_, schema_id_,
                             precise_schema_version_, 0, table_namespace_, is_exist_);
  return range.is_valid();
}

ObSchemaResolveCacheValue::ObSchemaResolveCacheValue()
  : format_version_(OB_SCHEMA_RESOLVE_CACHE_FORMAT_VERSION),
    range_count_(0),
    flags_(0),
    publish_refresh_seq_(0),
    ranges_(NULL)
{
}

ObSchemaResolveCacheValue::ObSchemaResolveCacheValue(
    const ObSchemaResolveRange *ranges,
    const int64_t range_count,
    const uint64_t publish_refresh_seq)
  : format_version_(OB_SCHEMA_RESOLVE_CACHE_FORMAT_VERSION),
    range_count_(static_cast<uint16_t>(range_count)),
    flags_(0),
    publish_refresh_seq_(publish_refresh_seq),
    ranges_(ranges)
{
}

bool ObSchemaResolveCacheValue::is_valid() const
{
  bool valid = OB_SCHEMA_RESOLVE_CACHE_FORMAT_VERSION == format_version_
      && range_count_ > 0
      && range_count_ <= OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT
      && OB_NOT_NULL(ranges_);
  for (int64_t i = 0; valid && i < range_count_; ++i) {
    valid = ranges_[i].is_valid();
    if (valid && i > 0) {
      const ObSchemaResolveRange &prev = ranges_[i - 1];
      const ObSchemaResolveRange &curr = ranges_[i];
      valid = prev.end_version_ < curr.start_version_
          && !(prev.end_version_ != OB_SCHEMA_VERSION_INF
               && prev.end_version_ + 1 == curr.start_version_
               && prev.same_result(curr));
    }
  }
  return valid;
}

int ObSchemaResolveCacheValue::find(
    const int64_t snapshot_version,
    ObSchemaResolveResult &result) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  result.reset();
  if (snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!is_valid()) {
    ret = OB_INVALID_DATA;
  } else {
    int64_t left = 0;
    int64_t right = range_count_ - 1;
    while (left <= right) {
      const int64_t mid = left + (right - left) / 2;
      if (snapshot_version < ranges_[mid].start_version_) {
        right = mid - 1;
      } else if (snapshot_version > ranges_[mid].end_version_) {
        left = mid + 1;
      } else {
        result.assign(ranges_[mid]);
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int64_t ObSchemaResolveCacheValue::size() const
{
  return sizeof(*this) + range_count_ * sizeof(ObSchemaResolveRange);
}

int ObSchemaResolveCacheValue::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  value = NULL;
  if (!is_valid() || OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolve cache value buffer", KR(ret), K(buf_len), K(size()), KPC(this));
  } else {
    ObSchemaResolveCacheValue *new_value = new (buf) ObSchemaResolveCacheValue();
    ObSchemaResolveRange *new_ranges = reinterpret_cast<ObSchemaResolveRange *>(
        buf + sizeof(ObSchemaResolveCacheValue));
    MEMCPY(new_ranges, ranges_, range_count_ * sizeof(ObSchemaResolveRange));
    new_value->format_version_ = format_version_;
    new_value->range_count_ = range_count_;
    new_value->flags_ = flags_;
    new_value->publish_refresh_seq_ = publish_refresh_seq_;
    new_value->ranges_ = new_ranges;
    value = new_value;
  }
  return ret;
}

namespace
{
struct ResolveRangeLess
{
  bool operator()(const ObSchemaResolveRange &lhs, const ObSchemaResolveRange &rhs) const
  {
    return lhs.start_version_ < rhs.start_version_
        || (lhs.start_version_ == rhs.start_version_
            && lhs.end_version_ < rhs.end_version_);
  }
};

bool is_protected_range(const ObSchemaResolveRange &range,
                        const ObSchemaResolveRange &authoritative_range)
{
  return range.end_version_ == OB_SCHEMA_VERSION_INF
      || (range.admission_seq_ == authoritative_range.admission_seq_
          && range.start_version_ <= authoritative_range.end_version_
          && range.end_version_ >= authoritative_range.start_version_);
}
} // namespace

bool ObSchemaResolveRangeSet::is_normalized(
    const ObIArray<ObSchemaResolveRange> &ranges)
{
  bool valid = ranges.count() > 0
      && ranges.count() <= OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT;
  for (int64_t i = 0; valid && i < ranges.count(); ++i) {
    valid = ranges.at(i).is_valid();
    if (valid && i > 0) {
      const ObSchemaResolveRange &prev = ranges.at(i - 1);
      const ObSchemaResolveRange &curr = ranges.at(i);
      valid = prev.end_version_ < curr.start_version_
          && !(prev.end_version_ != OB_SCHEMA_VERSION_INF
               && prev.end_version_ + 1 == curr.start_version_
               && prev.same_result(curr));
    }
  }
  return valid;
}

int ObSchemaResolveRangeSet::normalize_and_evict_(
    const ObSchemaResolveRange &authoritative_range,
    RangeArray &ranges)
{
  int ret = OB_SUCCESS;
  lib::ob_sort(ranges.begin(), ranges.end(), ResolveRangeLess());

  for (int64_t i = 1; OB_SUCC(ret) && i < ranges.count();) {
    ObSchemaResolveRange &prev = ranges.at(i - 1);
    const ObSchemaResolveRange &curr = ranges.at(i);
    if (prev.end_version_ >= curr.start_version_) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("overlapping schema resolve ranges", KR(ret), K(prev), K(curr));
    } else if (prev.end_version_ != OB_SCHEMA_VERSION_INF
               && prev.end_version_ + 1 == curr.start_version_
               && prev.same_result(curr)) {
      prev.end_version_ = curr.end_version_;
      prev.admission_seq_ = MAX(prev.admission_seq_, curr.admission_seq_);
      if (OB_FAIL(ranges.remove(i))) {
        LOG_WARN("failed to merge adjacent schema resolve range", KR(ret), K(i));
      }
    } else {
      ++i;
    }
  }

  while (OB_SUCC(ret) && ranges.count() > OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT) {
    int64_t evict_idx = -1;
    uint64_t min_seq = UINT64_MAX;
    for (int64_t i = 0; i < ranges.count(); ++i) {
      const ObSchemaResolveRange &candidate = ranges.at(i);
      if (!is_protected_range(candidate, authoritative_range)
          && candidate.admission_seq_ < min_seq) {
        min_seq = candidate.admission_seq_;
        evict_idx = i;
      }
    }
    if (evict_idx < 0) {
      for (int64_t i = 0; i < ranges.count(); ++i) {
        if (ranges.at(i).admission_seq_ < min_seq) {
          min_seq = ranges.at(i).admission_seq_;
          evict_idx = i;
        }
      }
    }
    if (evict_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to select schema resolve range to evict", KR(ret), K(ranges.count()));
    } else if (OB_FAIL(ranges.remove(evict_idx))) {
      LOG_WARN("failed to evict schema resolve range", KR(ret), K(evict_idx));
    }
  }

  if (OB_SUCC(ret) && !is_normalized(ranges)) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("schema resolve ranges are not normalized", KR(ret), K(ranges));
  }
  return ret;
}

int ObSchemaResolveRangeSet::merge(
    const ObSchemaResolveCacheValue *current,
    const ObSchemaResolveRange &authoritative_range,
    RangeArray &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!authoritative_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid authoritative schema resolve range", KR(ret), K(authoritative_range));
  } else if (OB_NOT_NULL(current) && !current->is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid current schema resolve value", KR(ret), KPC(current));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(current)
         && i < current->range_count_; ++i) {
      const ObSchemaResolveRange &old_range = current->ranges_[i];
      if (old_range.end_version_ < authoritative_range.start_version_
          || old_range.start_version_ > authoritative_range.end_version_) {
        if (OB_FAIL(result.push_back(old_range))) {
          LOG_WARN("failed to retain disjoint schema resolve range", KR(ret), K(old_range));
        }
      } else {
        if (old_range.start_version_ < authoritative_range.start_version_) {
          ObSchemaResolveRange left = old_range;
          left.end_version_ = authoritative_range.start_version_ - 1;
          if (OB_FAIL(result.push_back(left))) {
            LOG_WARN("failed to retain left schema resolve fragment", KR(ret), K(left));
          }
        }
        if (OB_SUCC(ret)
            && authoritative_range.end_version_ != OB_SCHEMA_VERSION_INF
            && old_range.end_version_ > authoritative_range.end_version_) {
          ObSchemaResolveRange right = old_range;
          right.start_version_ = authoritative_range.end_version_ + 1;
          if (OB_FAIL(result.push_back(right))) {
            LOG_WARN("failed to retain right schema resolve fragment", KR(ret), K(right));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(result.push_back(authoritative_range))) {
      LOG_WARN("failed to add authoritative schema resolve range", KR(ret), K(authoritative_range));
    } else if (OB_SUCC(ret)
               && OB_FAIL(normalize_and_evict_(authoritative_range, result))) {
      LOG_WARN("failed to normalize schema resolve ranges", KR(ret), K(authoritative_range));
    }
  }
  return ret;
}

void ObSchemaPublishSnapshot::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  refresh_seq_ = UINT64_MAX;
  cache_generations_.reset();
  refreshed_version_ = OB_INVALID_VERSION;
}

bool ObSchemaPublishSnapshot::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != tenant_id_
      && 0 == (refresh_seq_ & 1)
      && cache_generations_.is_valid()
      && refreshed_version_ >= 0;
}

int64_t ObOnDemandSchemaRetryPolicy::get_load_deadline(
    const bool worker_timeout_valid,
    const int64_t worker_timeout_us,
    const int64_t now_us)
{
  int64_t deadline = worker_timeout_us;
  if (!worker_timeout_valid) {
    deadline = now_us >= INT64_MAX - DEFAULT_LOAD_TIMEOUT_US
        ? INT64_MAX : now_us + DEFAULT_LOAD_TIMEOUT_US;
  }
  return deadline;
}

int ObOnDemandSchemaRetryPolicy::calc_backoff_info(
    const int64_t abs_deadline_us,
    const int64_t now_us,
    int64_t &retry_count,
    int64_t &backoff_us)
{
  int ret = OB_SUCCESS;
  backoff_us = 0;
  if (retry_count < 0 || abs_deadline_us < 0 || now_us < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (now_us >= abs_deadline_us) {
    ret = OB_TIMEOUT;
  } else if (retry_count >= MAX_STALE_RETRY_COUNT) {
    ret = OB_SCHEMA_EAGAIN;
  } else {
    const int64_t shift = MIN(retry_count, static_cast<int64_t>(5));
    backoff_us = MIN(STALE_RETRY_BASE_US << shift,
                     abs_deadline_us - now_us);
    ++retry_count;
  }
  return ret;
}

ObTenantSchemaPublishState::ObTenantSchemaPublishState()
  : publish_lock_(ObLatchIds::OB_SCHEMA_GETTER_LOCK),
    tenant_id_(OB_INVALID_TENANT_ID),
    refresh_seq_(0),
    cache_generations_(),
    refreshed_version_(OB_INVALID_VERSION),
    publishing_(false),
    inited_(false)
{
}

int ObTenantSchemaPublishState::init(
    const uint64_t tenant_id,
    const int64_t refreshed_version,
    const uint64_t cache_generation)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == tenant_id
             || refreshed_version < 0
             || 0 == cache_generation) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
    ATOMIC_STORE_REL(&refresh_seq_, 0);
    ATOMIC_STORE_REL(&cache_generations_.id_resolve_, cache_generation);
    ATOMIC_STORE_REL(&cache_generations_.name_resolve_, cache_generation);
    ATOMIC_STORE_REL(&cache_generations_.tablegroup_, cache_generation);
    ATOMIC_STORE_REL(&refreshed_version_, refreshed_version);
    publishing_ = false;
    inited_ = true;
  }
  return ret;
}

int ObTenantSchemaPublishState::read_stable(
    ObSchemaPublishSnapshot &snapshot,
    const int64_t abs_timeout_us) const
{
  int ret = OB_SUCCESS;
  snapshot.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t spin_count = 0;
    while (OB_SUCC(ret)) {
      const uint64_t seq1 = ATOMIC_LOAD_ACQ(&refresh_seq_);
      if (0 != (seq1 & 1)) {
        if (INT64_MAX != abs_timeout_us
            && ObTimeUtility::current_time() >= abs_timeout_us) {
          ret = OB_TIMEOUT;
        } else {
          PAUSE();
          if (0 == (++spin_count & 0x3FF)) {
            sched_yield();
          }
        }
      } else {
        ObSchemaCacheGenerations cache_generations;
        cache_generations.id_resolve_ =
            ATOMIC_LOAD_ACQ(&cache_generations_.id_resolve_);
        cache_generations.name_resolve_ =
            ATOMIC_LOAD_ACQ(&cache_generations_.name_resolve_);
        cache_generations.tablegroup_ =
            ATOMIC_LOAD_ACQ(&cache_generations_.tablegroup_);
        const int64_t frontier = ATOMIC_LOAD_ACQ(&refreshed_version_);
        const uint64_t seq2 = ATOMIC_LOAD_ACQ(&refresh_seq_);
        if (seq1 == seq2 && 0 == (seq2 & 1)) {
          snapshot.tenant_id_ = tenant_id_;
          snapshot.refresh_seq_ = seq2;
          snapshot.cache_generations_ = cache_generations;
          snapshot.refreshed_version_ = frontier;
          break;
        } else if (INT64_MAX != abs_timeout_us
                   && ObTimeUtility::current_time() >= abs_timeout_us) {
          ret = OB_TIMEOUT;
        } else {
          PAUSE();
        }
      }
    }
  }
  return ret;
}

int ObTenantSchemaPublishState::get_published_snapshot(
    const int64_t snapshot_version,
    const int64_t abs_timeout_us,
    ObSchemaPublishSnapshot &snapshot) const
{
  int ret = OB_SUCCESS;
  snapshot.reset();
  if (snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (INT64_MAX != abs_timeout_us
             && ObTimeUtility::current_time() >= abs_timeout_us) {
    ret = OB_TIMEOUT;
  } else if (OB_FAIL(read_stable(snapshot, abs_timeout_us))) {
    LOG_WARN("failed to read stable tenant schema publication state",
             KR(ret), K(tenant_id_), K(snapshot_version));
  } else if (snapshot_version > snapshot.refreshed_version_) {
    ret = OB_SCHEMA_EAGAIN;
  }
  return ret;
}

bool ObTenantSchemaPublishState::can_publish(const ObSchemaPublishSnapshot &token) const
{
  bool match = inited_ && token.is_valid() && token.tenant_id_ == tenant_id_;
  if (match) {
    const uint64_t seq1 = ATOMIC_LOAD_ACQ(&refresh_seq_);
    ObSchemaCacheGenerations cache_generations;
    cache_generations.id_resolve_ =
        ATOMIC_LOAD_ACQ(&cache_generations_.id_resolve_);
    cache_generations.name_resolve_ =
        ATOMIC_LOAD_ACQ(&cache_generations_.name_resolve_);
    cache_generations.tablegroup_ =
        ATOMIC_LOAD_ACQ(&cache_generations_.tablegroup_);
    const int64_t frontier = ATOMIC_LOAD_ACQ(&refreshed_version_);
    const uint64_t seq2 = ATOMIC_LOAD_ACQ(&refresh_seq_);
    match = 0 == (seq1 & 1)
        && seq1 == seq2
        && seq2 == token.refresh_seq_
        && cache_generations == token.cache_generations_
        && frontier == token.refreshed_version_;
  }
  return match;
}

int ObTenantSchemaPublishState::begin_publish_(ObSchemaPublishSnapshot &before)
{
  int ret = OB_SUCCESS;
  before.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(publish_lock_.lock())) {
    LOG_WARN("failed to lock tenant schema publish state", KR(ret), K(tenant_id_));
  } else {
    const uint64_t seq = ATOMIC_LOAD_ACQ(&refresh_seq_);
    if (publishing_ || 0 != (seq & 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant schema publish state is already odd", KR(ret), K(tenant_id_), K(seq));
      publish_lock_.unlock();
    } else if (seq >= UINT64_MAX - 1) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("tenant schema refresh sequence exhausted", KR(ret), K(tenant_id_), K(seq));
      publish_lock_.unlock();
    } else {
      before.tenant_id_ = tenant_id_;
      before.refresh_seq_ = seq;
      before.cache_generations_.id_resolve_ =
          ATOMIC_LOAD_ACQ(&cache_generations_.id_resolve_);
      before.cache_generations_.name_resolve_ =
          ATOMIC_LOAD_ACQ(&cache_generations_.name_resolve_);
      before.cache_generations_.tablegroup_ =
          ATOMIC_LOAD_ACQ(&cache_generations_.tablegroup_);
      before.refreshed_version_ = ATOMIC_LOAD_ACQ(&refreshed_version_);
      ATOMIC_STORE_REL(&refresh_seq_, seq + 1);
      publishing_ = true;
    }
  }
  return ret;
}

int ObTenantSchemaPublishState::bump_generation_(const uint64_t domains)
{
  int ret = OB_SUCCESS;
  if (SCHEMA_CACHE_DOMAIN_NONE == domains
      || 0 != (domains & ~static_cast<uint64_t>(SCHEMA_CACHE_DOMAIN_ALL))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!publishing_ || 0 == (ATOMIC_LOAD_ACQ(&refresh_seq_) & 1)) {
    ret = OB_STATE_NOT_MATCH;
  } else if ((0 != (domains & SCHEMA_CACHE_DOMAIN_ID_RESOLVE)
              && UINT64_MAX == ATOMIC_LOAD_ACQ(&cache_generations_.id_resolve_))
             || (0 != (domains & SCHEMA_CACHE_DOMAIN_NAME_RESOLVE)
                 && UINT64_MAX == ATOMIC_LOAD_ACQ(&cache_generations_.name_resolve_))
             || (0 != (domains & SCHEMA_CACHE_DOMAIN_TABLEGROUP)
                 && UINT64_MAX == ATOMIC_LOAD_ACQ(&cache_generations_.tablegroup_))) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    if (0 != (domains & SCHEMA_CACHE_DOMAIN_ID_RESOLVE)) {
      ATOMIC_AAF(&cache_generations_.id_resolve_, 1);
    }
    if (0 != (domains & SCHEMA_CACHE_DOMAIN_NAME_RESOLVE)) {
      ATOMIC_AAF(&cache_generations_.name_resolve_, 1);
    }
    if (0 != (domains & SCHEMA_CACHE_DOMAIN_TABLEGROUP)) {
      ATOMIC_AAF(&cache_generations_.tablegroup_, 1);
    }
  }
  return ret;
}

int ObTenantSchemaPublishState::finish_publish_(
    const int64_t refreshed_version,
    const bool commit)
{
  int ret = OB_SUCCESS;
  const uint64_t seq = ATOMIC_LOAD_ACQ(&refresh_seq_);
  const int64_t old_frontier = ATOMIC_LOAD_ACQ(&refreshed_version_);
  if (!publishing_ || 0 == (seq & 1)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (commit && refreshed_version < old_frontier) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (commit) {
      ATOMIC_STORE_REL(&refreshed_version_, refreshed_version);
    }
    ATOMIC_STORE_REL(&refresh_seq_, seq + 1);
    publishing_ = false;
    const int unlock_ret = publish_lock_.unlock();
    if (OB_SUCCESS != unlock_ret) {
      ret = unlock_ret;
      LOG_ERROR("failed to unlock tenant schema publish state", KR(ret), K(tenant_id_));
    }
  }
  return ret;
}

ObTenantSchemaPublicationGuard::ObTenantSchemaPublicationGuard()
  : state_(NULL), before_(), active_(false)
{
}

ObTenantSchemaPublicationGuard::~ObTenantSchemaPublicationGuard()
{
  if (active_ && OB_NOT_NULL(state_)) {
    const int tmp_ret = state_->finish_publish_(before_.refreshed_version_, false);
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR_RET(tmp_ret, "failed to abort tenant schema publication",
                    K(tmp_ret), K(before_));
    }
  }
}

int ObTenantSchemaPublicationGuard::begin(ObTenantSchemaPublishState &state)
{
  int ret = OB_SUCCESS;
  if (active_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(state.begin_publish_(before_))) {
    LOG_WARN("failed to begin tenant schema publication", KR(ret));
  } else {
    state_ = &state;
    active_ = true;
  }
  return ret;
}

int ObTenantSchemaPublicationGuard::bump_generation(const uint64_t domains)
{
  int ret = OB_SUCCESS;
  if (!active_ || OB_ISNULL(state_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(state_->bump_generation_(domains))) {
    LOG_WARN("failed to bump tenant schema cache generation",
             KR(ret), K(domains), K(before_));
  }
  return ret;
}

int ObTenantSchemaPublicationGuard::commit(const int64_t refreshed_version)
{
  int ret = OB_SUCCESS;
  if (!active_ || OB_ISNULL(state_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(state_->finish_publish_(refreshed_version, true))) {
    LOG_WARN("failed to commit tenant schema publication", KR(ret), K(refreshed_version), K(before_));
  } else {
    state_ = NULL;
    active_ = false;
  }
  return ret;
}

ObSchemaRefreshPublication::ObSchemaRefreshPublication()
  : state_(NULL), snapshot_(), guard_(), bump_domains_(SCHEMA_CACHE_DOMAIN_NONE),
    begin_ret_(OB_SUCCESS), inited_(false)
{
}

int ObSchemaRefreshPublication::init(ObTenantSchemaPublishState *state)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_NOT_NULL(state) && OB_FAIL(state->read_stable(snapshot_))) {
    LOG_WARN("failed to read schema refresh preparation snapshot", KR(ret));
  } else {
    state_ = state;
    inited_ = true;
  }
  return ret;
}

int ObSchemaRefreshPublication::begin()
{
  int ret = begin_ret_;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCC(ret) && is_enabled() && !guard_.is_active()) {
    if (OB_FAIL(guard_.begin(*state_))) {
      LOG_WARN("failed to begin prepared schema publication", KR(ret), K(snapshot_));
    } else if (guard_.before().refresh_seq_ != snapshot_.refresh_seq_
               || guard_.before().cache_generations_ != snapshot_.cache_generations_
               || guard_.before().refreshed_version_ != snapshot_.refreshed_version_) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("schema publication changed during preparation", KR(ret),
               K(snapshot_), "current", guard_.before());
    }
  }
  // A failed begin must not be swallowed by the repair-to-fallback path, nor
  // retried against a different epoch with the already prepared repair plan.
  begin_ret_ = ret;
  return ret;
}

int ObSchemaRefreshPublication::bump_pending_generations_()
{
  int ret = OB_SUCCESS;
  if (SCHEMA_CACHE_DOMAIN_NONE != bump_domains_) {
    if (OB_FAIL(guard_.bump_generation(bump_domains_))) {
      LOG_WARN("failed to invalidate schema refresh cache domains",
               KR(ret), K(bump_domains_));
    } else {
      bump_domains_ = SCHEMA_CACHE_DOMAIN_NONE;
    }
  }
  return ret;
}

int ObSchemaRefreshPublication::prepare_publish(const int64_t formal_version)
{
  int ret = OB_SUCCESS;
  if (formal_version <= 0 || !ObSchemaService::is_formal_version(formal_version)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(begin())) {
    LOG_WARN("failed to begin formal schema publication", KR(ret), K(formal_version));
  } else if (is_enabled()) {
    if (formal_version < guard_.before().refreshed_version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("formal schema publication would move backwards",
               KR(ret), K(formal_version), "before", guard_.before());
    } else if (OB_FAIL(bump_pending_generations_())) {
      LOG_WARN("failed to prepare schema cache generations", KR(ret), K(formal_version));
    }
  }
  return ret;
}

int ObSchemaRefreshPublication::finish(
    const int64_t store_version,
    const int64_t formal_version)
{
  int ret = begin_ret_;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (guard_.is_active()) {
    int64_t commit_version = guard_.before().refreshed_version_;
    if (OB_SUCC(ret) && OB_FAIL(bump_pending_generations_())) {
      LOG_WARN("failed to finish schema cache invalidation", KR(ret));
    }
    if (OB_SUCC(ret)) {
      if (store_version < commit_version || formal_version < commit_version
          || !ObSchemaService::is_formal_version(formal_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema store frontier after formal publication",
                 KR(ret), K(store_version), K(formal_version), K(commit_version));
      } else if (store_version >= formal_version) {
        // A late add_schema() error does not undo its formal store publication.
        // Conversely, a core/sys temporary store version below this target
        // must not make the on-demand frontier advance after a failed publish.
        commit_version = formal_version;
      }
    }
    const int finish_ret = guard_.commit(commit_version);
    if (OB_SUCCESS != finish_ret) {
      LOG_WARN("failed to close schema refresh publication",
               KR(finish_ret), K(commit_version));
      ret = OB_SUCC(ret) ? finish_ret : ret;
    }
  }
  if (inited_ && !guard_.is_active()) {
    // Refresh may check core/sys versions and retry after a publish error.
    // Those SQL reads run with an even sequence and a fresh preparation epoch.
    const int snapshot_ret = is_enabled() ? state_->read_stable(snapshot_) : OB_SUCCESS;
    if (OB_SUCCESS != snapshot_ret) {
      LOG_WARN("failed to reload schema publication after refresh attempt", KR(snapshot_ret));
      ret = OB_SUCC(ret) ? snapshot_ret : ret;
    }
    bump_domains_ = SCHEMA_CACHE_DOMAIN_NONE;
    begin_ret_ = snapshot_ret;
  }
  return ret;
}

ObTenantSchemaPublishStateMap::ObTenantSchemaPublishStateMap()
  : states_(),
    create_lock_(ObLatchIds::OB_SCHEMA_GETTER_LOCK),
    inited_(false)
{
}

ObTenantSchemaPublishStateMap::~ObTenantSchemaPublishStateMap()
{
  destroy();
}

int ObTenantSchemaPublishStateMap::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(states_.create(DEFAULT_BUCKET_COUNT,
                                    "SchemaPubBucket",
                                    "SchemaPubNode"))) {
    LOG_WARN("failed to create tenant schema publish state map", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObTenantSchemaPublishStateMap::destroy()
{
  if (inited_) {
    for (StateMap::iterator it = states_.begin(); it != states_.end(); ++it) {
      ObTenantSchemaPublishState *state = it->second;
      if (OB_NOT_NULL(state)) {
        state->~ObTenantSchemaPublishState();
        ob_free(state);
      }
    }
    states_.destroy();
    inited_ = false;
  }
}

int ObTenantSchemaPublishStateMap::get(
    const uint64_t tenant_id,
    ObTenantSchemaPublishState *&state) const
{
  int ret = OB_SUCCESS;
  state = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int hash_ret = states_.get_refactored(tenant_id, state);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
    }
  }
  return ret;
}

int ObTenantSchemaPublishStateMap::get_or_create(
    const uint64_t tenant_id,
    const int64_t initial_refreshed_version,
    ObTenantSchemaPublishState *&state)
{
  int ret = OB_SUCCESS;
  state = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id
      || initial_refreshed_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get(tenant_id, state)) && OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    lib::ObMutexGuard guard(create_lock_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else {
      const int second_get_ret = get(tenant_id, state);
      if (OB_SUCCESS == second_get_ret) {
        // Another creator won while this thread waited for create_lock_.
      } else if (OB_ENTRY_NOT_EXIST != second_get_ret) {
        ret = second_get_ret;
        LOG_WARN("failed to recheck tenant schema publish state",
                 KR(ret), K(tenant_id));
      } else {
        void *buf = ob_malloc(sizeof(ObTenantSchemaPublishState),
                              SET_USE_500("SchemaPubState", ObCtxIds::SCHEMA_SERVICE));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          ObTenantSchemaPublishState *new_state = new (buf) ObTenantSchemaPublishState();
          if (OB_FAIL(new_state->init(tenant_id, initial_refreshed_version))) {
            LOG_WARN("failed to init tenant schema publish state",
                     KR(ret), K(tenant_id), K(initial_refreshed_version));
            new_state->~ObTenantSchemaPublishState();
            ob_free(new_state);
          } else if (OB_FAIL(states_.set_refactored(tenant_id, new_state, 0))) {
            LOG_WARN("failed to add tenant schema publish state", KR(ret), K(tenant_id));
            new_state->~ObTenantSchemaPublishState();
            ob_free(new_state);
          } else {
            state = new_state;
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantSchemaPublishStateMap::retire(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantSchemaPublishState *state = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Serialize lifecycle changes with creation.  The state itself remains in
    // the map until destroy(), because readers can legally retain its address
    // while they perform a coordinated SQL load outside the map lock.
    lib::ObMutexGuard map_guard(create_lock_);
    if (OB_SUCCESS != map_guard.get_ret()) {
      ret = map_guard.get_ret();
    } else {
      ret = get(tenant_id, state);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret) && OB_ISNULL(state)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_SUCC(ret)) {
        ObTenantSchemaPublicationGuard publication;
        if (OB_FAIL(publication.begin(*state))) {
          LOG_WARN("failed to begin tenant schema state retirement",
                   KR(ret), K(tenant_id));
        } else if (OB_FAIL(publication.bump_generation())) {
          LOG_WARN("failed to invalidate tenant schema cache epoch on retirement",
                   KR(ret), K(tenant_id), K(publication.before()));
        } else if (OB_FAIL(publication.commit(
                       publication.before().refreshed_version_))) {
          LOG_WARN("failed to finish tenant schema state retirement",
                   KR(ret), K(tenant_id), K(publication.before()));
        }
      }
    }
  }
  return ret;
}

int ObTenantSchemaPublishStateMap::get_published_snapshot(
    const uint64_t tenant_id,
    const int64_t snapshot_version,
    const int64_t abs_timeout_us,
    ObTenantSchemaPublishState *&state,
    ObSchemaPublishSnapshot &snapshot) const
{
  int ret = OB_SUCCESS;
  state = NULL;
  snapshot.reset();
  if (snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (INT64_MAX != abs_timeout_us
             && ObTimeUtility::current_time() >= abs_timeout_us) {
    ret = OB_TIMEOUT;
  } else if (OB_FAIL(get(tenant_id, state))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // A point lookup cannot infer the publication frontier from its own
      // snapshot.  Its schema-store lifecycle initialization is not complete.
      ret = OB_SCHEMA_EAGAIN;
    }
  } else if (OB_ISNULL(state)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(state->get_published_snapshot(
                 snapshot_version, abs_timeout_us, snapshot))) {
    if (OB_SCHEMA_EAGAIN != ret) {
      LOG_WARN("failed to get published tenant schema snapshot",
               KR(ret), K(tenant_id), K(snapshot_version));
    }
  }
  return ret;
}

ObSchemaResolveCache::ObSchemaResolveCache()
  : id_cache_(),
    name_cache_(),
    write_locks_(),
    admission_seq_(0),
    inited_(false)
{
}

ObSchemaResolveCache::~ObSchemaResolveCache()
{
  destroy();
}

int ObSchemaResolveCache::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(id_cache_.init(OB_SCHEMA_ID_RESOLVE_CACHE_NAME, 10))) {
    LOG_WARN("failed to init schema id resolve cache", KR(ret));
  } else if (OB_FAIL(name_cache_.init(OB_SCHEMA_NAME_RESOLVE_CACHE_NAME, 10))) {
    LOG_WARN("failed to init schema name resolve cache", KR(ret));
    id_cache_.destroy();
  } else {
    admission_seq_ = 0;
    inited_ = true;
  }
  return ret;
}

void ObSchemaResolveCache::destroy()
{
  if (inited_) {
    name_cache_.destroy();
    id_cache_.destroy();
    inited_ = false;
  }
}

template <typename Key, typename Cache>
int ObSchemaResolveCache::get_(
    Cache &cache,
    const Key &key,
    const int64_t snapshot_version,
    ObSchemaResolveResult &result)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObSchemaResolveCacheValue *value = NULL;
  result.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || snapshot_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(cache.get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      record_schema_resolve_cache_miss(key);
    } else {
      LOG_WARN("failed to get schema resolve cache", KR(ret), K(key));
    }
  } else if (OB_ISNULL(value) || !value->is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("invalid schema resolve cache value, treat as miss", KR(ret), K(key), KP(value));
  } else {
    record_schema_resolve_cache_hit(key);
    if (OB_FAIL(value->find(snapshot_version, result))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to find schema resolve range", KR(ret), K(key), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObSchemaResolveCache::get(
    const ObSchemaIDResolveKey &key,
    const int64_t snapshot_version,
    ObSchemaResolveResult &result)
{
  return get_(id_cache_, key, snapshot_version, result);
}

int ObSchemaResolveCache::get(
    const ObSchemaNameResolveKey &key,
    const int64_t snapshot_version,
    ObSchemaResolveResult &result)
{
  return get_(name_cache_, key, snapshot_version, result);
}

int ObSchemaResolveCache::get_if_current(
    const ObSchemaIDResolveKey &key,
    const int64_t snapshot_version,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &token,
    ObSchemaResolveResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (key.tenant_id_ != token.tenant_id_
      || key.cache_generation_ != token.cache_generations_.id_resolve_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state.can_publish(token)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(get(key, snapshot_version, result))) {
  } else if (!state.can_publish(token)) {
    result.reset();
    ret = OB_SCHEMA_EAGAIN;
  }
  return ret;
}

int ObSchemaResolveCache::get_if_current(
    const ObSchemaNameResolveKey &key,
    const int64_t snapshot_version,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &token,
    ObSchemaResolveResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (key.tenant_id_ != token.tenant_id_
      || key.cache_generation_ != token.cache_generations_.name_resolve_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state.can_publish(token)) {
    ret = OB_SCHEMA_EAGAIN;
  } else if (OB_FAIL(get(key, snapshot_version, result))) {
  } else if (!state.can_publish(token)) {
    result.reset();
    ret = OB_SCHEMA_EAGAIN;
  }
  return ret;
}

template <typename Key, typename Cache>
int ObSchemaResolveCache::merge_and_put_(
    Cache &cache,
    const Key &key,
    const ObSchemaResolveRange &input_range,
    const bool only_if_present,
    const bool erase_on_failure)
{
  int ret = OB_SUCCESS;
  const ObSchemaResolveCacheValue *current = NULL;
  ObKVCacheHandle current_handle;
  ObSchemaResolveRange range = input_range;
  ObSchemaResolveRangeSet::RangeArray next_ranges;

  // Both callers hold the key's stripe. Maintenance and merge reads must not
  // inflate query hit counts or cache eviction scores.
  const int get_ret = cache.peek(key, current, current_handle);
  if (OB_ENTRY_NOT_EXIST == get_ret) {
    current = NULL;
  } else if (OB_SUCCESS != get_ret) {
    ret = get_ret;
    LOG_WARN("failed to reload schema resolve value under stripe", KR(ret), K(key));
  } else if (OB_ISNULL(current) || !current->is_valid()) {
    current = NULL;
    int tmp_ret = cache.erase(key);
    if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
      LOG_WARN("failed to erase invalid schema resolve value", KR(tmp_ret), K(key));
    }
  }

  if (OB_SUCC(ret) && (!only_if_present || OB_ENTRY_NOT_EXIST != get_ret)) {
    // An old loader that passed its token check holds this same stripe, so its
    // late fill is visible to the refresh peek. Later old-token loaders are
    // rejected by the odd/advanced publication sequence. A miss can safely stay
    // absent without admitting a range; invalid resident values still get repaired.
    if (0 == range.admission_seq_) {
      range.admission_seq_ = ATOMIC_AAF(&admission_seq_, 1);
    }
    if (OB_FAIL(ObSchemaResolveRangeSet::merge(current, range, next_ranges))) {
      LOG_WARN("failed to merge schema resolve range", KR(ret), K(key), K(range));
    } else {
      ObSchemaResolveCacheValue next_value(&next_ranges.at(0), next_ranges.count());
      if (OB_FAIL(cache.put(key, next_value, true))) {
        LOG_WARN("failed to publish schema resolve value", KR(ret), K(key), K(range));
      }
    }
  }

  if (OB_FAIL(ret) && erase_on_failure) {
    const int publish_ret = ret;
    const int tmp_ret = cache.erase(key);
    if (OB_SUCCESS == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
      ret = OB_SUCCESS;
      LOG_WARN("schema resolve publish failed and entry was erased, degrade to miss",
               KR(publish_ret), K(key));
    } else {
      ret = tmp_ret;
      LOG_ERROR("failed to erase schema resolve value after publish failure",
                KR(tmp_ret), KR(publish_ret), K(key));
    }
  }
  return ret;
}

int ObSchemaResolveCache::publish_if_current(
    const ObSchemaIDResolveKey &key,
    const ObSchemaResolveRange &range,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &token)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !range.is_valid()
             || NS_MAX != range.table_namespace_
             || key.tenant_id_ != token.tenant_id_
             || key.cache_generation_ != token.cache_generations_.id_resolve_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (!state.can_publish(token)) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(merge_and_put_(id_cache_, key, range,
                                    false /*only_if_present*/, false /*erase_on_failure*/))) {
      LOG_WARN("failed to publish id schema resolve range", KR(ret), K(key), K(range));
    }
  }
  return ret;
}

int ObSchemaResolveCache::publish_if_current(
    const ObSchemaNameResolveKey &key,
    const ObSchemaResolveRange &range,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &token)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !range.is_valid()
             || key.table_namespace_ != range.table_namespace_
             || key.tenant_id_ != token.tenant_id_
             || key.cache_generation_ != token.cache_generations_.name_resolve_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (!state.can_publish(token)) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(merge_and_put_(name_cache_, key, range,
                                    false /*only_if_present*/, false /*erase_on_failure*/))) {
      LOG_WARN("failed to publish name schema resolve range", KR(ret), K(key), K(range));
    }
  }
  return ret;
}

int ObSchemaResolveCache::refresh(
    const ObSchemaIDResolveKey &key,
    const ObSchemaResolveRange &range)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !range.is_valid()
             || NS_MAX != range.table_namespace_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(merge_and_put_(id_cache_, key, range,
                                    true /*only_if_present*/, true /*erase_on_failure*/))) {
      LOG_WARN("failed to refresh id schema resolve range", KR(ret), K(key), K(range));
    }
  }
  return ret;
}

int ObSchemaResolveCache::refresh(
    const ObSchemaNameResolveKey &key,
    const ObSchemaResolveRange &range)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || !range.is_valid()
             || key.table_namespace_ != range.table_namespace_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(merge_and_put_(name_cache_, key, range,
                                    true /*only_if_present*/, true /*erase_on_failure*/))) {
      LOG_WARN("failed to refresh name schema resolve range", KR(ret), K(key), K(range));
    }
  }
  return ret;
}

int ObSchemaResolveCache::erase(const ObSchemaIDResolveKey &key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(id_cache_.erase(key)) && OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSchemaResolveCache::erase(const ObSchemaNameResolveKey &key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(name_cache_.erase(key)) && OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

namespace
{

ObTableNameNamespace get_table_name_namespace(const ObSimpleTableSchemaV2 &schema)
{
  ObTableNameNamespace table_namespace = NS_NORMAL_TABLE;
  if (schema.is_user_hidden_table()) {
    table_namespace = NS_HIDDEN_TABLE;
  } else if (schema.is_aux_vp_table()) {
    table_namespace = NS_AUX_VP;
  } else if (schema.is_index_table()) {
    table_namespace = schema.is_built_in_index() ? NS_BUILT_IN_INDEX : NS_NORMAL_INDEX;
  }
  return table_namespace;
}

int normalize_schema_name(const ObString &name,
                          const ObNameCaseMode case_mode,
                          ObIAllocator &allocator,
                          ObString &normalized_name)
{
  int ret = OB_SUCCESS;
  normalized_name.reset();
  if (name.empty()
      || case_mode <= OB_NAME_CASE_INVALID
      || case_mode >= OB_NAME_CASE_MAX) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
    if (OB_FAIL(ob_write_string(allocator, name, normalized_name))) {
      LOG_WARN("fail to copy case-sensitive schema name", KR(ret), K(name));
    }
  } else if (OB_FAIL(ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI,
                                       name, normalized_name, allocator))) {
    LOG_WARN("fail to normalize schema name", KR(ret), K(name), K(case_mode));
  }
  return ret;
}

int build_name_resolve_key(const ObSimpleTableSchemaV2 &schema,
                           const ObNameCaseMode case_mode,
                           const uint64_t cache_generation,
                           ObIAllocator &allocator,
                           ObSchemaNameResolveKey &key)
{
  int ret = OB_SUCCESS;
  ObString name_for_key = schema.get_table_name_str();
  bool is_oracle_index = false;
  const ObTableNameNamespace table_namespace = get_table_name_namespace(schema);
  if (OB_FAIL(schema_resolve_util::check_is_oracle_index(
          schema.get_tenant_id(), schema.get_database_id(), table_namespace,
          is_oracle_index))) {
    LOG_WARN("failed to check oracle index", KR(ret), K(schema));
  } else if (is_oracle_index
             && OB_FAIL(schema_resolve_util::resolve_oracle_index_cache_key_name(
                    schema, name_for_key))) {
    LOG_WARN("failed to resolve oracle index cache key name", KR(ret), K(schema));
  }
  if (OB_FAIL(ret)) {
  } else {
    ObString normalized_name;
    if (OB_FAIL(normalize_schema_name(name_for_key, case_mode,
                                      allocator, normalized_name))) {
      LOG_WARN("fail to normalize table name for incremental refresh",
               KR(ret), K(schema), K(case_mode), K(name_for_key),
               K(is_oracle_index));
    } else {
      key = ObSchemaNameResolveKey(schema.get_tenant_id(), cache_generation,
                                   schema.get_database_id(), schema.get_session_id(),
                                   table_namespace, normalized_name);
      if (!key.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid name resolve key for incremental refresh", KR(ret), K(key));
      }
    }
  }
  return ret;
}

bool has_tablegroup(const ObSimpleTableSchemaV2 *schema)
{
  return OB_NOT_NULL(schema) && OB_INVALID_ID != schema->get_tablegroup_id();
}

} // namespace

int ObSchemaCacheIncrementalRefresher::refresh_table(
    const uint64_t tenant_id,
    const ObSchemaCacheGenerations &cache_generations,
    const uint64_t table_id,
    const int64_t change_schema_version,
    const ObSimpleTableSchemaV2 *before_schema,
    const ObNameCaseMode before_case_mode,
    const ObSimpleTableSchemaV2 *after_schema,
    const ObNameCaseMode after_case_mode)
{
  int ret = OB_SUCCESS;
  ObSchemaNameResolveKey before_name_key;
  ObSchemaNameResolveKey after_name_key;
  bool has_before_name_key = false;
  bool has_after_name_key = false;
  ObArenaAllocator allocator("SchCacheRefresh");
  if (OB_INVALID_TENANT_ID == tenant_id
      || !cache_generations.is_valid()
      || OB_INVALID_ID == table_id
      || change_schema_version <= 0
      || (OB_ISNULL(before_schema) && OB_ISNULL(after_schema))
      || (OB_NOT_NULL(before_schema)
          && (before_schema->get_tenant_id() != tenant_id
              || before_schema->get_table_id() != table_id))
      || (OB_NOT_NULL(after_schema)
          && (after_schema->get_tenant_id() != tenant_id
              || after_schema->get_table_id() != table_id
              || after_schema->get_schema_version() <= 0
              || after_schema->get_schema_version() > change_schema_version))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid incremental schema cache refresh argument", KR(ret), K(tenant_id),
             K(cache_generations), K(table_id), K(change_schema_version),
             KP(before_schema), KP(after_schema));
  } else if (OB_NOT_NULL(before_schema)
             && OB_FAIL(build_name_resolve_key(*before_schema, before_case_mode,
                                               cache_generations.name_resolve_, allocator,
                                               before_name_key))) {
    LOG_WARN("fail to build before name key", KR(ret), KPC(before_schema));
  } else if (OB_NOT_NULL(before_schema)) {
    has_before_name_key = true;
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(after_schema)) {
    if (OB_FAIL(build_name_resolve_key(*after_schema, after_case_mode,
                                       cache_generations.name_resolve_, allocator,
                                       after_name_key))) {
      LOG_WARN("fail to build after name key", KR(ret), KPC(after_schema));
    } else {
      has_after_name_key = true;
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaIDResolveKey id_key(
        tenant_id, cache_generations.id_resolve_, table_id);
    const bool is_exist = OB_NOT_NULL(after_schema);
    const int64_t precise_version = is_exist
        ? after_schema->get_schema_version() : OB_INVALID_VERSION;
    ObSchemaResolveRange id_range(change_schema_version, OB_SCHEMA_VERSION_INF,
                                  is_exist ? table_id : OB_INVALID_ID,
                                  precise_version, 0, NS_MAX, is_exist);
    if (OB_FAIL(resolve_cache_.refresh(id_key, id_range))) {
      LOG_WARN("fail to refresh table id resolve key", KR(ret), K(id_key), K(id_range));
    }
  }

  const bool same_name_key = has_before_name_key && has_after_name_key
      && before_name_key == after_name_key;
  if (OB_SUCC(ret) && has_before_name_key && !same_name_key) {
    ObSchemaResolveRange negative_name_range(
        change_schema_version, OB_SCHEMA_VERSION_INF,
        OB_INVALID_ID, OB_INVALID_VERSION, 0,
        before_name_key.table_namespace_, false);
    if (OB_FAIL(resolve_cache_.refresh(before_name_key, negative_name_range))) {
      LOG_WARN("fail to close old table name resolve key",
               KR(ret), K(before_name_key), K(negative_name_range));
    }
  }
  if (OB_SUCC(ret) && has_after_name_key) {
    ObSchemaResolveRange positive_name_range(
        change_schema_version, OB_SCHEMA_VERSION_INF,
        table_id, after_schema->get_schema_version(), 0,
        after_name_key.table_namespace_, true);
    if (OB_FAIL(resolve_cache_.refresh(after_name_key, positive_name_range))) {
      LOG_WARN("fail to refresh current table name resolve key",
               KR(ret), K(after_name_key), K(positive_name_range));
    }
  }

  const uint64_t before_tablegroup_id = has_tablegroup(before_schema)
      ? before_schema->get_tablegroup_id() : OB_INVALID_ID;
  const uint64_t after_tablegroup_id = has_tablegroup(after_schema)
      ? after_schema->get_tablegroup_id() : OB_INVALID_ID;
  if (OB_SUCC(ret) && before_tablegroup_id != after_tablegroup_id) {
    if (OB_INVALID_ID != before_tablegroup_id
        && OB_FAIL(tablegroup_cache_.erase(tenant_id, cache_generations.tablegroup_,
                                           before_tablegroup_id))) {
      LOG_WARN("fail to erase old tablegroup member cache", KR(ret), K(tenant_id),
               K(cache_generations), K(before_tablegroup_id));
    } else if (OB_INVALID_ID != after_tablegroup_id
               && OB_FAIL(tablegroup_cache_.erase(tenant_id, cache_generations.tablegroup_,
                                                  after_tablegroup_id))) {
      LOG_WARN("fail to erase new tablegroup member cache", KR(ret), K(tenant_id),
               K(cache_generations), K(after_tablegroup_id));
    }
  }
  return ret;
}

int ObSchemaCacheIncrementalRefresher::finalize_table_names_at_boundary(
    const uint64_t tenant_id,
    const ObSchemaCacheGenerations &cache_generations,
    const int64_t change_schema_version,
    const ObIArray<const ObSimpleTableSchemaV2 *> &after_schemas,
    const ObNameCaseMode after_case_mode)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SchNameFinalize");
  if (OB_INVALID_TENANT_ID == tenant_id
      || !cache_generations.is_valid()
      || change_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema name boundary finalization argument", KR(ret),
             K(tenant_id), K(cache_generations), K(change_schema_version));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < after_schemas.count(); ++i) {
    const ObSimpleTableSchemaV2 *after_schema = after_schemas.at(i);
    if (OB_ISNULL(after_schema)) {
      // A dropped object has no final positive name binding.
    } else if (after_schema->get_tenant_id() != tenant_id
               || after_schema->get_schema_version() <= 0
               || after_schema->get_schema_version() > change_schema_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid schema in name boundary finalization", KR(ret), K(i),
               K(tenant_id), K(change_schema_version), KPC(after_schema));
    } else {
      ObSchemaNameResolveKey after_name_key;
      if (OB_FAIL(build_name_resolve_key(
              *after_schema, after_case_mode,
              cache_generations.name_resolve_, allocator, after_name_key))) {
        LOG_WARN("fail to build final name resolve key", KR(ret), K(i),
                 KPC(after_schema));
      } else {
        ObSchemaResolveRange positive_name_range(
            change_schema_version, OB_SCHEMA_VERSION_INF,
            after_schema->get_table_id(), after_schema->get_schema_version(), 0,
            after_name_key.table_namespace_, true);
        if (OB_FAIL(resolve_cache_.refresh(after_name_key, positive_name_range))) {
          LOG_WARN("fail to publish final table name binding", KR(ret), K(i),
                   K(after_name_key), K(positive_name_range));
        }
      }
    }
  }
  return ret;
}

ObSimpleSchemaCacheKey::ObSimpleSchemaCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    table_id_(OB_INVALID_ID),
    precise_schema_version_(OB_INVALID_VERSION)
{
}

ObSimpleSchemaCacheKey::ObSimpleSchemaCacheKey(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t precise_schema_version)
  : tenant_id_(tenant_id),
    table_id_(table_id),
    precise_schema_version_(precise_schema_version)
{
}

bool ObSimpleSchemaCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != tenant_id_
      && OB_INVALID_ID != table_id_
      && precise_schema_version_ >= 0;
}

bool ObSimpleSchemaCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObSimpleSchemaCacheKey &other_key =
      reinterpret_cast<const ObSimpleSchemaCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && table_id_ == other_key.table_id_
      && precise_schema_version_ == other_key.precise_schema_version_;
}

uint64_t ObSimpleSchemaCacheKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&table_id_, sizeof(table_id_), hash_value);
  hash_value = murmurhash(&precise_schema_version_, sizeof(precise_schema_version_), hash_value);
  return hash_value;
}

int ObSimpleSchemaCacheKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  key = NULL;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    key = new (buf) ObSimpleSchemaCacheKey(*this);
  }
  return ret;
}

ObSimpleSchemaCacheValue::ObSimpleSchemaCacheValue()
  : schema_(NULL)
{
}

ObSimpleSchemaCacheValue::ObSimpleSchemaCacheValue(
    const ObSimpleTableSchemaV2 *schema)
  : schema_(schema)
{
}

int64_t ObSimpleSchemaCacheValue::size() const
{
  return sizeof(*this)
      + (OB_ISNULL(schema_) ? 0 : schema_->get_convert_size())
      + sizeof(ObDataBuffer);
}

int ObSimpleSchemaCacheValue::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  value = NULL;
  if (OB_ISNULL(schema_) || OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSimpleSchemaCacheValue *new_value = new (buf) ObSimpleSchemaCacheValue();
    ObSimpleTableSchemaV2 *new_schema = NULL;
    if (OB_FAIL(ObSchemaUtils::deep_copy_schema(
        buf + sizeof(ObSimpleSchemaCacheValue), *schema_, new_schema))) {
      LOG_WARN("failed to deep copy simple schema cache value", KR(ret), KPC(schema_));
    } else {
      new_value->schema_ = new_schema;
      value = new_value;
    }
  }
  return ret;
}

int ObSimpleSchemaObjectCache::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(cache_.init(OB_SIMPLE_SCHEMA_OBJECT_CACHE_NAME, 5))) {
    LOG_WARN("failed to init simple schema object cache", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObSimpleSchemaObjectCache::destroy()
{
  if (inited_) {
    cache_.destroy();
    inited_ = false;
  }
}

int ObSimpleSchemaObjectCache::validate_(
    const ObSimpleSchemaCacheKey &key,
    const ObSimpleSchemaCacheValue *value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value) || OB_ISNULL(value->schema_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (key.tenant_id_ != value->schema_->get_tenant_id()
             || key.table_id_ != value->schema_->get_table_id()
             || key.precise_schema_version_ != value->schema_->get_schema_version()) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("simple schema object cache key does not match content",
              KR(ret), K(key), KPC(value->schema_));
  }
  return ret;
}

int ObSimpleSchemaObjectCache::get(
    const ObSimpleSchemaCacheKey &key,
    const ObSimpleTableSchemaV2 *&schema,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObSimpleSchemaCacheValue *value = NULL;
  schema = NULL;
  handle.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(cache_.get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      EVENT_INC(ObStatEventIds::SIMPLE_SCHEMA_OBJECT_CACHE_MISS);
    } else {
      LOG_WARN("failed to get simple schema object cache", KR(ret), K(key));
    }
  } else {
    EVENT_INC(ObStatEventIds::SIMPLE_SCHEMA_OBJECT_CACHE_HIT);
    if (OB_FAIL(validate_(key, value))) {
      LOG_ERROR("invalid simple schema object cache value", KR(ret), K(key), KP(value));
    } else {
      schema = value->schema_;
    }
  }
  return ret;
}

int ObSimpleSchemaObjectCache::put_and_fetch(
    const ObSimpleSchemaCacheKey &key,
    const ObSimpleTableSchemaV2 &schema,
    const ObSimpleTableSchemaV2 *&cached_schema,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObSimpleSchemaCacheValue *value = NULL;
  cached_schema = NULL;
  handle.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()
             || key.tenant_id_ != schema.get_tenant_id()
             || key.table_id_ != schema.get_table_id()
             || key.precise_schema_version_ != schema.get_schema_version()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSimpleSchemaCacheValue input(&schema);
    ret = cache_.put_and_fetch(key, input, value, handle, false);
    if (OB_ENTRY_EXIST == ret) {
      ret = cache_.get(key, value, handle);
    }
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to publish simple schema object cache", KR(ret), K(key));
      }
    } else if (OB_FAIL(validate_(key, value))) {
      LOG_ERROR("invalid published simple schema object", KR(ret), K(key), KP(value));
    } else {
      cached_schema = value->schema_;
    }
  }
  return ret;
}

int ObSimpleSchemaObjectCache::erase(const ObSimpleSchemaCacheKey &key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(cache_.erase(key)) && OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObSchemaLoadKey::ObSchemaLoadKey()
  : kind_(LOAD_KIND_MAX),
    tenant_id_(OB_INVALID_TENANT_ID),
    cache_generation_(0),
    physical_key_hash_(0)
{
}

ObSchemaLoadKey::ObSchemaLoadKey(const ObSimpleSchemaCacheKey &key)
  : kind_(key.is_valid() ? LOAD_SIMPLE_OBJECT : LOAD_KIND_MAX),
    tenant_id_(key.tenant_id_),
    cache_generation_(0),
    physical_key_hash_(key.hash())
{
}

ObSchemaLoadKey::ObSchemaLoadKey(
    const ObSchemaLoadKind kind,
    const uint64_t tenant_id,
    const uint64_t cache_generation,
    const uint64_t physical_key_hash)
  : kind_(kind),
    tenant_id_(tenant_id),
    cache_generation_(cache_generation),
    physical_key_hash_(physical_key_hash)
{
}

bool ObSchemaLoadKey::is_valid() const
{
  return kind_ < LOAD_KIND_MAX
      && OB_INVALID_TENANT_ID != tenant_id_
      && OB_INVALID_ID != tenant_id_
      && (LOAD_SIMPLE_OBJECT == kind_ ? 0 == cache_generation_
                                      : cache_generation_ > 0);
}

bool ObSchemaLoadKey::operator==(const ObSchemaLoadKey &other) const
{
  return kind_ == other.kind_
      && tenant_id_ == other.tenant_id_
      && cache_generation_ == other.cache_generation_
      && physical_key_hash_ == other.physical_key_hash_;
}

uint64_t ObSchemaLoadKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&kind_, sizeof(kind_), hash_value);
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&cache_generation_, sizeof(cache_generation_), hash_value);
  hash_value = murmurhash(&physical_key_hash_, sizeof(physical_key_hash_), hash_value);
  return hash_value;
}

ObSchemaLoadCoordinator::ObSchemaLoadCoordinator()
  : cond_slots_(),
    loading_flights_(),
    registry_lock_(ObLatchIds::OB_SCHEMA_GETTER_LOCK),
    flight_seq_(0),
    inited_(false)
{
}

ObSchemaLoadCoordinator::~ObSchemaLoadCoordinator()
{
  destroy();
}

int ObSchemaLoadCoordinator::init()
{
  int ret = OB_SUCCESS;
  int64_t inited_cond_count = 0;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(loading_flights_.create(FLIGHT_BUCKET_COUNT,
                                             "SchLoadBucket",
                                             "SchLoadNode"))) {
    LOG_WARN("failed to create schema load registry", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < COND_SLOT_COUNT; ++i) {
      if (OB_FAIL(cond_slots_[i].init(ObWaitEventIds::SCHEMA_GETTER_COND_WAIT))) {
        LOG_WARN("failed to init schema load condition", KR(ret), K(i));
      } else {
        ++inited_cond_count;
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    } else {
      for (int64_t i = 0; i < inited_cond_count; ++i) {
        cond_slots_[i].destroy();
      }
      loading_flights_.destroy();
    }
  }
  return ret;
}

void ObSchemaLoadCoordinator::destroy()
{
  if (inited_) {
    for (int64_t i = 0; i < COND_SLOT_COUNT; ++i) {
      cond_slots_[i].destroy();
    }
    loading_flights_.destroy();
    flight_seq_ = 0;
    inited_ = false;
  }
}

int ObSchemaLoadCoordinator::try_acquire(
    const ObSchemaLoadKey &key,
    bool &is_leader,
    uint64_t &flight_id)
{
  int ret = OB_SUCCESS;
  is_leader = false;
  flight_id = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSpinLockGuard guard(registry_lock_);
    uint64_t current_flight_id = 0;
    const int get_ret = loading_flights_.get_refactored(key, current_flight_id);
    if (OB_HASH_NOT_EXIST == get_ret) {
      if (UINT64_MAX == flight_seq_) {
        ret = OB_SIZE_OVERFLOW;
        LOG_ERROR("schema load flight sequence is exhausted", KR(ret), K(key));
      } else {
        const uint64_t new_flight_id = ++flight_seq_;
        if (OB_FAIL(loading_flights_.set_refactored(key, new_flight_id, 0))) {
          LOG_WARN("failed to register schema load", KR(ret), K(key), K(new_flight_id));
        } else {
          is_leader = true;
          flight_id = new_flight_id;
        }
      }
    } else if (OB_SUCCESS == get_ret) {
      if (0 == current_flight_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid schema load flight id", KR(ret), K(key), K(current_flight_id));
      } else {
        flight_id = current_flight_id;
      }
    } else {
      ret = get_ret;
      LOG_WARN("failed to check schema load registry", KR(ret), K(key));
    }
  }
  return ret;
}

int ObSchemaLoadCoordinator::is_active_(
    const ObSchemaLoadKey &key,
    const uint64_t flight_id,
    bool &is_active)
{
  int ret = OB_SUCCESS;
  is_active = false;
  ObSpinLockGuard guard(registry_lock_);
  uint64_t current_flight_id = 0;
  const int get_ret = loading_flights_.get_refactored(key, current_flight_id);
  if (OB_SUCCESS == get_ret) {
    is_active = (flight_id == current_flight_id);
  } else if (OB_HASH_NOT_EXIST != get_ret) {
    ret = get_ret;
  }
  return ret;
}

int ObSchemaLoadCoordinator::wait(
    const ObSchemaLoadKey &key,
    const uint64_t flight_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || 0 == flight_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCond &cond = cond_slots_[key.hash() % COND_SLOT_COUNT];
    ObThreadCondGuard guard(cond);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_UNLIKELY(ERRSIM_ON_DEMAND_LOAD_WAIT_TIMEOUT)) {
      ret = OB_TIMEOUT;
      LOG_WARN("[ERRSIM] inject on-demand load wait timeout", KR(ret), K(key));
    } else {
      bool is_active = true;
      while (OB_SUCC(ret) && is_active) {
        if (OB_FAIL(is_active_(key, flight_id, is_active))) {
          LOG_WARN("failed to recheck schema load before wait",
                   KR(ret), K(key), K(flight_id));
        } else if (is_active) {
          const int64_t now = ObTimeUtility::current_time();
          if (INT64_MAX != abs_timeout_us && now >= abs_timeout_us) {
            ret = OB_TIMEOUT;
          } else {
            const uint64_t wait_us = INT64_MAX == abs_timeout_us
                ? 500 * 1000
                : static_cast<uint64_t>(MAX(static_cast<int64_t>(1),
                                            abs_timeout_us - now));
            const int wait_ret = cond.wait_us(wait_us);
            if (OB_TIMEOUT == wait_ret && INT64_MAX == abs_timeout_us) {
              // Periodically recheck the predicate for callers without a deadline.
            } else if (OB_SUCCESS != wait_ret) {
              ret = wait_ret;
              if (OB_TIMEOUT != ret) {
                LOG_WARN("failed to wait for schema load",
                         KR(ret), K(key), K(flight_id));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaLoadCoordinator::finish(
    const ObSchemaLoadKey &key,
    const uint64_t flight_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!key.is_valid() || 0 == flight_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObThreadCond &cond = cond_slots_[key.hash() % COND_SLOT_COUNT];
    ObThreadCondGuard cond_guard(cond);
    if (OB_SUCCESS != cond_guard.get_ret()) {
      ret = cond_guard.get_ret();
      // Do not leave this exact flight behind if the condition mutex itself is
      // broken.  Identity matching prevents a cleanup retry from erasing a
      // replacement flight for the same physical key.
      ObSpinLockGuard registry_guard(registry_lock_);
      uint64_t current_flight_id = 0;
      const int get_ret = loading_flights_.get_refactored(key, current_flight_id);
      if (OB_SUCCESS == get_ret && flight_id == current_flight_id) {
        const int erase_ret = loading_flights_.erase_refactored(key);
        if (OB_SUCCESS != erase_ret && OB_HASH_NOT_EXIST != erase_ret) {
          LOG_ERROR("failed emergency erase of schema load flight",
                    KR(erase_ret), KR(ret), K(key), K(flight_id));
        }
      } else if (OB_SUCCESS != get_ret && OB_HASH_NOT_EXIST != get_ret) {
        LOG_ERROR("failed to inspect schema load flight during emergency cleanup",
                  KR(get_ret), KR(ret), K(key), K(flight_id));
      }
    } else {
      {
        ObSpinLockGuard registry_guard(registry_lock_);
        uint64_t current_flight_id = 0;
        const int get_ret = loading_flights_.get_refactored(key, current_flight_id);
        if (OB_SUCCESS == get_ret && flight_id == current_flight_id) {
          ret = loading_flights_.erase_refactored(key);
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        } else if (OB_HASH_NOT_EXIST == get_ret
                   || (OB_SUCCESS == get_ret && flight_id != current_flight_id)) {
          // Idempotent completion and stale retries are both successful no-ops.
        } else {
          ret = get_ret;
        }
      }
      // Always wake the shared slot.  Waiters recheck both key and flight_id,
      // so this is harmless for unrelated or replacement flights.
      const int broadcast_ret = cond.broadcast();
      if (OB_SUCCESS != broadcast_ret && OB_SUCC(ret)) {
        ret = broadcast_ret;
      }
    }
  }
  return ret;
}

ObSchemaLoadGuard::~ObSchemaLoadGuard()
{
  reset();
}

int ObSchemaLoadGuard::acquire(
    ObSchemaLoadCoordinator &coordinator,
    const ObSchemaLoadKey &key,
    bool &is_leader,
    uint64_t &flight_id)
{
  int ret = OB_SUCCESS;
  is_leader = false;
  flight_id = 0;
  if (leader_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(coordinator.try_acquire(key, is_leader, flight_id))) {
    LOG_WARN("failed to acquire schema load flight", KR(ret), K(key));
  } else if (is_leader) {
    coordinator_ = &coordinator;
    key_ = key;
    flight_id_ = flight_id;
    leader_ = true;
  }
  return ret;
}

void ObSchemaLoadGuard::reset()
{
  if (leader_ && OB_NOT_NULL(coordinator_)) {
    static const int64_t MAX_FINISH_RETRY_COUNT = 3;
    int tmp_ret = OB_SUCCESS;
    int64_t retry_count = 0;
    do {
      tmp_ret = coordinator_->finish(key_, flight_id_);
      ++retry_count;
    } while (OB_SUCCESS != tmp_ret && retry_count < MAX_FINISH_RETRY_COUNT);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "failed to finish schema load flight",
                   K(tmp_ret), K(key_), K(flight_id_));
    }
  }
  coordinator_ = NULL;
  key_ = ObSchemaLoadKey();
  flight_id_ = 0;
  leader_ = false;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
