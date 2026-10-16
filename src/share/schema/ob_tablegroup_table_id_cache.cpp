/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_tablegroup_table_id_cache.h"
#include "share/schema/ob_simple_schema_kv_cache.h"
#include "share/cache/ob_cache_name_define.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ObTablegroupTableIdCacheKey::ObTablegroupTableIdCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cache_generation_(0),
    tablegroup_id_(OB_INVALID_ID)
{
}

ObTablegroupTableIdCacheKey::ObTablegroupTableIdCacheKey(const uint64_t tenant_id,
                                                         const uint64_t cache_generation,
                                                         const uint64_t tablegroup_id)
  : tenant_id_(tenant_id),
    cache_generation_(cache_generation),
    tablegroup_id_(tablegroup_id)
{
}

int ObTablegroupTableIdCacheKey::init(const uint64_t tenant_id,
                                      const uint64_t cache_generation,
                                      const uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || 0 == cache_generation
                  || OB_INVALID_ID == tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_generation), K(tablegroup_id));
  } else {
    tenant_id_ = tenant_id;
    cache_generation_ = cache_generation;
    tablegroup_id_ = tablegroup_id;
  }
  return ret;
}

bool ObTablegroupTableIdCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && cache_generation_ > 0
      && OB_INVALID_ID != tablegroup_id_;
}

bool ObTablegroupTableIdCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObTablegroupTableIdCacheKey &other_key =
      reinterpret_cast<const ObTablegroupTableIdCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_
      && cache_generation_ == other_key.cache_generation_
      && tablegroup_id_ == other_key.tablegroup_id_;
}

uint64_t ObTablegroupTableIdCacheKey::hash() const
{
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
  hash_code = murmurhash(&cache_generation_, sizeof(cache_generation_), hash_code);
  hash_code = murmurhash(&tablegroup_id_, sizeof(tablegroup_id_), hash_code);
  return hash_code;
}

int64_t ObTablegroupTableIdCacheKey::size() const
{
  return sizeof(*this);
}

int ObTablegroupTableIdCacheKey::deep_copy(char *buf,
                                           const int64_t buf_len,
                                           ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  key = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(size()));
  } else {
    ObTablegroupTableIdCacheKey *new_key = new (buf) ObTablegroupTableIdCacheKey();
    if (OB_FAIL(new_key->init(tenant_id_, cache_generation_, tablegroup_id_))) {
      LOG_WARN("fail to init key", KR(ret), KPC(this));
    } else {
      key = new_key;
    }
  }
  return ret;
}

ObTablegroupTableIdCacheValue::ObTablegroupTableIdCacheValue()
  : count_(0),
    table_ids_(NULL)
{
}

int ObTablegroupTableIdCacheValue::set(const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  count_ = table_ids.count();
  if (0 == count_) {
    table_ids_ = NULL;
  } else {
    // Contiguous storage of ObArray / ObSEArray; pointer only valid until put()'s deep_copy.
    table_ids_ = const_cast<uint64_t *>(&table_ids.at(0));
  }
  return ret;
}

int ObTablegroupTableIdCacheValue::get_table_ids(ObIArray<uint64_t> &table_ids) const
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (OB_FAIL(table_ids.reserve(count_))) {
    LOG_WARN("fail to reserve", KR(ret), K_(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(table_ids_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_ids_ is null", KR(ret), K_(count));
      } else if (OB_FAIL(table_ids.push_back(table_ids_[i]))) {
        LOG_WARN("fail to push table_id", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int64_t ObTablegroupTableIdCacheValue::size() const
{
  return static_cast<int64_t>(sizeof(*this) + sizeof(uint64_t) * count_);
}

int ObTablegroupTableIdCacheValue::deep_copy(char *buf,
                                             const int64_t buf_len,
                                             ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  value = NULL;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(size()));
  } else if (count_ > 0 && OB_ISNULL(table_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_ids_ is null", KR(ret), K_(count));
  } else {
    ObTablegroupTableIdCacheValue *new_value = new (buf) ObTablegroupTableIdCacheValue();
    new_value->count_ = count_;
    if (0 == count_) {
      new_value->table_ids_ = NULL;
    } else {
      new_value->table_ids_ = reinterpret_cast<uint64_t *>(buf + sizeof(ObTablegroupTableIdCacheValue));
      MEMCPY(new_value->table_ids_, table_ids_, sizeof(uint64_t) * count_);
    }
    value = new_value;
  }
  return ret;
}

ObTablegroupTableIdCache::ObTablegroupTableIdCache()
  : cache_(),
    is_inited_(false)
{
}

ObTablegroupTableIdCache::~ObTablegroupTableIdCache()
{
  destroy();
}

int ObTablegroupTableIdCache::init()
{
  int ret = OB_SUCCESS;
  const int64_t priority = 1;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(cache_.init(OB_TABLEGROUP_TABLE_ID_CACHE_NAME, priority))) {
    LOG_WARN("fail to init tablegroup table id cache", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTablegroupTableIdCache::destroy()
{
  if (is_inited_) {
    cache_.destroy();
    is_inited_ = false;
  }
}

int ObTablegroupTableIdCache::get(const uint64_t tenant_id,
                                  const uint64_t cache_generation,
                                  const uint64_t tablegroup_id,
                                  ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObTablegroupTableIdCacheValue *value = NULL;
  ObTablegroupTableIdCacheKey key;
  table_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(key.init(tenant_id, cache_generation, tablegroup_id))) {
    LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(cache_generation), K(tablegroup_id));
  } else if (OB_FAIL(cache_.get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      EVENT_INC(ObStatEventIds::TABLEGROUP_TABLE_ID_CACHE_MISS);
    } else {
      LOG_WARN("fail to get from cache", KR(ret), K(key));
    }
  } else {
    EVENT_INC(ObStatEventIds::TABLEGROUP_TABLE_ID_CACHE_HIT);
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", KR(ret), K(key));
    } else if (OB_FAIL(value->get_table_ids(table_ids))) {
      LOG_WARN("fail to get table ids from value", KR(ret), K(key));
    }
  }
  LOG_TRACE("get tablegroup table id cache", KR(ret), K(key), "cnt", table_ids.count());
  return ret;
}

int ObTablegroupTableIdCache::put(const uint64_t tenant_id,
                                  const uint64_t cache_generation,
                                  const uint64_t tablegroup_id,
                                  const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObTablegroupTableIdCacheKey key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(key.init(tenant_id, cache_generation, tablegroup_id))) {
    LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(cache_generation), K(tablegroup_id));
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(put_(key, table_ids))) {
      LOG_WARN("fail to put cache", KR(ret), K(key), "cnt", table_ids.count());
    }
  }
  LOG_TRACE("put tablegroup table id cache", KR(ret), K(key), "cnt", table_ids.count());
  return ret;
}

int ObTablegroupTableIdCache::put_if_current(
    const uint64_t tenant_id,
    const uint64_t cache_generation,
    const uint64_t tablegroup_id,
    const ObIArray<uint64_t> &table_ids,
    const ObTenantSchemaPublishState &state,
    const ObSchemaPublishSnapshot &token)
{
  int ret = OB_SUCCESS;
  ObTablegroupTableIdCacheKey key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(key.init(tenant_id, cache_generation, tablegroup_id))) {
    LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(cache_generation), K(tablegroup_id));
  } else if (tenant_id != token.tenant_id_
             || cache_generation != token.cache_generations_.tablegroup_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("publication token does not match tablegroup cache key",
             KR(ret), K(key), K(token));
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (!state.can_publish(token)) {
      ret = OB_SCHEMA_EAGAIN;
    } else if (OB_FAIL(put_(key, table_ids))) {
      LOG_WARN("fail to conditionally put tablegroup cache",
               KR(ret), K(key), "cnt", table_ids.count());
    }
  }
  return ret;
}

int ObTablegroupTableIdCache::put_(
    const ObTablegroupTableIdCacheKey &key,
    const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObTablegroupTableIdCacheValue value;
  if (OB_FAIL(value.set(table_ids))) {
    LOG_WARN("fail to set value", KR(ret), K(key));
  } else if (OB_FAIL(cache_.put(key, value))) {
    LOG_WARN("fail to put cache", KR(ret), K(key), "cnt", table_ids.count());
  }
  return ret;
}

int ObTablegroupTableIdCache::erase(const uint64_t tenant_id,
                                    const uint64_t cache_generation,
                                    const uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  ObTablegroupTableIdCacheKey key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(key.init(tenant_id, cache_generation, tablegroup_id))) {
    LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(cache_generation), K(tablegroup_id));
  } else {
    lib::ObMutexGuard guard(lock_for_(key.hash()).mutex_);
    if (OB_SUCCESS != guard.get_ret()) {
      ret = guard.get_ret();
    } else if (OB_FAIL(cache_.erase(key)) && OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  LOG_TRACE("erase exact tablegroup table id cache", KR(ret), K(key));
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
