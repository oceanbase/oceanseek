/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_TABLE_ID_CACHE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_TABLE_ID_CACHE_H_

#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_mutex.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObTenantSchemaPublishState;
struct ObSchemaPublishSnapshot;

/**
 * B-class full-scan cache: tablegroup_id -> table_id list.
 * Key has no schema_version (latest-only).  cache_generation isolates refresh
 * epochs so an in-flight old loader cannot repopulate the active cache.
 */
class ObTablegroupTableIdCacheKey : public common::ObIKVCacheKey
{
public:
  ObTablegroupTableIdCacheKey();
  ObTablegroupTableIdCacheKey(const uint64_t tenant_id,
                              const uint64_t cache_generation,
                              const uint64_t tablegroup_id);
  virtual ~ObTablegroupTableIdCacheKey() {}

  int init(const uint64_t tenant_id,
           const uint64_t cache_generation,
           const uint64_t tablegroup_id);
  bool is_valid() const;
  virtual bool operator==(const ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int hash(uint64_t &hash_value) const override
  {
    hash_value = hash();
    return common::OB_SUCCESS;
  }
  virtual int64_t size() const override;
  // The KVCache instance and its memory are owned by tenant 500. Keep the
  // business tenant embedded in the key to isolate tenants.
  virtual uint64_t get_tenant_id() const override { return OB_SERVER_TENANT_ID; }
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheKey *&key) const override;

  uint64_t get_tablegroup_id() const { return tablegroup_id_; }

  TO_STRING_KV(K_(tenant_id), K_(cache_generation), K_(tablegroup_id));

private:
  uint64_t tenant_id_;
  uint64_t cache_generation_;
  uint64_t tablegroup_id_;
};

class ObTablegroupTableIdCacheValue : public common::ObIKVCacheValue
{
public:
  ObTablegroupTableIdCacheValue();
  virtual ~ObTablegroupTableIdCacheValue() {}

  // For put(): table_ids may point at caller's temporary array; valid until put()'s deep_copy.
  int set(const common::ObIArray<uint64_t> &table_ids);
  int get_table_ids(common::ObIArray<uint64_t> &table_ids) const;

  int64_t get_count() const { return count_; }
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        ObIKVCacheValue *&value) const override;

  TO_STRING_KV(K_(count), KP_(table_ids));

private:
  int64_t count_;
  uint64_t *table_ids_;
};

class ObTablegroupTableIdCache
{
public:
  ObTablegroupTableIdCache();
  virtual ~ObTablegroupTableIdCache();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  // OB_ENTRY_NOT_EXIST on miss.
  int get(const uint64_t tenant_id,
          const uint64_t cache_generation,
          const uint64_t tablegroup_id,
          common::ObIArray<uint64_t> &table_ids);
  int put(const uint64_t tenant_id,
          const uint64_t cache_generation,
          const uint64_t tablegroup_id,
          const common::ObIArray<uint64_t> &table_ids);
  // Serialize the miss-loader publish with exact invalidation and reject a
  // stale publication token while holding the same key stripe.  This closes
  // the old-loader-put-after-refresh-erase window within one generation.
  int put_if_current(const uint64_t tenant_id,
                     const uint64_t cache_generation,
                     const uint64_t tablegroup_id,
                     const common::ObIArray<uint64_t> &table_ids,
                     const ObTenantSchemaPublishState &state,
                     const ObSchemaPublishSnapshot &token);
  // Exact invalidation used by incremental schema publication.
  int erase(const uint64_t tenant_id,
            const uint64_t cache_generation,
            const uint64_t tablegroup_id);

private:
  struct WriteLock
  {
    WriteLock() : mutex_(common::ObLatchIds::OB_SCHEMA_GETTER_LOCK) {}
    lib::ObMutex mutex_;
  };
  static const int64_t WRITE_LOCK_COUNT = 256;
  typedef common::ObKVCache<ObTablegroupTableIdCacheKey, ObTablegroupTableIdCacheValue> KVCache;
  WriteLock &lock_for_(const uint64_t hash)
  { return write_locks_[hash % WRITE_LOCK_COUNT]; }
  int put_(const ObTablegroupTableIdCacheKey &key,
           const common::ObIArray<uint64_t> &table_ids);
  KVCache cache_;
  WriteLock write_locks_[WRITE_LOCK_COUNT];
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTablegroupTableIdCache);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_TABLE_ID_CACHE_H_
