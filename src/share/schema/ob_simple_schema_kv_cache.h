/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SIMPLE_SCHEMA_KV_CACHE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SIMPLE_SCHEMA_KV_CACHE_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/schema/ob_schema_cache_types.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

static const int64_t OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT = 16;
static const int64_t OB_SCHEMA_VERSION_INF = INT64_MAX;
static const uint16_t OB_SCHEMA_RESOLVE_CACHE_FORMAT_VERSION = 1;

enum ObTableNameNamespace : uint8_t
{
  NS_NORMAL_TABLE = 0,
  NS_HIDDEN_TABLE,
  NS_NORMAL_INDEX,
  NS_BUILT_IN_INDEX,
  NS_AUX_VP,
  NS_MAX
};

class ObSchemaIDResolveKey final : public common::ObIKVCacheKey
{
public:
  ObSchemaIDResolveKey();
  ObSchemaIDResolveKey(uint64_t tenant_id, uint64_t cache_generation, uint64_t schema_id);
  virtual ~ObSchemaIDResolveKey() {}
  bool is_valid() const;
  // The KVCache instance and its memory are owned by tenant 500. tenant_id_
  // remains embedded in the key to isolate business tenants.
  virtual uint64_t get_tenant_id() const override { return OB_SERVER_TENANT_ID; }
  virtual bool operator==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, int64_t buf_len,
                        common::ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(cache_generation), K_(schema_id));

  uint64_t tenant_id_;
  uint64_t cache_generation_;
  uint64_t schema_id_;
};

// Name resolve cache key for the on-demand schema cache.
//
// The key keeps the requested name and the name case mode, and mirrors the
// ORIGINAL SchemaMgr name map contract (ObTableSchemaHashWrapper): equality is
// decided by ObCharset::strcmp on the collation selected by the case mode, and
// hash() uses ObCharset::hash on the same collation. Deriving a canonical byte
// string (lowercase or sort key) is not equivalent: lowercased bytes disagree
// with general_ci for U+212A, and a sort key cannot stay equivalent for invalid
// UTF-8 input.
class ObSchemaNameResolveKey final : public common::ObIKVCacheKey
{
public:
  ObSchemaNameResolveKey();
  ObSchemaNameResolveKey(uint64_t tenant_id,
                         uint64_t cache_generation,
                         uint64_t database_id,
                         uint64_t exact_session_id,
                         ObTableNameNamespace table_namespace,
                         const common::ObNameCaseMode name_case_mode,
                         const common::ObString &lookup_name);
  virtual ~ObSchemaNameResolveKey() {}
  bool is_valid() const;
  // The KVCache instance and its memory are owned by tenant 500. tenant_id_
  // remains embedded in the key to isolate business tenants.
  virtual uint64_t get_tenant_id() const override { return OB_SERVER_TENANT_ID; }
  virtual bool operator==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, int64_t buf_len,
                        common::ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(cache_generation), K_(database_id),
               K_(exact_session_id), K_(table_namespace), K_(name_case_mode),
               K_(lookup_name));

  uint64_t tenant_id_;
  uint64_t cache_generation_;
  uint64_t database_id_;
  uint64_t exact_session_id_;
  ObTableNameNamespace table_namespace_;
  // The requested name is kept verbatim. Equality and hash follow the same
  // collation based contract as the ORIGINAL SchemaMgr name map
  // (ObTableSchemaHashWrapper): case mode selects utf8mb4_general_ci or
  // utf8mb4_bin, so keys never disagree with ObCharset::strcmp, including for
  // invalid UTF-8 input.
  common::ObNameCaseMode name_case_mode_;
  common::ObString lookup_name_;
};

struct ObSchemaResolveRange
{
  ObSchemaResolveRange();
  ObSchemaResolveRange(int64_t start_version,
                       int64_t end_version,
                       uint64_t schema_id,
                       int64_t precise_schema_version,
                       uint64_t admission_seq,
                       ObTableNameNamespace table_namespace,
                       bool is_exist);
  bool is_valid() const;
  bool contains(int64_t snapshot_version) const;
  bool same_result(const ObSchemaResolveRange &other) const;
  void reset();
  TO_STRING_KV(K_(start_version), K_(end_version), K_(schema_id),
               K_(precise_schema_version), K_(admission_seq),
               K_(table_namespace), K_(is_exist));

  int64_t start_version_;
  int64_t end_version_;
  uint64_t schema_id_;
  int64_t precise_schema_version_;
  uint64_t admission_seq_;
  ObTableNameNamespace table_namespace_;
  bool is_exist_;
  uint8_t reserved_[6];
};

struct ObSchemaResolveResult
{
  ObSchemaResolveResult() { reset(); }
  void reset();
  void assign(const ObSchemaResolveRange &range);
  bool is_valid() const;
  TO_STRING_KV(K_(range_start), K_(range_end), K_(schema_id),
               K_(precise_schema_version), K_(table_namespace), K_(is_exist));

  int64_t range_start_;
  int64_t range_end_;
  uint64_t schema_id_;
  int64_t precise_schema_version_;
  ObTableNameNamespace table_namespace_;
  bool is_exist_;
};

class ObSchemaResolveCacheValue final : public common::ObIKVCacheValue
{
public:
  ObSchemaResolveCacheValue();
  ObSchemaResolveCacheValue(const ObSchemaResolveRange *ranges,
                            int64_t range_count,
                            uint64_t publish_refresh_seq = 0);
  virtual ~ObSchemaResolveCacheValue() {}
  bool is_valid() const;
  int find(int64_t snapshot_version, ObSchemaResolveResult &result) const;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, int64_t buf_len,
                        common::ObIKVCacheValue *&value) const override;
  TO_STRING_KV(K_(format_version), K_(range_count), K_(flags),
               K_(publish_refresh_seq), KP_(ranges));

  uint16_t format_version_;
  uint16_t range_count_;
  uint32_t flags_;
  uint64_t publish_refresh_seq_;
  const ObSchemaResolveRange *ranges_;
};

class ObSchemaResolveRangeSet
{
public:
  typedef common::ObSEArray<ObSchemaResolveRange,
                            OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT * 2 + 1> RangeArray;

  // authoritative_range replaces every overlapping part of current.  The result
  // is sorted, non-overlapping and capped at OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT.
  static int merge(const ObSchemaResolveCacheValue *current,
                   const ObSchemaResolveRange &authoritative_range,
                   RangeArray &result);
  static bool is_normalized(const common::ObIArray<ObSchemaResolveRange> &ranges);

private:
  static int normalize_and_evict_(const ObSchemaResolveRange &authoritative_range,
                                  RangeArray &ranges);
};

struct ObSchemaPublishSnapshot
{
  ObSchemaPublishSnapshot() { reset(); }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(refresh_seq), K_(cache_generations),
               K_(refreshed_version));

  uint64_t tenant_id_;
  uint64_t refresh_seq_;
  ObSchemaCacheGenerations cache_generations_;
  int64_t refreshed_version_;
};

// Incremental cache repair deliberately trades cache retention for a bounded
// schema-refresh critical section.  Streams without an explicit DDL boundary
// cannot be repaired correctly for historical guards, and a large backlog
// must fall back to a new generation instead of issuing one SQL per boundary.
struct ObSchemaCacheIncrementalRefreshPolicy
{
  static const int64_t MAX_OPERATION_COUNT = 4096;
  static const int64_t MAX_DDL_BOUNDARY_COUNT = 64;
  static const int64_t MAX_TARGETED_INVALIDATION_OPERATION_COUNT = 65536;
  // Bound publication work after deduplication and whole-domain invalidation.
  static const int64_t MAX_TARGETED_INVALIDATION_KEY_COUNT = 1024;

  static bool should_fallback(const int64_t operation_count,
                              const int64_t ddl_boundary_count,
                              const bool has_unterminated_ddl)
  {
    return operation_count > MAX_OPERATION_COUNT
        || ddl_boundary_count > MAX_DDL_BOUNDARY_COUNT
        || has_unterminated_ddl;
  }
};

struct ObOnDemandSchemaRetryPolicy
{
  static const int64_t DEFAULT_LOAD_TIMEOUT_US = 30L * 1000L * 1000L;
  static const int64_t MAX_STALE_RETRY_COUNT = 64;
  static const int64_t STALE_RETRY_BASE_US = 50;

  static int64_t get_load_deadline(bool worker_timeout_valid,
                                   int64_t worker_timeout_us,
                                   int64_t now_us);
  static int calc_backoff_info(int64_t abs_deadline_us,
                                 int64_t now_us,
                                 int64_t &retry_count,
                                 int64_t &backoff_us);
};

// Seqlock state for one tenant.  A single publication writer is enforced by
// publish_lock_; readers only use atomic stable reads.
class ObTenantSchemaPublishState
{
  friend class ObTenantSchemaPublicationGuard;
public:
  ObTenantSchemaPublishState();
  ~ObTenantSchemaPublishState() {}
  int init(uint64_t tenant_id, int64_t refreshed_version, uint64_t cache_generation = 1);
  int read_stable(ObSchemaPublishSnapshot &snapshot,
                  int64_t abs_timeout_us = INT64_MAX) const;
  int get_published_snapshot(int64_t snapshot_version,
                             int64_t abs_timeout_us,
                             ObSchemaPublishSnapshot &snapshot) const;
  bool can_publish(const ObSchemaPublishSnapshot &token) const;
  bool is_inited() const { return inited_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

private:
  int begin_publish_(ObSchemaPublishSnapshot &before);
  int bump_generation_(uint64_t domains);
  int finish_publish_(int64_t refreshed_version, bool commit);

  lib::ObMutex publish_lock_;
  uint64_t tenant_id_;
  uint64_t refresh_seq_;
  ObSchemaCacheGenerations cache_generations_;
  int64_t refreshed_version_;
  bool publishing_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSchemaPublishState);
};

// RAII closes an odd refresh_seq window on every exit.  commit() publishes the
// new frontier; destruction without commit keeps the old frontier.
class ObTenantSchemaPublicationGuard
{
public:
  ObTenantSchemaPublicationGuard();
  ~ObTenantSchemaPublicationGuard();
  int begin(ObTenantSchemaPublishState &state);
  int bump_generation(uint64_t domains = SCHEMA_CACHE_DOMAIN_ALL);
  int commit(int64_t refreshed_version);
  bool is_active() const { return active_; }
  const ObSchemaPublishSnapshot &before() const { return before_; }

private:
  ObTenantSchemaPublishState *state_;
  ObSchemaPublishSnapshot before_;
  bool active_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSchemaPublicationGuard);
};

// One schema refresh keeps readers on the last complete snapshot while SQL
// preparation (including resident core/sys publication) runs. The odd window
// starts only at the first metadata mutation and ends with the formal publish
// attempt, before post-publication GC or any SQL retry. schema_refresh_mutex_
// serializes preparation with other refresh/lifecycle writers; begin() also
// verifies that snapshot.
class ObSchemaRefreshPublication
{
public:
  ObSchemaRefreshPublication();
  int init(ObTenantSchemaPublishState *state);
  bool is_enabled() const { return NULL != state_; }
  bool is_active() const { return guard_.is_active(); }
  const ObSchemaCacheGenerations &cache_generations() const
  { return snapshot_.cache_generations_; }
  int begin();
  void add_bump_domains(uint64_t domains) { bump_domains_ |= domains; }
  int prepare_publish(int64_t formal_version);
  int finish(int64_t store_version, int64_t formal_version);

private:
  int bump_pending_generations_();
  ObTenantSchemaPublishState *state_;
  ObSchemaPublishSnapshot snapshot_;
  ObTenantSchemaPublicationGuard guard_;
  uint64_t bump_domains_;
  int begin_ret_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaRefreshPublication);
};

class ObTenantSchemaPublishStateMap
{
public:
  ObTenantSchemaPublishStateMap();
  ~ObTenantSchemaPublishStateMap();
  int init();
  void destroy();
  int get_or_create(uint64_t tenant_id,
                    int64_t initial_refreshed_version,
                    ObTenantSchemaPublishState *&state);
  // State addresses stay stable until service shutdown because an on-demand
  // lookup may retain the raw pointer after leaving the map.  Tenant teardown
  // therefore retires the cache epoch instead of erasing/freeing the state.
  // A later lifecycle for the same tenant id inherits the monotonic frontier
  // and fresh metadata generations, so old resolve/member entries cannot
  // become visible again. Precise-version objects do not use these epochs.
  int retire(uint64_t tenant_id);
  int get(uint64_t tenant_id, ObTenantSchemaPublishState *&state) const;
  // Point lookups must never create or advance publication state.  The schema
  // store lifecycle initializes it before a guard can become externally visible.
  int get_published_snapshot(uint64_t tenant_id,
                             int64_t snapshot_version,
                             int64_t abs_timeout_us,
                             ObTenantSchemaPublishState *&state,
                             ObSchemaPublishSnapshot &snapshot) const;

private:
  static const int64_t DEFAULT_BUCKET_COUNT = 1024;
  typedef common::hash::ObHashMap<
      uint64_t,
      ObTenantSchemaPublishState *,
      common::hash::LatchReadWriteDefendMode> StateMap;
  StateMap states_;
  lib::ObMutex create_lock_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSchemaPublishStateMap);
};

class ObSchemaResolveCache
{
public:
  ObSchemaResolveCache();
  ~ObSchemaResolveCache();
  int init();
  void destroy();

  int get(const ObSchemaIDResolveKey &key,
          int64_t snapshot_version,
          ObSchemaResolveResult &result);
  int get(const ObSchemaNameResolveKey &key,
          int64_t snapshot_version,
          ObSchemaResolveResult &result);
  int get_if_current(const ObSchemaIDResolveKey &key,
                     int64_t snapshot_version,
                     const ObTenantSchemaPublishState &state,
                     const ObSchemaPublishSnapshot &token,
                     ObSchemaResolveResult &result);
  int get_if_current(const ObSchemaNameResolveKey &key,
                     int64_t snapshot_version,
                     const ObTenantSchemaPublishState &state,
                     const ObSchemaPublishSnapshot &token,
                     ObSchemaResolveResult &result);
  int publish_if_current(const ObSchemaIDResolveKey &key,
                         const ObSchemaResolveRange &range,
                         const ObTenantSchemaPublishState &state,
                         const ObSchemaPublishSnapshot &token);
  int publish_if_current(const ObSchemaNameResolveKey &key,
                         const ObSchemaResolveRange &range,
                         const ObTenantSchemaPublishState &state,
                         const ObSchemaPublishSnapshot &token);
  // Refresh is already inside an odd publication window. Under the same stripe
  // as query publication, it peeks and repairs only physically resident keys,
  // including negative entries and entries not covering the DDL version. A
  // missing key is left for query backsource, also during name finalization.
  // A failed publish is degraded to erase-and-miss; an error return means erase
  // could not be confirmed and the caller must bump generation before commit.
  int refresh(const ObSchemaIDResolveKey &key,
              const ObSchemaResolveRange &range);
  int refresh(const ObSchemaNameResolveKey &key,
              const ObSchemaResolveRange &range);
  int erase(const ObSchemaIDResolveKey &key);
  int erase(const ObSchemaNameResolveKey &key);

private:
  struct ResolveLock
  {
    ResolveLock() : mutex_(common::ObLatchIds::OB_SCHEMA_GETTER_LOCK) {}
    lib::ObMutex mutex_;
  };
  static const int64_t RESOLVE_WRITE_LOCK_COUNT = 256;
  typedef common::ObKVCache<ObSchemaIDResolveKey, ObSchemaResolveCacheValue> IDCache;
  typedef common::ObKVCache<ObSchemaNameResolveKey, ObSchemaResolveCacheValue> NameCache;

  template <typename Key, typename Cache>
  int get_(Cache &cache, const Key &key, int64_t snapshot_version,
           ObSchemaResolveResult &result);
  template <typename Key, typename Cache>
  int merge_and_put_(Cache &cache, const Key &key,
                     const ObSchemaResolveRange &range,
                     bool only_if_present,
                     bool erase_on_failure);
  ResolveLock &lock_for_(uint64_t hash)
  { return write_locks_[hash % RESOLVE_WRITE_LOCK_COUNT]; }

  IDCache id_cache_;
  NameCache name_cache_;
  ResolveLock write_locks_[RESOLVE_WRITE_LOCK_COUNT];
  uint64_t admission_seq_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaResolveCache);
};

class ObTablegroupTableIdCache;

// Applies one table DDL to the active cache-domain generations while the
// tenant publication sequence is odd. Only resident resolve keys are repaired
// so unrelated keys and precise-version object entries stay reusable. An
// error means the caller must invalidate affected keys or isolate only the
// mutable metadata domains before it publishes the new schema frontier.
class ObSchemaCacheIncrementalRefresher
{
public:
  ObSchemaCacheIncrementalRefresher(
      ObSchemaResolveCache &resolve_cache,
      ObTablegroupTableIdCache &tablegroup_cache)
    : resolve_cache_(resolve_cache), tablegroup_cache_(tablegroup_cache)
  {}
  ~ObSchemaCacheIncrementalRefresher() {}

  int refresh_table(uint64_t tenant_id,
                    const ObSchemaCacheGenerations &cache_generations,
                    uint64_t table_id,
                    int64_t change_schema_version,
                    const ObSimpleTableSchemaV2 *before_schema,
                    common::ObNameCaseMode before_case_mode,
                    const ObSimpleTableSchemaV2 *after_schema,
                    common::ObNameCaseMode after_case_mode);

  // A single DDL boundary may transfer names between several table IDs (for
  // example, vector rebuild swaps the old and new index names).  Call this
  // after every per-table refresh at that boundary so final positive bindings
  // win over another table's old-name tombstone.
  int finalize_table_names_at_boundary(
      uint64_t tenant_id,
      const ObSchemaCacheGenerations &cache_generations,
      int64_t change_schema_version,
      const common::ObIArray<const ObSimpleTableSchemaV2 *> &after_schemas,
      common::ObNameCaseMode after_case_mode);

private:
  ObSchemaResolveCache &resolve_cache_;
  ObTablegroupTableIdCache &tablegroup_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaCacheIncrementalRefresher);
};

// Like the full schema cache, a simple object is identified by tenant, table
// and precise version. Metadata publication does not invalidate its contents.
class ObSimpleSchemaCacheKey final : public common::ObIKVCacheKey
{
public:
  ObSimpleSchemaCacheKey();
  ObSimpleSchemaCacheKey(uint64_t tenant_id,
                         uint64_t table_id,
                         int64_t precise_schema_version);
  virtual ~ObSimpleSchemaCacheKey() {}
  bool is_valid() const;
  // The KVCache instance and its memory are owned by tenant 500. tenant_id_
  // remains embedded in the key to isolate business tenants.
  virtual uint64_t get_tenant_id() const override { return OB_SERVER_TENANT_ID; }
  virtual bool operator==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, int64_t buf_len,
                        common::ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(table_id),
               K_(precise_schema_version));

  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t precise_schema_version_;
};

class ObSimpleSchemaCacheValue final : public common::ObIKVCacheValue
{
public:
  ObSimpleSchemaCacheValue();
  explicit ObSimpleSchemaCacheValue(const ObSimpleTableSchemaV2 *schema);
  virtual ~ObSimpleSchemaCacheValue() {}
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, int64_t buf_len,
                        common::ObIKVCacheValue *&value) const override;
  TO_STRING_KV(KP_(schema));

  const ObSimpleTableSchemaV2 *schema_;
};

class ObSimpleSchemaObjectCache
{
public:
  ObSimpleSchemaObjectCache() : inited_(false) {}
  ~ObSimpleSchemaObjectCache() { destroy(); }
  int init();
  void destroy();
  int get(const ObSimpleSchemaCacheKey &key,
          const ObSimpleTableSchemaV2 *&schema,
          common::ObKVCacheHandle &handle);
  int put_and_fetch(const ObSimpleSchemaCacheKey &key,
                    const ObSimpleTableSchemaV2 &schema,
                    const ObSimpleTableSchemaV2 *&cached_schema,
                    common::ObKVCacheHandle &handle);
  int erase(const ObSimpleSchemaCacheKey &key);

private:
  typedef common::ObKVCache<ObSimpleSchemaCacheKey, ObSimpleSchemaCacheValue> ObjectCache;
  int validate_(const ObSimpleSchemaCacheKey &key,
                const ObSimpleSchemaCacheValue *value) const;
  ObjectCache cache_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSimpleSchemaObjectCache);
};

enum ObSchemaLoadKind : uint8_t
{
  LOAD_ID_RESOLVE = 0,
  LOAD_NAME_RESOLVE,
  LOAD_SIMPLE_OBJECT,
  LOAD_KIND_MAX
};

struct ObSchemaLoadKey
{
  ObSchemaLoadKey();
  // Object loads share a flight across metadata refreshes. The precise version
  // is part of the physical key hash; cache_generation_ is unused and zero.
  explicit ObSchemaLoadKey(const ObSimpleSchemaCacheKey &key);
  ObSchemaLoadKey(ObSchemaLoadKind kind,
                  uint64_t tenant_id,
                  uint64_t cache_generation,
                  uint64_t physical_key_hash);
  bool is_valid() const;
  bool operator==(const ObSchemaLoadKey &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_value) const
  {
    hash_value = hash();
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(kind), K_(tenant_id), K_(cache_generation), K_(physical_key_hash));

  ObSchemaLoadKind kind_;
  uint64_t tenant_id_;
  uint64_t cache_generation_;
  uint64_t physical_key_hash_;
};

// The registry stores no schema/result pointer.  A waiter must re-read its cache
// after wait() returns; different snapshot versions can share one flight safely.
class ObSchemaLoadCoordinator
{
public:
  ObSchemaLoadCoordinator();
  ~ObSchemaLoadCoordinator();
  int init();
  void destroy();
  int try_acquire(const ObSchemaLoadKey &key,
                  bool &is_leader,
                  uint64_t &flight_id);
  int wait(const ObSchemaLoadKey &key,
           uint64_t flight_id,
           int64_t abs_timeout_us);
  int finish(const ObSchemaLoadKey &key, uint64_t flight_id);

private:
  static const int64_t COND_SLOT_COUNT = 64;
  static const int64_t FLIGHT_BUCKET_COUNT = 1024;
  int is_active_(const ObSchemaLoadKey &key,
                 uint64_t flight_id,
                 bool &is_active);

  typedef common::hash::ObHashMap<
      ObSchemaLoadKey,
      uint64_t,
      common::hash::NoPthreadDefendMode> FlightMap;

  common::ObThreadCond cond_slots_[COND_SLOT_COUNT];
  FlightMap loading_flights_;
  common::ObSpinLock registry_lock_;
  uint64_t flight_seq_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaLoadCoordinator);
};

class ObSchemaLoadGuard
{
public:
  ObSchemaLoadGuard()
    : coordinator_(NULL), key_(), flight_id_(0), leader_(false) {}
  ~ObSchemaLoadGuard();
  int acquire(ObSchemaLoadCoordinator &coordinator,
              const ObSchemaLoadKey &key,
              bool &is_leader,
              uint64_t &flight_id);
  void reset();

private:
  ObSchemaLoadCoordinator *coordinator_;
  ObSchemaLoadKey key_;
  uint64_t flight_id_;
  bool leader_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaLoadGuard);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_SIMPLE_SCHEMA_KV_CACHE_H_
