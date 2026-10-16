/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include <atomic>
#include <chrono>
#include <thread>
#include <gtest/gtest.h>
#include "deps/oblib/unittest/lib/unittest_diagnostic_info_util.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_summary.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/ob_thread_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_simple_schema_kv_cache.h"
#include "share/schema/ob_tablegroup_table_id_cache.h"

#define private public
#define protected public
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#undef protected
#undef private
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace share::schema;

namespace
{
static ObSimpleMemLimitGetter TEST_MEM_LIMIT_GETTER;
static const uint64_t TEST_TENANT_ID = 500100;

int64_t get_stat_event_value(
    ObDiagnosticInfoSlot &slot,
    const ObStatEventIds::ObStatEventIdEnum event_id)
{
  ObStatEventAddStat *stat = slot.get_stats().get(event_id);
  return NULL == stat ? 0 : stat->get_stat_value();
}

ObSchemaResolveRange make_exist_range(
    const int64_t start,
    const int64_t end,
    const int64_t precise_version,
    const uint64_t admission_seq,
    const uint64_t table_id = 500001)
{
  return ObSchemaResolveRange(start, end, table_id, precise_version,
                              admission_seq, NS_MAX, true);
}

ObSchemaResolveRange make_name_range(
    const int64_t start,
    const int64_t end,
    const uint64_t table_id,
    const int64_t precise_version,
    const bool is_exist,
    const ObTableNameNamespace table_namespace = NS_NORMAL_TABLE)
{
  return ObSchemaResolveRange(start, end,
                              is_exist ? table_id : OB_INVALID_ID,
                              is_exist ? precise_version : OB_INVALID_VERSION,
                              0, table_namespace, is_exist);
}

template <typename Key>
int seed_resolve_cache(ObSchemaResolveCache &cache,
                       const Key &key,
                       const ObSchemaResolveRange &range)
{
  int ret = OB_SUCCESS;
  ObTenantSchemaPublishState state;
  ObSchemaPublishSnapshot token;
  if (!key.is_valid() || !range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(state.init(key.tenant_id_, range.start_version_, key.cache_generation_))) {
  } else if (OB_FAIL(state.read_stable(token))) {
  } else {
    ret = cache.publish_if_current(key, range, state, token);
  }
  return ret;
}

void init_simple_table(ObSimpleTableSchemaV2 &schema,
                       const uint64_t table_id,
                       const int64_t schema_version,
                       const char *table_name,
                       const uint64_t database_id = 500010,
                       const uint64_t session_id = 0,
                       const uint64_t tablegroup_id = OB_INVALID_ID)
{
  schema.set_tenant_id(TEST_TENANT_ID);
  schema.set_table_id(table_id);
  schema.set_database_id(database_id);
  schema.set_schema_version(schema_version);
  schema.set_session_id(session_id);
  schema.set_tablegroup_id(tablegroup_id);
  ASSERT_EQ(OB_SUCCESS, schema.set_table_name(table_name));
}

class TestSimpleSchemaKVCache : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  void SetUp() override
  {
    auto tenant_allocator_guard = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        TEST_TENANT_ID, ObCtxIds::KVSTORE_CACHE_ID);
    if (NULL == tenant_allocator_guard.ref_allocator()) {
      ASSERT_EQ(OB_SUCCESS,
                ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
                    TEST_TENANT_ID));
    }
    auto server_allocator_guard = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        OB_SERVER_TENANT_ID, ObCtxIds::KVSTORE_CACHE_ID);
    if (NULL == server_allocator_guard.ref_allocator()) {
      ASSERT_EQ(OB_SUCCESS,
                ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
                    OB_SERVER_TENANT_ID));
    }
    ASSERT_EQ(OB_SUCCESS,
              TEST_MEM_LIMIT_GETTER.add_tenant(TEST_TENANT_ID,
                                               16L << 20,
                                               64L << 20));
    ASSERT_EQ(OB_SUCCESS,
              TEST_MEM_LIMIT_GETTER.add_tenant(OB_SERVER_TENANT_ID,
                                               0,
                                               INT64_MAX));
    int ret = ObKVGlobalCache::get_instance().init(
        &TEST_MEM_LIMIT_GETTER, 1024, 1L << 30, lib::ACHUNK_SIZE);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
    ret = ObClockGenerator::init();
    ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
    CHUNK_MGR.set_limit(5L << 30);
  }

  void TearDown() override
  {
    ObKVGlobalCache::get_instance().destroy();
    TEST_MEM_LIMIT_GETTER.reset();
  }
};

// Exercise the full refresh planner with deterministic history batches and
// locally initialized caches; no observer or SQL connection is required.
class IncrementalHistorySchemaService : public ObSchemaServiceSQLImpl
{
public:
  struct TableVersion
  {
    uint64_t table_id_;
    int64_t version_;
    const char *name_;
  };
  struct FetchExpectation
  {
    int64_t version_;
    int64_t key_count_;
    uint64_t table_mask_;
  };

  IncrementalHistorySchemaService(const TableVersion *versions, const int64_t version_count,
                                  const FetchExpectation *fetches, const int64_t fetch_count)
    : versions_(versions), version_count_(version_count),
      fetches_(fetches), fetch_count_(fetch_count), fetch_index_(0),
      probe_state_(nullptr), probe_cache_(nullptr), probe_token_(),
      fail_fetch_index_(-1), invalidate_fetch_index_(-1), probe_count_(0),
      expect_initial_miss_(false)
  {}

  void probe_preparation(ObTenantSchemaPublishState &state,
                         ObSchemaResolveCache &cache,
                         const ObSchemaPublishSnapshot &token,
                         const int64_t fail_fetch_index = -1,
                         const int64_t invalidate_fetch_index = -1,
                         const bool expect_initial_miss = false)
  {
    probe_state_ = &state;
    probe_cache_ = &cache;
    probe_token_ = token;
    fail_fetch_index_ = fail_fetch_index;
    invalidate_fetch_index_ = invalidate_fetch_index;
    expect_initial_miss_ = expect_initial_miss;
  }

  int get_batch_tables(const ObRefreshSchemaStatus &schema_status,
                       ObISQLClient &client,
                       ObIAllocator &allocator,
                       const int64_t schema_version,
                       ObArray<SchemaKey> &schema_keys,
                       ObIArray<ObSimpleTableSchemaV2 *> &schemas) override
  {
    int ret = OB_SUCCESS;
    uint64_t table_mask = 0;
    const TableVersion *row = nullptr;
    void *buf = nullptr;
    ObSimpleTableSchemaV2 *schema = nullptr;
    UNUSED(client);
    schemas.reuse();
    if (nullptr != probe_state_) {
      // Model an already admitted point lookup completing during each SQL
      // fetch. Preparation must allow both old-token hits and late fills.
      ObSchemaPublishSnapshot current;
      ObSchemaResolveResult result;
      const ObSchemaIDResolveKey key(schema_status.tenant_id_,
                                     probe_token_.cache_generations_.id_resolve_, 500001);
      EXPECT_EQ(OB_SUCCESS, probe_state_->read_stable(
          current, ObTimeUtility::current_time() + 1000));
      EXPECT_EQ(probe_token_.refresh_seq_, current.refresh_seq_);
      EXPECT_TRUE(probe_state_->can_publish(probe_token_));
      const bool is_initial_miss = expect_initial_miss_ && 0 == probe_count_;
      EXPECT_EQ(is_initial_miss ? OB_ENTRY_NOT_EXIST : OB_SUCCESS, probe_cache_->get_if_current(
          key, 100, *probe_state_, probe_token_, result));
      if (!is_initial_miss) {
        EXPECT_EQ(100, result.precise_schema_version_);
      }
      EXPECT_EQ(OB_SUCCESS, probe_cache_->publish_if_current(
          key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0),
          *probe_state_, probe_token_));
      ++probe_count_;
      if (fetch_index_ == fail_fetch_index_) {
        return OB_TIMEOUT;
      }
    }
    if (OB_SYS_TENANT_ID != schema_status.tenant_id_ || fetch_index_ >= fetch_count_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (schema_version != fetches_[fetch_index_].version_
               || schema_keys.count() != fetches_[fetch_index_].key_count_) {
      ret = OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_keys.count(); ++i) {
      const SchemaKey &key = schema_keys.at(i);
      if (key.tenant_id_ != schema_status.tenant_id_ || key.schema_version_ != schema_version
          || key.table_id_ < 500001 || key.table_id_ > 500003) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const uint64_t table_bit = 1ULL << (key.table_id_ - 500001);
        if (0 != (table_mask & table_bit)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          table_mask |= table_bit;
        }
      }
      row = nullptr;
      for (int64_t j = 0; OB_SUCC(ret) && j < version_count_; ++j) {
        if (versions_[j].table_id_ == key.table_id_ && versions_[j].version_ == schema_version) {
          row = &versions_[j];
        }
      }
      if (OB_SUCC(ret) && nullptr != row) {
        if (nullptr == (buf = allocator.alloc(sizeof(ObSimpleTableSchemaV2)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          schema = new (buf) ObSimpleTableSchemaV2(&allocator);
          schema->set_tenant_id(schema_status.tenant_id_);
          schema->set_table_id(row->table_id_);
          schema->set_database_id(500010);
          schema->set_schema_version(row->version_);
          if (OB_FAIL(schema->set_table_name(row->name_))) {
          } else if (OB_FAIL(schemas.push_back(schema))) {
          }
        }
      }
    }
    if (OB_SUCC(ret) && table_mask != fetches_[fetch_index_].table_mask_) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      if (nullptr != probe_state_ && fetch_index_ == invalidate_fetch_index_) {
        ObTenantSchemaPublicationGuard concurrent_publication;
        EXPECT_EQ(OB_SUCCESS, concurrent_publication.begin(*probe_state_));
        EXPECT_EQ(OB_SUCCESS, concurrent_publication.bump_generation());
        EXPECT_EQ(OB_SUCCESS,
                  concurrent_publication.commit(probe_token_.refreshed_version_));
      }
      ++fetch_index_;
    }
    return ret;
  }

  int64_t get_fetch_count() const { return fetch_index_; }
  int64_t get_probe_count() const { return probe_count_; }

private:
  const TableVersion *versions_;
  const int64_t version_count_;
  const FetchExpectation *fetches_;
  const int64_t fetch_count_;
  int64_t fetch_index_;
  ObTenantSchemaPublishState *probe_state_;
  ObSchemaResolveCache *probe_cache_;
  ObSchemaPublishSnapshot probe_token_;
  int64_t fail_fetch_index_;
  int64_t invalidate_fetch_index_;
  int64_t probe_count_;
  bool expect_initial_miss_;
};

class IsolatedIncrementalSchemaService : public ObMultiVersionSchemaService
{
public:
  explicit IsolatedIncrementalSchemaService(ObSchemaService &history)
  {
    schema_service_ = &history;
    schema_mode_ = ObSimpleSchemaMode::ON_DEMAND;
  }
  ~IsolatedIncrementalSchemaService()
  {
    // The history provider is borrowed; the base destructor normally owns it.
    schema_service_ = nullptr;
  }
  int init_test_caches()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(get_schema_resolve_cache().init())) {
    } else if (OB_FAIL(get_tablegroup_table_id_cache().init())) {
    }
    return ret;
  }
};

class IsolatedPublicationSchemaService : public ObMultiVersionSchemaService
{
public:
  IsolatedPublicationSchemaService(ObTenantSchemaPublishState &state,
                                   const ObSchemaPublishSnapshot &before)
    : state_(state), before_(before), publish_count_(0), gc_count_(0),
      store_version_(120), publish_ret_(OB_SUCCESS), gc_ret_(OB_SUCCESS),
      gc_reader_ret_(OB_ERR_UNEXPECTED), gc_snapshot_(),
      expect_publication_(true), expect_force_add_(false)
  {
    schema_mode_ = ObSimpleSchemaMode::ON_DEMAND;
  }

  int init_test_store()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(schema_store_map_.init(16))) {
    } else if (OB_FAIL(schema_store_map_.create(OB_SYS_TENANT_ID, 8, 8))) {
    } else {
      schema_store_map_.get(OB_SYS_TENANT_ID)->update_refreshed_version(100);
    }
    return ret;
  }

  int add_schema_to_store_(const uint64_t tenant_id, const bool force_add) override
  {
    ++publish_count_;
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    EXPECT_EQ(expect_force_add_, force_add);
    EXPECT_EQ(!expect_publication_, state_.can_publish(before_));
    ObSchemaPublishSnapshot blocked;
    EXPECT_EQ(expect_publication_ ? OB_TIMEOUT : OB_SUCCESS,
              state_.read_stable(blocked, ObTimeUtility::current_time() + 1000));
    schema_store_map_.get(tenant_id)->update_refreshed_version(store_version_);
    return publish_ret_;
  }

  int gc_schema_after_publish_(const uint64_t tenant_id) override
  {
    ++gc_count_;
    EXPECT_EQ(OB_SYS_TENANT_ID, tenant_id);
    EXPECT_EQ(1, publish_count_);
    const int64_t snapshot_version = expect_publication_
        ? store_version_ : before_.refreshed_version_;
    // Keep GC in progress until the reader completes. This catches a window
    // that only closes after GC returns, even if the final frontier is correct.
    std::thread reader([&]() {
      gc_reader_ret_ = state_.get_published_snapshot(
          snapshot_version, ObTimeUtility::current_time() + 1000L * 1000L,
          gc_snapshot_);
    });
    reader.join();
    EXPECT_EQ(OB_SUCCESS, gc_reader_ret_);
    EXPECT_EQ(snapshot_version, gc_snapshot_.refreshed_version_);
    return gc_ret_;
  }

  ObTenantSchemaPublishState &state_;
  const ObSchemaPublishSnapshot &before_;
  int64_t publish_count_;
  int64_t gc_count_;
  int64_t store_version_;
  int publish_ret_;
  int gc_ret_;
  int gc_reader_ret_;
  ObSchemaPublishSnapshot gc_snapshot_;
  bool expect_publication_;
  bool expect_force_add_;
};

TEST(SimpleSchemaKVCacheStruct, name_key_deep_copy_owns_name)
{
  char name_buf[] = "MixedCaseName";
  ObSchemaNameResolveKey key(1001, 7, 2001, 0, NS_NORMAL_TABLE,
                             ObString(sizeof(name_buf) - 1, name_buf));
  ASSERT_TRUE(key.is_valid());
  ASSERT_EQ(OB_SERVER_TENANT_ID, key.get_tenant_id());

  char copy_buf[sizeof(ObSchemaNameResolveKey) + sizeof(name_buf)] = {};
  ObIKVCacheKey *base_copy = NULL;
  ASSERT_EQ(OB_SUCCESS, key.deep_copy(copy_buf, sizeof(copy_buf), base_copy));
  ASSERT_NE(nullptr, base_copy);
  ObSchemaNameResolveKey *copy = static_cast<ObSchemaNameResolveKey *>(base_copy);
  ASSERT_EQ(key.hash(), copy->hash());
  EXPECT_EQ(OB_SERVER_TENANT_ID, copy->get_tenant_id());
  name_buf[0] = 'x';
  EXPECT_EQ(ObString("MixedCaseName"), copy->normalized_name_);
  EXPECT_NE(key.normalized_name_, copy->normalized_name_);
}

TEST(SimpleSchemaKVCacheStruct, on_demand_keys_charge_memory_to_server_tenant)
{
  ObSchemaIDResolveKey id_key(TEST_TENANT_ID, 7, 500001);
  ObSchemaNameResolveKey name_key(
      TEST_TENANT_ID, 7, 500010, 0, NS_NORMAL_TABLE,
      ObString::make_string("t1"));
  ObSimpleSchemaCacheKey object_key(TEST_TENANT_ID, 500001, 100);
  ObTablegroupTableIdCacheKey tablegroup_key(TEST_TENANT_ID, 7, 500020);

  EXPECT_EQ(OB_SERVER_TENANT_ID, id_key.get_tenant_id());
  EXPECT_EQ(OB_SERVER_TENANT_ID, name_key.get_tenant_id());
  EXPECT_EQ(OB_SERVER_TENANT_ID, object_key.get_tenant_id());
  EXPECT_EQ(OB_SERVER_TENANT_ID, tablegroup_key.get_tenant_id());
}

TEST(SimpleSchemaKVCacheStruct, authoritative_range_splits_and_merges)
{
  ObSchemaResolveRange old_range = make_exist_range(10, OB_SCHEMA_VERSION_INF, 10, 1);
  ObSchemaResolveCacheValue current(&old_range, 1);
  ObSchemaResolveRangeSet::RangeArray ranges;

  ObSchemaResolveRange v20 = make_exist_range(20, OB_SCHEMA_VERSION_INF, 20, 2);
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(&current, v20, ranges));
  ASSERT_EQ(2, ranges.count());
  EXPECT_EQ(10, ranges.at(0).start_version_);
  EXPECT_EQ(19, ranges.at(0).end_version_);
  EXPECT_EQ(20, ranges.at(1).start_version_);
  EXPECT_EQ(OB_SCHEMA_VERSION_INF, ranges.at(1).end_version_);

  ObSchemaResolveRange current2_ranges[2] = {ranges.at(0), ranges.at(1)};
  ObSchemaResolveCacheValue current2(current2_ranges, ARRAYSIZEOF(current2_ranges));
  ObSchemaResolveRange negative(30, 39, OB_INVALID_ID, OB_INVALID_VERSION,
                                3, NS_MAX, false);
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(&current2, negative, ranges));
  ASSERT_EQ(4, ranges.count());
  EXPECT_EQ(20, ranges.at(1).start_version_);
  EXPECT_EQ(29, ranges.at(1).end_version_);
  EXPECT_FALSE(ranges.at(2).is_exist_);
  EXPECT_EQ(40, ranges.at(3).start_version_);
  EXPECT_EQ(OB_SCHEMA_VERSION_INF, ranges.at(3).end_version_);
  EXPECT_TRUE(ObSchemaResolveRangeSet::is_normalized(ranges));
}

TEST(SimpleSchemaKVCacheStruct, range_count_is_bounded_by_admission_age)
{
  ObSchemaResolveRange old_ranges[OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT];
  for (int64_t i = 0; i < OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT; ++i) {
    const int64_t start = i * 10;
    old_ranges[i] = make_exist_range(start, start + 8, start, i + 1,
                                     600000 + i);
  }
  ObSchemaResolveCacheValue current(old_ranges, OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT);
  ASSERT_TRUE(current.is_valid());
  ObSchemaResolveRange newest = make_exist_range(200, OB_SCHEMA_VERSION_INF,
                                                  200, 100, 700000);
  ObSchemaResolveRangeSet::RangeArray result;
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(&current, newest, result));
  ASSERT_EQ(OB_MAX_SCHEMA_RESOLVE_RANGE_COUNT, result.count());
  EXPECT_EQ(10, result.at(0).start_version_); // admission_seq=1 was evicted
  EXPECT_EQ(200, result.at(result.count() - 1).start_version_);
  EXPECT_EQ(OB_SCHEMA_VERSION_INF, result.at(result.count() - 1).end_version_);
}

TEST(SimpleSchemaKVCacheStruct, name_lifecycle_keeps_drop_recreate_gap)
{
  ObSchemaResolveRange old_table(10, 29, 500001, 10, 1,
                                 NS_NORMAL_TABLE, true);
  ObSchemaResolveRange dropped(30, 39, OB_INVALID_ID, OB_INVALID_VERSION, 2,
                               NS_NORMAL_TABLE, false);
  ObSchemaResolveRange recreated(40, OB_SCHEMA_VERSION_INF, 500002, 40, 3,
                                 NS_NORMAL_TABLE, true);
  ObSchemaResolveRangeSet::RangeArray ranges;
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(NULL, old_table, ranges));
  ObSchemaResolveCacheValue value1(&ranges.at(0), ranges.count());
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(&value1, dropped, ranges));
  ObSchemaResolveRange value2_ranges[2] = {ranges.at(0), ranges.at(1)};
  ObSchemaResolveCacheValue value2(value2_ranges, ARRAYSIZEOF(value2_ranges));
  ASSERT_EQ(OB_SUCCESS, ObSchemaResolveRangeSet::merge(&value2, recreated, ranges));
  ASSERT_EQ(3, ranges.count());
  EXPECT_EQ(500001U, ranges.at(0).schema_id_);
  EXPECT_FALSE(ranges.at(1).is_exist_);
  EXPECT_EQ(500002U, ranges.at(2).schema_id_);
  EXPECT_TRUE(ObSchemaResolveRangeSet::is_normalized(ranges));
}

TEST(SimpleSchemaKVCacheConcurrency, publish_state_rejects_stale_token)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  ASSERT_TRUE(state.can_publish(token));

  {
    ObTenantSchemaPublicationGuard publication;
    ASSERT_EQ(OB_SUCCESS, publication.begin(state));
    EXPECT_FALSE(state.can_publish(token));
    ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
    ASSERT_EQ(OB_SUCCESS, publication.commit(120));
  }

  ObSchemaPublishSnapshot current;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(current));
  EXPECT_EQ(2U, current.refresh_seq_);
  EXPECT_EQ(2U, current.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, current.cache_generations_.name_resolve_);
  EXPECT_EQ(2U, current.cache_generations_.tablegroup_);
  EXPECT_EQ(120, current.refreshed_version_);
  EXPECT_FALSE(state.can_publish(token));
  EXPECT_TRUE(state.can_publish(current));
}

TEST(SimpleSchemaKVCacheConcurrency, publication_guard_aborts_to_even_seq)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  {
    ObTenantSchemaPublicationGuard publication;
    ASSERT_EQ(OB_SUCCESS, publication.begin(state));
    ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  }
  ObSchemaPublishSnapshot current;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(current));
  EXPECT_EQ(2U, current.refresh_seq_);
  EXPECT_EQ(2U, current.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, current.cache_generations_.name_resolve_);
  EXPECT_EQ(2U, current.cache_generations_.tablegroup_);
  EXPECT_EQ(100, current.refreshed_version_);
}

TEST(SimpleSchemaKVCacheConcurrency, refresh_preparation_and_temporary_store_keep_old_frontier)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ASSERT_TRUE(publication.is_enabled());
  EXPECT_FALSE(publication.is_active());
  EXPECT_TRUE(state.can_publish(before));

  // Core/sys can publish v118/v119 to the schema store. A later SQL failure
  // must leave point lookups on v100 without opening a publication window.
  publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_ALL);
  ASSERT_EQ(OB_SUCCESS, publication.finish(119, OB_INVALID_VERSION));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(before.refreshed_version_, after.refreshed_version_);
  EXPECT_EQ(before.refresh_seq_, after.refresh_seq_);
  EXPECT_EQ(before.cache_generations_, after.cache_generations_);
  EXPECT_TRUE(state.can_publish(before));
}

TEST(SimpleSchemaKVCacheConcurrency, full_refresh_final_publish_invalidates_all_domains_once)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_ALL);
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  EXPECT_TRUE(publication.is_active());
  EXPECT_FALSE(state.can_publish(before));
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  // Close the window after store publication, before post-publication GC.
  ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(120, after.refreshed_version_);
  EXPECT_EQ(before.refresh_seq_ + 2, after.refresh_seq_);
  EXPECT_EQ(before.cache_generations_.id_resolve_ + 1, after.cache_generations_.id_resolve_);
  EXPECT_EQ(before.cache_generations_.name_resolve_ + 1, after.cache_generations_.name_resolve_);
  EXPECT_EQ(before.cache_generations_.tablegroup_ + 1, after.cache_generations_.tablegroup_);
}

TEST(SimpleSchemaKVCacheConcurrency, failed_formal_publish_closes_before_retry)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ASSERT_EQ(OB_SUCCESS, publication.begin());
  publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_NAME_RESOLVE);
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  ASSERT_EQ(OB_SUCCESS, publication.finish(119, 120));
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot retry_snapshot;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      retry_snapshot, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(100, retry_snapshot.refreshed_version_);
  EXPECT_EQ(1U, retry_snapshot.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, retry_snapshot.cache_generations_.name_resolve_);
  EXPECT_EQ(1U, retry_snapshot.cache_generations_.tablegroup_);
  EXPECT_TRUE(state.can_publish(retry_snapshot));

  // Retry preparation starts with the current generations, and an empty DDL
  // batch still opens a window at its final publication.
  EXPECT_EQ(retry_snapshot.cache_generations_, publication.cache_generations());
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(144));
  EXPECT_FALSE(state.can_publish(retry_snapshot));
  ASSERT_EQ(OB_SUCCESS, publication.finish(144, 144));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(144, after.refreshed_version_);
  EXPECT_EQ(retry_snapshot.cache_generations_, after.cache_generations_);
}

TEST(SimpleSchemaKVCacheConcurrency, refresh_begin_rejects_changed_preparation_snapshot)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ObTenantSchemaPublicationGuard concurrent_publication;
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.bump_generation());
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.commit(120));

  EXPECT_EQ(OB_SCHEMA_EAGAIN, publication.begin());
  EXPECT_EQ(OB_SCHEMA_EAGAIN, publication.begin());
  EXPECT_EQ(OB_SCHEMA_EAGAIN, publication.finish(120, OB_INVALID_VERSION));
  ObSchemaPublishSnapshot current;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      current, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(120, current.refreshed_version_);
  EXPECT_EQ(2U, current.cache_generations_.id_resolve_);
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(144));
  ASSERT_EQ(OB_SUCCESS, publication.finish(144, 144));
}

TEST(SimpleSchemaKVCacheConcurrency, formal_publish_wrapper_distinguishes_early_and_late_errors)
{
  const int64_t store_versions[] = {119, 120};
  for (int64_t store_version : store_versions) {
    SCOPED_TRACE(store_version);
    ObTenantSchemaPublishState state;
    ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
    ObSchemaPublishSnapshot before;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
    IsolatedPublicationSchemaService service(state, before);
    ASSERT_EQ(OB_SUCCESS, service.init_test_store());
    service.store_version_ = store_version;
    service.publish_ret_ = OB_ALLOCATE_MEMORY_FAILED;
    ObSchemaRefreshPublication publication;
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_ALL);

    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
              service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 120, publication));
    EXPECT_EQ(1, service.publish_count_);
    EXPECT_EQ(0, service.gc_count_);
    EXPECT_FALSE(publication.is_active());
    ObSchemaPublishSnapshot after;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
    EXPECT_EQ(120 == store_version ? 120 : 100, after.refreshed_version_);
    EXPECT_EQ(before.refresh_seq_ + 2, after.refresh_seq_);
    EXPECT_EQ(2U, after.cache_generations_.id_resolve_);
    EXPECT_EQ(2U, after.cache_generations_.name_resolve_);
    EXPECT_EQ(2U, after.cache_generations_.tablegroup_);
  }
}

TEST(SimpleSchemaKVCacheConcurrency, formal_publish_wrapper_stops_after_begin_failure)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  IsolatedPublicationSchemaService service(state, before);
  ASSERT_EQ(OB_SUCCESS, service.init_test_store());
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ObTenantSchemaPublicationGuard concurrent_publication;
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.bump_generation());
  ASSERT_EQ(OB_SUCCESS, concurrent_publication.commit(104));

  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 120, publication));
  EXPECT_EQ(0, service.publish_count_);
  EXPECT_EQ(0, service.gc_count_);
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(104, after.refreshed_version_);
  EXPECT_EQ(2U, after.cache_generations_.id_resolve_);
}

TEST(SimpleSchemaKVCacheConcurrency, original_publish_wrapper_preserves_legacy_version_handling)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  IsolatedPublicationSchemaService service(state, before);
  ASSERT_EQ(OB_SUCCESS, service.init_test_store());
  service.schema_mode_ = ObSimpleSchemaMode::ORIGINAL;
  service.store_version_ = 119;
  service.expect_publication_ = false;
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(nullptr));
  EXPECT_FALSE(publication.is_enabled());

  EXPECT_EQ(OB_SUCCESS,
            service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 119, publication));
  EXPECT_EQ(1, service.publish_count_);
  EXPECT_EQ(1, service.gc_count_);
  EXPECT_FALSE(publication.is_active());
  EXPECT_TRUE(state.can_publish(before));
}

TEST(SimpleSchemaKVCacheConcurrency, formal_publish_closes_before_gc_including_gc_failure)
{
  const int gc_returns[] = {OB_SUCCESS, OB_ALLOCATE_MEMORY_FAILED};
  for (int gc_ret : gc_returns) {
    SCOPED_TRACE(gc_ret);
    ObTenantSchemaPublishState state;
    ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
    ObSchemaPublishSnapshot before;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
    IsolatedPublicationSchemaService service(state, before);
    ASSERT_EQ(OB_SUCCESS, service.init_test_store());
    service.gc_ret_ = gc_ret;
    ObSchemaRefreshPublication publication;
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_ALL);

    // Refresh retains its outer mutex throughout publication and GC. Readers
    // only need the publication window to close, not this mutex to be released.
    {
      lib::ObMutexGuard refresh_guard(service.schema_refresh_mutex_);
      EXPECT_EQ(gc_ret,
                service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 120, publication));
    }
    EXPECT_EQ(1, service.publish_count_);
    EXPECT_EQ(1, service.gc_count_);
    EXPECT_EQ(OB_SUCCESS, service.gc_reader_ret_);
    EXPECT_FALSE(publication.is_active());
    EXPECT_EQ(120, service.gc_snapshot_.refreshed_version_);
    EXPECT_EQ(before.refresh_seq_ + 2, service.gc_snapshot_.refresh_seq_);
    EXPECT_EQ(2U, service.gc_snapshot_.cache_generations_.id_resolve_);
    EXPECT_EQ(2U, service.gc_snapshot_.cache_generations_.name_resolve_);
    EXPECT_EQ(2U, service.gc_snapshot_.cache_generations_.tablegroup_);
    // A GC error must neither roll back the committed frontier nor reopen it.
    EXPECT_TRUE(state.can_publish(service.gc_snapshot_));
  }
}

TEST(SimpleSchemaKVCacheConcurrency, invalid_formal_publish_closes_active_window_without_gc)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  IsolatedPublicationSchemaService service(state, before);
  ASSERT_EQ(OB_SUCCESS, service.init_test_store());
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ASSERT_EQ(OB_SUCCESS, publication.begin());
  publication.add_bump_domains(SCHEMA_CACHE_DOMAIN_NAME_RESOLVE);

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 119, publication));
  EXPECT_EQ(0, service.publish_count_);
  EXPECT_EQ(0, service.gc_count_);
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(100, after.refreshed_version_);
  EXPECT_EQ(before.refresh_seq_ + 2, after.refresh_seq_);
  EXPECT_EQ(1U, after.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, after.cache_generations_.name_resolve_);
  EXPECT_EQ(1U, after.cache_generations_.tablegroup_);
}

TEST(SimpleSchemaKVCacheConcurrency, formal_publish_finish_error_is_preserved_without_gc)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 120));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  IsolatedPublicationSchemaService service(state, before);
  ASSERT_EQ(OB_SUCCESS, service.init_test_store());
  // Deliberately leave the store behind the already published frontier while
  // the store hook reports success, so finish() must reject the inconsistency.
  service.store_version_ = 119;
  service.gc_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));

  EXPECT_EQ(OB_ERR_UNEXPECTED,
            service.publish_schema_with_cache_(OB_SYS_TENANT_ID, 144, publication));
  EXPECT_EQ(1, service.publish_count_);
  EXPECT_EQ(0, service.gc_count_);
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(120, after.refreshed_version_);
  EXPECT_EQ(before.refresh_seq_ + 2, after.refresh_seq_);
}

TEST(SimpleSchemaKVCacheConcurrency, add_schema_preserves_force_add_and_gc_errors)
{
  const int publish_returns[] = {OB_SUCCESS, OB_ALLOCATE_MEMORY_FAILED};
  const int gc_returns[] = {OB_SUCCESS, OB_ERR_UNEXPECTED};
  for (int publish_ret : publish_returns) {
    for (int gc_ret : gc_returns) {
      SCOPED_TRACE(publish_ret);
      SCOPED_TRACE(gc_ret);
      ObTenantSchemaPublishState state;
      ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
      ObSchemaPublishSnapshot before;
      ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
      IsolatedPublicationSchemaService service(state, before);
      ASSERT_EQ(OB_SUCCESS, service.init_test_store());
      service.expect_publication_ = false;
      service.expect_force_add_ = true;
      service.publish_ret_ = publish_ret;
      service.gc_ret_ = gc_ret;

      EXPECT_EQ(OB_SUCCESS == publish_ret ? gc_ret : publish_ret,
                service.add_schema(OB_SYS_TENANT_ID, true));
      EXPECT_EQ(1, service.publish_count_);
      EXPECT_EQ(OB_SUCCESS == publish_ret ? 1 : 0, service.gc_count_);
      EXPECT_TRUE(state.can_publish(before));
    }
  }
}

TEST(SimpleSchemaKVCacheConcurrency, selective_generation_bump_preserves_id_cache)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation(
      SCHEMA_CACHE_DOMAIN_NAME_RESOLVE | SCHEMA_CACHE_DOMAIN_TABLEGROUP));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(after));
  EXPECT_EQ(before.cache_generations_.id_resolve_,
            after.cache_generations_.id_resolve_);
  EXPECT_EQ(before.cache_generations_.name_resolve_ + 1,
            after.cache_generations_.name_resolve_);
  EXPECT_EQ(before.cache_generations_.tablegroup_ + 1,
            after.cache_generations_.tablegroup_);
  EXPECT_FALSE(state.can_publish(before));
  EXPECT_TRUE(state.can_publish(after));
}

TEST(SimpleSchemaKVCacheConcurrency, stable_reader_waits_for_committed_publication)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());

  std::atomic<bool> reader_done(false);
  std::atomic<int> reader_ret(OB_ERR_UNEXPECTED);
  ObSchemaPublishSnapshot snapshot;
  std::thread reader([&]() {
    reader_ret.store(state.read_stable(
        snapshot, ObTimeUtility::current_time() + 2L * 1000L * 1000L));
    reader_done.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(reader_done.load());
  EXPECT_EQ(OB_SUCCESS, publication.commit(120));
  reader.join();
  EXPECT_EQ(OB_SUCCESS, reader_ret.load());
  EXPECT_EQ(2U, snapshot.refresh_seq_);
  EXPECT_EQ(2U, snapshot.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, snapshot.cache_generations_.name_resolve_);
  EXPECT_EQ(2U, snapshot.cache_generations_.tablegroup_);
  EXPECT_EQ(120, snapshot.refreshed_version_);
}

TEST(SimpleSchemaKVCacheConcurrency, stable_reader_obeys_absolute_deadline)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ObSchemaPublishSnapshot snapshot;
  EXPECT_EQ(OB_TIMEOUT, state.read_stable(
      snapshot, ObTimeUtility::current_time() + 10L * 1000L));
}

TEST(SimpleSchemaKVCacheConcurrency, singleflight_wakes_waiter_without_result_pointer)
{
  ObSchemaLoadCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init());
  ObSchemaLoadKey key(LOAD_ID_RESOLVE, 1001, 1, 9988);
  bool leader = false;
  uint64_t leader_flight_id = 0;
  uint64_t waiter_flight_id = 0;
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key, leader, leader_flight_id));
  ASSERT_TRUE(leader);
  ASSERT_NE(0U, leader_flight_id);
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key, leader, waiter_flight_id));
  ASSERT_FALSE(leader);
  ASSERT_EQ(leader_flight_id, waiter_flight_id);

  std::atomic<int> wait_ret(OB_ERR_UNEXPECTED);
  std::thread waiter([&]() {
    wait_ret.store(coordinator.wait(
        key, waiter_flight_id,
        ObTimeUtility::current_time() + 2L * 1000L * 1000L));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key, leader_flight_id));
  // Cleanup is idempotent so a retry after an ambiguous finish cannot leak or
  // poison the next flight for this key.
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key, leader_flight_id));
  waiter.join();
  EXPECT_EQ(OB_SUCCESS, wait_ret.load());

  uint64_t next_flight_id = 0;
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key, leader, next_flight_id));
  EXPECT_TRUE(leader);
  EXPECT_NE(leader_flight_id, next_flight_id);
  EXPECT_EQ(OB_SUCCESS, coordinator.finish(key, next_flight_id));
}

TEST(SimpleSchemaKVCacheConcurrency, object_flight_survives_metadata_publication)
{
  ObSchemaLoadCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));
  const ObSimpleSchemaCacheKey object_key(1001, 500001, 100);
  const ObSchemaLoadKey key(object_key);
  ASSERT_TRUE(key.is_valid());
  EXPECT_FALSE(ObSchemaLoadKey(ObSimpleSchemaCacheKey()).is_valid());
  EXPECT_FALSE(ObSchemaLoadKey(LOAD_SIMPLE_OBJECT, 1001, 1, object_key.hash()).is_valid());
  EXPECT_FALSE(ObSchemaLoadKey(LOAD_ID_RESOLVE, 1001, 0, object_key.hash()).is_valid());

  bool leader = false;
  uint64_t flight = 0;
  ObSchemaLoadGuard load_guard;
  ASSERT_EQ(OB_SUCCESS, load_guard.acquire(coordinator, key, leader, flight));
  ASSERT_TRUE(leader);
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  bool follower_is_leader = true;
  uint64_t follower_flight = 0;
  const ObSchemaLoadKey same_key(ObSimpleSchemaCacheKey(1001, 500001, 100));
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(
      same_key, follower_is_leader, follower_flight));
  EXPECT_FALSE(follower_is_leader);
  EXPECT_EQ(flight, follower_flight);

  ObSchemaLoadGuard next_version_guard;
  const ObSchemaLoadKey next_version(ObSimpleSchemaCacheKey(1001, 500001, 120));
  ASSERT_EQ(OB_SUCCESS, next_version_guard.acquire(
      coordinator, next_version, follower_is_leader, follower_flight));
  EXPECT_TRUE(follower_is_leader);
  EXPECT_NE(flight, follower_flight);
  load_guard.reset();
  EXPECT_EQ(OB_SUCCESS, coordinator.wait(
      same_key, flight, ObTimeUtility::current_time() + 1000000));
}

TEST(SimpleSchemaKVCacheConcurrency, singleflight_stale_finish_preserves_replacement)
{
  ObSchemaLoadCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init());
  ObSchemaLoadKey key(LOAD_ID_RESOLVE, 1001, 1, 9988);
  bool leader = false;
  uint64_t old_flight_id = 0;
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key, leader, old_flight_id));
  ASSERT_TRUE(leader);
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key, old_flight_id));

  uint64_t replacement_flight_id = 0;
  ASSERT_EQ(OB_SUCCESS,
            coordinator.try_acquire(key, leader, replacement_flight_id));
  ASSERT_TRUE(leader);
  ASSERT_NE(old_flight_id, replacement_flight_id);

  // Simulate a delayed retry from the old leader after a new flight has been
  // installed for the same key.  It must not erase the replacement.
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key, old_flight_id));
  uint64_t observed_flight_id = 0;
  ASSERT_EQ(OB_SUCCESS,
            coordinator.try_acquire(key, leader, observed_flight_id));
  EXPECT_FALSE(leader);
  EXPECT_EQ(replacement_flight_id, observed_flight_id);
  EXPECT_EQ(OB_SUCCESS, coordinator.finish(key, replacement_flight_id));
}

TEST(SimpleSchemaKVCacheConcurrency, singleflight_ignores_unrelated_slot_broadcast)
{
  ObSchemaLoadCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init());
  ObSchemaLoadKey key1(LOAD_ID_RESOLVE, 1001, 1, 100);
  ObSchemaLoadKey key2;
  for (uint64_t physical_hash = 101; ; ++physical_hash) {
    key2 = ObSchemaLoadKey(LOAD_ID_RESOLVE, 1001, 1, physical_hash);
    if (key1.hash() % 64 == key2.hash() % 64 && !(key1 == key2)) {
      break;
    }
  }
  bool leader = false;
  uint64_t flight_id1 = 0;
  uint64_t flight_id2 = 0;
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key1, leader, flight_id1));
  ASSERT_TRUE(leader);
  ASSERT_EQ(OB_SUCCESS, coordinator.try_acquire(key2, leader, flight_id2));
  ASSERT_TRUE(leader);

  std::atomic<bool> wait_done(false);
  std::atomic<int> wait_ret(OB_ERR_UNEXPECTED);
  std::thread waiter([&]() {
    wait_ret.store(coordinator.wait(
        key1, flight_id1,
        ObTimeUtility::current_time() + 2L * 1000L * 1000L));
    wait_done.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key2, flight_id2));
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(wait_done.load());
  ASSERT_EQ(OB_SUCCESS, coordinator.finish(key1, flight_id1));
  waiter.join();
  EXPECT_EQ(OB_SUCCESS, wait_ret.load());
}

TEST(SimpleSchemaKVCacheConcurrency, load_guard_clears_output_on_reentry)
{
  ObSchemaLoadCoordinator coordinator;
  ASSERT_EQ(OB_SUCCESS, coordinator.init());
  ObSchemaLoadGuard guard;
  ObSchemaLoadKey key(ObSimpleSchemaCacheKey(1001, 500001, 100));
  bool leader = false;
  uint64_t flight_id = 0;
  ASSERT_EQ(OB_SUCCESS, guard.acquire(coordinator, key, leader, flight_id));
  ASSERT_TRUE(leader);
  ASSERT_NE(0U, flight_id);
  leader = true;
  flight_id = 42;
  EXPECT_EQ(OB_INIT_TWICE,
            guard.acquire(coordinator, key, leader, flight_id));
  EXPECT_FALSE(leader);
  EXPECT_EQ(0U, flight_id);
}

TEST(SimpleSchemaKVCacheConcurrency, publish_state_map_creates_one_state)
{
  ObTenantSchemaPublishStateMap state_map;
  ASSERT_EQ(OB_SUCCESS, state_map.init());
  static const int64_t THREAD_COUNT = 8;
  ObTenantSchemaPublishState *states[THREAD_COUNT] = {};
  std::atomic<int> failures(0);
  std::thread threads[THREAD_COUNT];
  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i] = std::thread([&, i]() {
      if (OB_SUCCESS != state_map.get_or_create(1001, 100, states[i])) {
        failures.fetch_add(1);
      }
    });
  }
  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i].join();
  }
  ASSERT_EQ(0, failures.load());
  ASSERT_NE(nullptr, states[0]);
  for (int64_t i = 1; i < THREAD_COUNT; ++i) {
    EXPECT_EQ(states[0], states[i]);
  }
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, states[0]->read_stable(token));
  EXPECT_EQ(100, token.refreshed_version_);
  ObTenantSchemaPublishState *same_state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get_or_create(1001, 200, same_state));
  EXPECT_EQ(states[0], same_state);
  ASSERT_EQ(OB_SUCCESS, same_state->read_stable(token));
  // A repeated lifecycle initialization is idempotent; only publication commit
  // may advance an existing frontier.
  EXPECT_EQ(100, token.refreshed_version_);
  ObTenantSchemaPublishState *state = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, state_map.get_or_create(1001, -1, state));
  EXPECT_EQ(nullptr, state);
}

TEST(SimpleSchemaKVCacheConcurrency, publish_state_map_retire_preserves_address_and_epoch)
{
  ObTenantSchemaPublishStateMap state_map;
  ASSERT_EQ(OB_SUCCESS, state_map.init());
  ObTenantSchemaPublishState *state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get_or_create(1001, 100, state));
  ASSERT_NE(nullptr, state);
  ObTenantSchemaPublishState *const original_state = state;
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(before));

  ASSERT_EQ(OB_SUCCESS, state_map.retire(1001));
  state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get(1001, state));
  ASSERT_EQ(original_state, state);
  ObSchemaPublishSnapshot retired;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(retired));
  EXPECT_GT(retired.cache_generations_.id_resolve_,
            before.cache_generations_.id_resolve_);
  EXPECT_GT(retired.cache_generations_.name_resolve_,
            before.cache_generations_.name_resolve_);
  EXPECT_GT(retired.cache_generations_.tablegroup_,
            before.cache_generations_.tablegroup_);
  EXPECT_GT(retired.refresh_seq_, before.refresh_seq_);
  EXPECT_EQ(before.refreshed_version_, retired.refreshed_version_);

  // Reinitializing the schema-store lifecycle reuses the stable state address;
  // only a real publication is allowed to advance its frontier.
  ObTenantSchemaPublishState *reinitialized_state = NULL;
  ASSERT_EQ(OB_SUCCESS,
            state_map.get_or_create(1001, 200, reinitialized_state));
  ASSERT_EQ(original_state, reinitialized_state);
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(*reinitialized_state));
  ASSERT_EQ(OB_SUCCESS, publication.commit(200));
  ObSchemaPublishSnapshot reinitialized;
  ASSERT_EQ(OB_SUCCESS, reinitialized_state->read_stable(reinitialized));
  EXPECT_EQ(200, reinitialized.refreshed_version_);
  EXPECT_EQ(retired.cache_generations_, reinitialized.cache_generations_);
}

TEST(SimpleSchemaKVCacheConcurrency, retained_reader_is_safe_during_tenant_retirement)
{
  ObTenantSchemaPublishStateMap state_map;
  ASSERT_EQ(OB_SUCCESS, state_map.init());
  ObTenantSchemaPublishState *state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get_or_create(1001, 100, state));
  ASSERT_NE(nullptr, state);
  ObTenantSchemaPublishState *const retained_state = state;
  std::atomic<bool> reader_started(false);
  std::atomic<bool> stop(false);
  std::atomic<int> reader_ret(OB_SUCCESS);

  std::thread reader([&]() {
    reader_started.store(true, std::memory_order_release);
    while (!stop.load(std::memory_order_acquire)
           && OB_SUCCESS == reader_ret.load(std::memory_order_acquire)) {
      ObSchemaPublishSnapshot snapshot;
      const int tmp_ret = retained_state->read_stable(
          snapshot, ObTimeUtility::current_time() + 1000L * 1000L);
      if (OB_SUCCESS != tmp_ret) {
        reader_ret.store(tmp_ret, std::memory_order_release);
      }
    }
  });
  while (!reader_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  int retire_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == retire_ret && i < 64; ++i) {
    retire_ret = state_map.retire(1001);
  }
  stop.store(true, std::memory_order_release);
  reader.join();

  EXPECT_EQ(OB_SUCCESS, retire_ret);
  EXPECT_EQ(OB_SUCCESS, reader_ret.load(std::memory_order_acquire));
  ObTenantSchemaPublishState *current_state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get(1001, current_state));
  EXPECT_EQ(retained_state, current_state);
}

TEST(SimpleSchemaKVCachePolicy, incremental_refresh_falls_back_on_unsafe_or_large_stream)
{
  typedef ObSchemaCacheIncrementalRefreshPolicy Policy;
  EXPECT_FALSE(Policy::should_fallback(
      Policy::MAX_OPERATION_COUNT, Policy::MAX_DDL_BOUNDARY_COUNT, false));
  EXPECT_TRUE(Policy::should_fallback(
      Policy::MAX_OPERATION_COUNT + 1, 0, false));
  EXPECT_TRUE(Policy::should_fallback(
      1, Policy::MAX_DDL_BOUNDARY_COUNT + 1, false));
  // Old/synthetic streams without END_SIGN must use a new generation because
  // their historical publication boundary cannot be reconstructed safely.
  EXPECT_TRUE(Policy::should_fallback(1, 1, true));
}

TEST(SimpleSchemaKVCachePolicy, stale_retry_and_missing_worker_timeout_are_bounded)
{
  typedef ObOnDemandSchemaRetryPolicy Policy;
  const int64_t now = 1000;
  EXPECT_EQ(now + Policy::DEFAULT_LOAD_TIMEOUT_US,
            Policy::get_load_deadline(false, 0, now));
  EXPECT_EQ(4321, Policy::get_load_deadline(true, 4321, now));

  int64_t retry_count = Policy::MAX_STALE_RETRY_COUNT - 1;
  int64_t backoff_us = 0;
  ASSERT_EQ(OB_SUCCESS,
            Policy::calc_backoff_info(100000, now, retry_count, backoff_us));
  EXPECT_EQ(Policy::MAX_STALE_RETRY_COUNT, retry_count);
  EXPECT_GT(backoff_us, 0);
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            Policy::calc_backoff_info(100000, now, retry_count, backoff_us));
  EXPECT_EQ(0, backoff_us);
  EXPECT_EQ(OB_TIMEOUT,
            Policy::calc_backoff_info(now, now, retry_count, backoff_us));
}

TEST(SimpleSchemaKVCacheConcurrency, point_lookup_requires_lifecycle_state_and_cannot_advance_frontier)
{
  ObTenantSchemaPublishStateMap state_map;
  ASSERT_EQ(OB_SUCCESS, state_map.init());
  ObTenantSchemaPublishState *state = NULL;
  ObSchemaPublishSnapshot token;
  const int64_t deadline = ObTimeUtility::current_time() + 100L * 1000L;
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            state_map.get_published_snapshot(1001, 200, deadline, state, token));
  EXPECT_EQ(nullptr, state);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, state_map.get(1001, state));

  ASSERT_EQ(OB_SUCCESS, state_map.get_or_create(1001, 100, state));
  ASSERT_NE(nullptr, state);
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            state_map.get_published_snapshot(1001, 101, deadline, state, token));
  ASSERT_EQ(OB_SUCCESS,
            state_map.get_published_snapshot(1001, 100, deadline, state, token));
  EXPECT_EQ(100, token.refreshed_version_);
}

TEST(SimpleSchemaKVCacheConcurrency, schema_mgr_info_keeps_publish_state_pointer)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 100));

  ObSchemaMgrInfo info;
  info.set_publish_state(&state);
  EXPECT_EQ(&state, info.get_publish_state());

  ObSchemaMgrInfo copied(info);
  EXPECT_EQ(&state, copied.get_publish_state());
  copied.reset();
  EXPECT_EQ(nullptr, copied.get_publish_state());
}

TEST(SimpleSchemaKVCacheConcurrency, publication_rejects_frontier_regression)
{
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(1001, 200));
  {
    ObTenantSchemaPublicationGuard publication;
    ASSERT_EQ(OB_SUCCESS, publication.begin(state));
    EXPECT_EQ(OB_INVALID_ARGUMENT, publication.commit(100));
  }
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  EXPECT_EQ(200, token.refreshed_version_);
}

TEST_F(TestSimpleSchemaKVCache, resolve_cache_cow_keeps_concurrent_ranges)
{
  ObSchemaResolveCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  ObSchemaIDResolveKey key(
      TEST_TENANT_ID, token.cache_generations_.id_resolve_, 500001);
  ObSchemaResolveRange range1 = make_exist_range(20, 29, 20, 1);
  ObSchemaResolveRange range2 = make_exist_range(30, OB_SCHEMA_VERSION_INF, 30, 2);

  std::atomic<int> ret1(OB_ERR_UNEXPECTED);
  std::atomic<int> ret2(OB_ERR_UNEXPECTED);
  std::thread writer1([&]() {
    ret1.store(cache.publish_if_current(key, range1, state, token));
  });
  std::thread writer2([&]() {
    ret2.store(cache.publish_if_current(key, range2, state, token));
  });
  writer1.join();
  writer2.join();
  ASSERT_EQ(OB_SUCCESS, ret1.load());
  ASSERT_EQ(OB_SUCCESS, ret2.load());

  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, cache.get(key, 25, result));
  EXPECT_EQ(20, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, cache.get(key, 35, result));
  EXPECT_EQ(30, result.precise_schema_version_);

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  ASSERT_EQ(OB_SUCCESS, publication.commit(110));
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            cache.publish_if_current(key, make_exist_range(40, 49, 40, 3),
                                     state, token));
}

TEST_F(TestSimpleSchemaKVCache, resolve_cache_writes_do_not_count_as_reads)
{
  ObSchemaResolveCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  const ObSchemaIDResolveKey id_key(TEST_TENANT_ID, 1, 500001);
  const ObSchemaNameResolveKey name_key(
      TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE, ObString::make_string("table_a"));
  // Both loader merges and incremental repairs must leave read statistics alone.
  for (int64_t version = 80; version <= 100; version += 20) {
    ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(id_key,
        make_exist_range(version, OB_SCHEMA_VERSION_INF, version, 0), state, token));
    ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(name_key,
        make_name_range(version, OB_SCHEMA_VERSION_INF, 500001, version, true), state, token));
  }
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, cache.refresh(id_key,
      make_exist_range(120, OB_SCHEMA_VERSION_INF, 120, 0)));
  ASSERT_EQ(OB_SUCCESS, cache.refresh(name_key,
      make_name_range(120, OB_SCHEMA_VERSION_INF, 500001, 120, true)));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ObArray<ObKVCacheInstHandle> inst_handles;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().get_cache_inst_info(
      OB_SERVER_TENANT_ID, inst_handles));
  ASSERT_EQ(2, inst_handles.count());
  for (int64_t i = 0; i < inst_handles.count(); ++i) {
    ASSERT_NE(nullptr, inst_handles.at(i).get_inst());
    EXPECT_EQ(0, inst_handles.at(i).get_inst()->status_.total_hit_cnt_.value());
  }
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, cache.get(id_key, 120, result));
  EXPECT_EQ(120, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, cache.get(name_key, 120, result));
  EXPECT_EQ(120, result.precise_schema_version_);
  for (int64_t i = 0; i < inst_handles.count(); ++i) {
    EXPECT_EQ(1, inst_handles.at(i).get_inst()->status_.total_hit_cnt_.value());
  }
}

TEST_F(TestSimpleSchemaKVCache, on_demand_kvcache_hit_and_miss_stat_events)
{
  lib::ObUnitTestEnableDiagnoseGuard enable_diagnose_guard;
  ObDiagnosticInfoSlot slot;
  ObTenantDiagnosticInfoSummaryGuard diagnostic_guard(&slot);
  ObSchemaResolveCache resolve_cache;
  ObSimpleSchemaObjectCache object_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, object_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());

  const ObSchemaIDResolveKey id_key(TEST_TENANT_ID, 1, 500001);
  ObSchemaResolveResult resolve_result;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, resolve_cache.get(id_key, 100, resolve_result));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SCHEMA_ID_RESOLVE_CACHE_MISS));
  const ObSchemaResolveRange id_range = make_exist_range(100, 110, 100, 1);
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache, id_key, id_range));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(id_key, 105, resolve_result));
  // A KV hit whose cached range does not cover the requested version is not a KV miss.
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, resolve_cache.get(id_key, 120, resolve_result));
  const ObSchemaIDResolveKey invalid_id_key;
  EXPECT_EQ(OB_INVALID_ARGUMENT, resolve_cache.get(invalid_id_key, 120, resolve_result));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SCHEMA_ID_RESOLVE_CACHE_MISS));
  EXPECT_EQ(2, get_stat_event_value(slot, ObStatEventIds::SCHEMA_ID_RESOLVE_CACHE_HIT));

  const ObSchemaNameResolveKey name_key(
      TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE, ObString::make_string("t1"));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, resolve_cache.get(name_key, 100, resolve_result));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SCHEMA_NAME_RESOLVE_CACHE_MISS));
  const ObSchemaResolveRange name_range = make_name_range(100, 110, 500001, 100, true);
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache, name_key, name_range));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(name_key, 105, resolve_result));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, resolve_cache.get(name_key, 120, resolve_result));
  const ObSchemaNameResolveKey invalid_name_key;
  EXPECT_EQ(OB_INVALID_ARGUMENT, resolve_cache.get(invalid_name_key, 120, resolve_result));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SCHEMA_NAME_RESOLVE_CACHE_MISS));
  EXPECT_EQ(2, get_stat_event_value(slot, ObStatEventIds::SCHEMA_NAME_RESOLVE_CACHE_HIT));

  const ObSimpleSchemaCacheKey object_key(TEST_TENANT_ID, 500001, 100);
  const ObSimpleTableSchemaV2 *cached_schema = NULL;
  ObKVCacheHandle object_handle;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, object_cache.get(object_key, cached_schema, object_handle));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SIMPLE_SCHEMA_OBJECT_CACHE_MISS));
  ObSimpleTableSchemaV2 schema;
  init_simple_table(schema, 500001, 100, "t1");
  ASSERT_EQ(OB_SUCCESS,
            object_cache.put_and_fetch(object_key, schema, cached_schema, object_handle));
  object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, object_cache.get(object_key, cached_schema, object_handle));
  const ObSimpleSchemaCacheKey invalid_object_key;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            object_cache.get(invalid_object_key, cached_schema, object_handle));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SIMPLE_SCHEMA_OBJECT_CACHE_MISS));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::SIMPLE_SCHEMA_OBJECT_CACHE_HIT));

  ObArray<uint64_t> table_ids;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            tablegroup_cache.get(TEST_TENANT_ID, 1, 500020, table_ids));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::TABLEGROUP_TABLE_ID_CACHE_MISS));
  ObSEArray<uint64_t, 1> input_table_ids;
  ASSERT_EQ(OB_SUCCESS, input_table_ids.push_back(500001));
  ASSERT_EQ(OB_SUCCESS,
            tablegroup_cache.put(TEST_TENANT_ID, 1, 500020, input_table_ids));
  ASSERT_EQ(OB_SUCCESS,
            tablegroup_cache.get(TEST_TENANT_ID, 1, 500020, table_ids));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            tablegroup_cache.get(TEST_TENANT_ID, 1, OB_INVALID_ID, table_ids));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::TABLEGROUP_TABLE_ID_CACHE_MISS));
  EXPECT_EQ(1, get_stat_event_value(slot, ObStatEventIds::TABLEGROUP_TABLE_ID_CACHE_HIT));
}

TEST_F(TestSimpleSchemaKVCache, on_demand_collects_lobs_outside_resident_schema_mgr)
{
  const int64_t schema_version = 100;
  IncrementalHistorySchemaService history(nullptr, 0, nullptr, 0);
  IsolatedIncrementalSchemaService service(history);
  ObMySQLProxy unused_sql_proxy;
  service.sql_proxy_ = &unused_sql_proxy;
  service.config_ = &GCONF;
  service.init_ = true;
  ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
  ASSERT_EQ(OB_SUCCESS, service.get_simple_schema_object_cache().init());
  ASSERT_EQ(OB_SUCCESS, service.simple_schema_on_demand_coordinator_.init(&service));
  ObTenantSchemaPublishState state;
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, schema_version));
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));

  ObSchemaMgr resident_mgr;
  ASSERT_EQ(OB_SUCCESS, resident_mgr.init());
  const ObSchemaMgr *mgr = &resident_mgr;
  ObSchemaMgrHandle handle(ObSchemaMgrItem::MOD_STACK);
  ObRefreshSchemaStatus status;
  status.tenant_id_ = TEST_TENANT_ID;
  ObSchemaMgrInfo mgr_info(TEST_TENANT_ID, schema_version, mgr, handle, status);
  mgr_info.set_publish_state(&state);
  ObTableSchema owners[3];
  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.init(ObSimpleSchemaMode::ON_DEMAND));
  guard.schema_service_ = &service;
  guard.schema_guard_type_ = ObSchemaGetterGuard::SCHEMA_GUARD;
  ASSERT_EQ(OB_SUCCESS, guard.schema_mgr_infos_.push_back(mgr_info));

  // Populate the real guard/object caches only: nonresident user schemas must
  // still be found even though the old ObSchemaMgr auxiliary lookup is empty.
  for (int64_t i = 0; i < 3; ++i) {
    ObTableSchema &owner = owners[i];
    const uint64_t owner_id = 500001 + i * 3;
    init_simple_table(owner, owner_id, schema_version, "lob_owner");
    owner.set_table_type(1 == i ? USER_INDEX : USER_TABLE);
    if (1 == i) {
      owner.set_data_table_id(owners[0].get_table_id());
      owner.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
    }
    owner.set_aux_lob_meta_tid(i < 2 ? owner_id + 1 : OB_INVALID_ID);
    owner.set_aux_lob_piece_tid(i < 2 ? owner_id + 2 : OB_INVALID_ID);
    SchemaObj pinned_owner;
    pinned_owner.schema_type_ = TABLE_SCHEMA;
    pinned_owner.tenant_id_ = TEST_TENANT_ID;
    pinned_owner.schema_id_ = owner_id;
    pinned_owner.schema_ = &owner;
    ASSERT_EQ(OB_SUCCESS, guard.schema_objs_.push_back(pinned_owner));
    for (int64_t j = 0; i < 2 && j < OB_AUX_LOB_TABLE_CNT; ++j) {
      const uint64_t lob_id = owner_id + j + 1;
      ObSimpleTableSchemaV2 lob;
      init_simple_table(lob, lob_id, schema_version, "lob_aux");
      lob.set_table_type(0 == j ? AUX_LOB_META : AUX_LOB_PIECE);
      lob.set_data_table_id(owner_id);
      const ObSimpleTableSchemaV2 *cached_lob = nullptr;
      ObKVCacheHandle object_handle;
      ASSERT_EQ(OB_SUCCESS, service.get_simple_schema_object_cache().put_and_fetch(
          ObSimpleSchemaCacheKey(TEST_TENANT_ID, lob_id, schema_version),
          lob, cached_lob, object_handle));
      ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
          ObSchemaIDResolveKey(TEST_TENANT_ID, token.cache_generations_.id_resolve_, lob_id),
          make_exist_range(schema_version, OB_SCHEMA_VERSION_INF, schema_version, 0, lob_id),
          state, token));
    }
  }

  ObArray<const ObSimpleTableSchemaV2 *> lob_schemas;
  ASSERT_EQ(OB_SUCCESS, resident_mgr.get_aux_schemas(
      TEST_TENANT_ID, owners[0].get_table_id(), lob_schemas, AUX_LOB_META));
  ASSERT_EQ(0, lob_schemas.count());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, guard.get_lob_schemas_with_data_table_id(
        TEST_TENANT_ID, owners[i].get_table_id(), lob_schemas));
    ASSERT_EQ(i == 0 ? 2 : 4, lob_schemas.count());
  }
  for (int64_t i = 0; i < lob_schemas.count(); ++i) {
    ASSERT_NE(nullptr, lob_schemas.at(i));
    EXPECT_EQ(500002 + (i / 2) * 3 + i % 2, lob_schemas.at(i)->get_table_id());
    EXPECT_EQ(owners[i / 2].get_table_id(), lob_schemas.at(i)->get_data_table_id());
    EXPECT_EQ(i % 2 == 0 ? AUX_LOB_META : AUX_LOB_PIECE, lob_schemas.at(i)->get_table_type());
  }
}

TEST_F(TestSimpleSchemaKVCache, resolve_cache_rejects_mismatched_namespace)
{
  ObSchemaResolveCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));

  ObSchemaIDResolveKey id_key(
      TEST_TENANT_ID, token.cache_generations_.id_resolve_, 500001);
  ObSchemaResolveRange wrong_id_range(20, 29, 500001, 20, 1,
                                      NS_NORMAL_TABLE, true);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            cache.publish_if_current(id_key, wrong_id_range, state, token));

  ObSchemaNameResolveKey name_key(TEST_TENANT_ID,
                                  token.cache_generations_.name_resolve_,
                                  500010, 0, NS_NORMAL_TABLE,
                                  ObString::make_string("t1"));
  ObSchemaResolveRange wrong_name_range = make_exist_range(20, 29, 20, 2);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            cache.publish_if_current(name_key, wrong_name_range, state, token));
  ObSchemaResolveRange name_range(20, 29, 500001, 20, 3,
                                  NS_NORMAL_TABLE, true);
  EXPECT_EQ(OB_SUCCESS,
            cache.publish_if_current(name_key, name_range, state, token));
}

TEST_F(TestSimpleSchemaKVCache, refresh_crossing_rejects_old_loader_generation)
{
  ObSchemaResolveCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot old_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(old_token));
  ObSchemaIDResolveKey old_key(
      TEST_TENANT_ID, old_token.cache_generations_.id_resolve_, 500001);
  ASSERT_EQ(OB_SUCCESS,
            cache.publish_if_current(old_key, make_exist_range(10, OB_SCHEMA_VERSION_INF, 10, 1),
                                     state, old_token));

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            cache.publish_if_current(old_key, make_exist_range(20, OB_SCHEMA_VERSION_INF, 20, 2),
                                     state, old_token));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, cache.get(old_key, 25, result));
  EXPECT_EQ(10, result.precise_schema_version_);
  result.reset();
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            cache.get_if_current(old_key, 25, state, old_token, result));
  EXPECT_FALSE(result.is_valid());
  ObSchemaPublishSnapshot new_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(new_token));
  ObSchemaIDResolveKey new_key(
      TEST_TENANT_ID, new_token.cache_generations_.id_resolve_, 500001);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(new_key, 25, result));
}

TEST_F(TestSimpleSchemaKVCache, retired_tenant_epoch_cannot_reuse_old_cache_entries)
{
  ObSchemaResolveCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishStateMap state_map;
  ASSERT_EQ(OB_SUCCESS, state_map.init());
  ObTenantSchemaPublishState *state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get_or_create(TEST_TENANT_ID, 100, state));
  ASSERT_NE(nullptr, state);
  ObSchemaPublishSnapshot old_token;
  ASSERT_EQ(OB_SUCCESS, state->read_stable(old_token));

  ObSchemaIDResolveKey old_key(
      TEST_TENANT_ID, old_token.cache_generations_.id_resolve_, 500001);
  ObSchemaResolveRange old_range(
      10, OB_SCHEMA_VERSION_INF, 500001, 10, 0, NS_MAX, true);
  ASSERT_EQ(OB_SUCCESS,
            cache.publish_if_current(old_key, old_range, *state, old_token));

  ASSERT_EQ(OB_SUCCESS, state_map.retire(TEST_TENANT_ID));
  ObTenantSchemaPublishState *current_state = NULL;
  ASSERT_EQ(OB_SUCCESS, state_map.get(TEST_TENANT_ID, current_state));
  ASSERT_EQ(state, current_state);
  ObSchemaPublishSnapshot current_token;
  ASSERT_EQ(OB_SUCCESS, current_state->read_stable(current_token));
  ASSERT_GT(current_token.cache_generations_.id_resolve_,
            old_token.cache_generations_.id_resolve_);

  // The old physical value may stay in the process cache, but the current
  // lifecycle can only construct a key from its new generation.
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, cache.get(old_key, 50, result));
  ObSchemaIDResolveKey current_key(
      TEST_TENANT_ID, current_token.cache_generations_.id_resolve_, 500001);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get_if_current(
      current_key, 50, *current_state, current_token, result));
  EXPECT_FALSE(result.is_valid());
}

TEST_F(TestSimpleSchemaKVCache, object_cache_survives_metadata_publication)
{
  ObSimpleSchemaObjectCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));

  ObSimpleTableSchemaV2 old_table;
  ObSimpleTableSchemaV2 new_table;
  init_simple_table(old_table, 500001, 100, "old_name");
  init_simple_table(new_table, 500001, 120, "new_name");
  const ObSimpleSchemaCacheKey old_key(TEST_TENANT_ID, 500001, 100);
  const ObSimpleSchemaCacheKey new_key(TEST_TENANT_ID, 500001, 120);
  const ObSimpleTableSchemaV2 *old_cached = NULL;
  const ObSimpleTableSchemaV2 *new_cached = NULL;
  ObKVCacheHandle old_handle;
  ObKVCacheHandle new_handle;
  ASSERT_EQ(OB_SUCCESS, cache.put_and_fetch(old_key, old_table, old_cached, old_handle));

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation());
  ASSERT_FALSE(state.can_publish(before));
  // Object hits and late puts work even while metadata publication is odd.
  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, cache.get(old_key, cached, handle));
  EXPECT_EQ(old_cached, cached);
  ASSERT_EQ(OB_SUCCESS, cache.put_and_fetch(new_key, new_table, new_cached, new_handle));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ASSERT_EQ(OB_SUCCESS, cache.get(old_key, cached, handle));
  EXPECT_EQ(old_cached, cached);
  EXPECT_EQ(100, cached->get_schema_version());
  ASSERT_EQ(OB_SUCCESS, cache.get(new_key, cached, handle));
  EXPECT_EQ(new_cached, cached);
  EXPECT_EQ(120, cached->get_schema_version());
  EXPECT_EQ(ObString("old_name"), old_cached->get_table_name_str());
  EXPECT_EQ(ObString("new_name"), new_cached->get_table_name_str());
}

TEST_F(TestSimpleSchemaKVCache, object_cache_checks_exact_identity)
{
  ObSimpleSchemaObjectCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObSimpleTableSchemaV2 table;
  init_simple_table(table, 500001, 100, "table_a");
  const ObSimpleSchemaCacheKey key(TEST_TENANT_ID, 500001, 100);
  const ObSimpleSchemaCacheKey other_keys[] = {
    ObSimpleSchemaCacheKey(TEST_TENANT_ID + 1, 500001, 100),
    ObSimpleSchemaCacheKey(TEST_TENANT_ID, 500002, 100),
    ObSimpleSchemaCacheKey(TEST_TENANT_ID, 500001, 120)
  };
  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, cache.put_and_fetch(key, table, cached, handle));
  for (int64_t i = 0; i < ARRAYSIZEOF(other_keys); ++i) {
    EXPECT_FALSE(key == other_keys[i]);
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              cache.put_and_fetch(other_keys[i], table, cached, handle));
    EXPECT_EQ(nullptr, cached);
    EXPECT_FALSE(handle.is_valid());
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(other_keys[i], cached, handle));
  }
  // KVCache owns a deep copy of the name and object.
  ASSERT_EQ(OB_SUCCESS, table.set_table_name("changed_source"));
  ASSERT_EQ(OB_SUCCESS, cache.get(key, cached, handle));
  EXPECT_EQ(ObString("table_a"), cached->get_table_name_str());
}

TEST_F(TestSimpleSchemaKVCache, metadata_fallback_reuses_id_and_object_cache)
{
  ObSchemaResolveCache resolve_cache;
  ObSimpleSchemaObjectCache object_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, object_cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));

  ObSchemaIDResolveKey id_key(
      TEST_TENANT_ID, before.cache_generations_.id_resolve_, 500001);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      id_key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0),
      state, before));
  ObSimpleTableSchemaV2 table;
  init_simple_table(table, 500001, 100, "table_a");
  ObSimpleSchemaCacheKey object_key(
      TEST_TENANT_ID, 500001, 100);
  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, object_cache.put_and_fetch(
      object_key, table, cached, handle));
  handle.reset();

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation(
      SCHEMA_CACHE_DOMAIN_NAME_RESOLVE | SCHEMA_CACHE_DOMAIN_TABLEGROUP));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(after));

  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS,
            resolve_cache.get_if_current(id_key, 120, state, after, result));
  EXPECT_EQ(100, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS,
            object_cache.get(object_key, cached, handle));
  ASSERT_NE(nullptr, cached);
  EXPECT_EQ(100, cached->get_schema_version());
}

TEST_F(TestSimpleSchemaKVCache, tablegroup_cache_generation_isolates_refresh_epochs)
{
  ObTablegroupTableIdCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObSEArray<uint64_t, 2> old_ids;
  ASSERT_EQ(OB_SUCCESS, old_ids.push_back(500001));
  ASSERT_EQ(OB_SUCCESS, cache.put(TEST_TENANT_ID, 1, 500010, old_ids));

  ObArray<uint64_t> result;
  ASSERT_EQ(OB_SUCCESS, cache.get(TEST_TENANT_ID, 1, 500010, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(500001U, result.at(0));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(TEST_TENANT_ID, 2, 500010, result));

  ObSEArray<uint64_t, 2> new_ids;
  ASSERT_EQ(OB_SUCCESS, new_ids.push_back(500002));
  ASSERT_EQ(OB_SUCCESS, cache.put(TEST_TENANT_ID, 2, 500010, new_ids));
  ASSERT_EQ(OB_SUCCESS, cache.get(TEST_TENANT_ID, 2, 500010, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(500002U, result.at(0));
}

TEST_F(TestSimpleSchemaKVCache, object_cache_is_first_writer_wins_and_handle_pins_value)
{
  ObSimpleSchemaObjectCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObSimpleTableSchemaV2 first;
  first.set_tenant_id(TEST_TENANT_ID);
  first.set_table_id(500001);
  first.set_database_id(500010);
  first.set_schema_version(100);
  ASSERT_EQ(OB_SUCCESS, first.set_table_name("first_name"));

  ObSimpleSchemaCacheKey key(TEST_TENANT_ID, 500001, 100);
  const ObSimpleTableSchemaV2 *cached = NULL;
  ObKVCacheHandle handle;
  ASSERT_EQ(OB_SUCCESS, cache.put_and_fetch(key, first, cached, handle));
  ASSERT_NE(nullptr, cached);
  ASSERT_TRUE(handle.is_valid());
  EXPECT_NE(&first, cached);
  EXPECT_EQ(ObString("first_name"), cached->get_table_name_str());

  ObSimpleTableSchemaV2 conflicting;
  conflicting.set_tenant_id(TEST_TENANT_ID);
  conflicting.set_table_id(500001);
  conflicting.set_database_id(500010);
  conflicting.set_schema_version(100);
  ASSERT_EQ(OB_SUCCESS, conflicting.set_table_name("conflicting_name"));
  ObKVCacheHandle conflicting_handle;
  ASSERT_EQ(OB_SUCCESS,
            cache.put_and_fetch(key, conflicting, cached, conflicting_handle));
  EXPECT_EQ(ObString("first_name"), cached->get_table_name_str());

  ObKVCacheHandle get_handle;
  ASSERT_EQ(OB_SUCCESS, cache.get(key, cached, get_handle));
  EXPECT_EQ(ObString("first_name"), cached->get_table_name_str());
  const ObSimpleTableSchemaV2 *pinned = cached;
  ASSERT_EQ(OB_SUCCESS, cache.erase(key));
  get_handle.reset();
  conflicting_handle.reset();
  ASSERT_TRUE(handle.is_valid());
  EXPECT_EQ(ObString("first_name"), pinned->get_table_name_str());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(key, cached, get_handle));
}

TEST_F(TestSimpleSchemaKVCache, fallback_plan_rejects_another_tenant_without_erasing_cache)
{
  typedef ObMultiVersionSchemaService::IncrementalSchemaCacheFallbackPlan Plan;
  ObSchemaResolveCache cache;
  ObTablegroupTableIdCache unused_tablegroup_cache;
  ObTenantSchemaPublishState state;
  ObSchemaPublishSnapshot token;
  ObSchemaResolveResult result;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  Plan plan;
  const ObSchemaIDResolveKey key(OB_SERVER_TENANT_ID, 1, 500001);
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SERVER_TENANT_ID, 100));
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(
      key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, token));
  operation.tenant_id_ = TEST_TENANT_ID;
  operation.table_id_ = 500001;
  operation.op_type_ = OB_DDL_ALTER_TABLE;
  operation.schema_version_ = 110;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
  EXPECT_EQ(OB_STATE_NOT_MATCH, plan.apply(
      OB_SERVER_TENANT_ID, token.cache_generations_, cache, unused_tablegroup_cache, repair_result));
  EXPECT_EQ(OB_SUCCESS, cache.get(key, 100, result));
}

TEST_F(TestSimpleSchemaKVCache, fallback_plan_rejects_invalid_arguments_and_unprepared_state)
{
  typedef ObMultiVersionSchemaService::IncrementalSchemaCacheFallbackPlan Plan;
  ObSchemaResolveCache cache;
  ObTablegroupTableIdCache unused_tablegroup_cache;
  ObTenantSchemaPublishState state;
  ObSchemaPublishSnapshot token;
  ObSchemaCacheGenerations invalid_generations;
  ObSchemaResolveResult result;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  Plan plan;
  const ObSchemaIDResolveKey key(TEST_TENANT_ID, 1, 500001);
  const uint64_t invalid_tenants[] = {OB_INVALID_TENANT_ID, OB_INVALID_ID};
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(
      key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, token));
  EXPECT_EQ(OB_NOT_INIT, plan.apply(TEST_TENANT_ID, token.cache_generations_,
                                   cache, unused_tablegroup_cache, repair_result));
  operation.tenant_id_ = TEST_TENANT_ID;
  operation.table_id_ = 500001;
  operation.op_type_ = OB_DDL_ALTER_TABLE;
  operation.schema_version_ = 110;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  invalid_generations.reset();
  ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
  EXPECT_EQ(OB_INVALID_ARGUMENT, plan.apply(TEST_TENANT_ID, invalid_generations,
                                            cache, unused_tablegroup_cache, repair_result));
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_tenants); ++i) {
    ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
    EXPECT_EQ(OB_INVALID_ARGUMENT, plan.apply(invalid_tenants[i], token.cache_generations_,
                                              cache, unused_tablegroup_cache, repair_result));
    EXPECT_EQ(OB_INVALID_ARGUMENT, plan.prepare(invalid_tenants[i], operations));
    // A failed preparation must not leave a previously valid plan executable.
    EXPECT_EQ(OB_NOT_INIT, plan.apply(TEST_TENANT_ID, token.cache_generations_,
                                     cache, unused_tablegroup_cache, repair_result));
    EXPECT_EQ(0, plan.get_table_id_count());
    EXPECT_EQ(SCHEMA_CACHE_DOMAIN_NONE, repair_result.bump_domains_);
    EXPECT_EQ(OB_SUCCESS, cache.get(key, 100, result));
  }
}

TEST_F(TestSimpleSchemaKVCache, fallback_plan_reports_success_when_erase_failure_is_covered)
{
  typedef ObMultiVersionSchemaService::IncrementalSchemaCacheFallbackPlan Plan;
  for (int64_t tablegroup = 0; tablegroup <= 1; ++tablegroup) {
    ObSchemaResolveCache uninitialized_resolve_cache;
    ObTablegroupTableIdCache uninitialized_tablegroup_cache;
    ObTenantSchemaPublishState state;
    ObSchemaRefreshPublication publication;
    ObSchemaCacheRepairResult repair_result;
    ObSchemaService::SchemaOperationSetWithAlloc operations;
    ObSchemaOperation operation;
    Plan plan;
    const uint64_t expected_domains = tablegroup ? SCHEMA_CACHE_DOMAIN_TABLEGROUP
                                                : SCHEMA_CACHE_DOMAIN_METADATA;
    SCOPED_TRACE(tablegroup);
    ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    operation.tenant_id_ = TEST_TENANT_ID;
    operation.table_id_ = 500001;
    operation.tablegroup_id_ = 500010;
    operation.op_type_ = tablegroup ? OB_DDL_DEL_TABLEGROUP : OB_DDL_ALTER_TABLE;
    operation.schema_version_ = 110;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
    ASSERT_EQ(OB_SUCCESS, publication.begin());
    repair_result.reason_ = OB_NOT_SUPPORTED;
    // An erase failure is handled by domain invalidation, not exposed as an
    // unhandled error that would unnecessarily invalidate unrelated domains.
    ASSERT_EQ(OB_SUCCESS, plan.apply(TEST_TENANT_ID, publication.cache_generations(),
                                     uninitialized_resolve_cache, uninitialized_tablegroup_cache,
                                     repair_result));
    EXPECT_TRUE(repair_result.is_valid());
    EXPECT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
    EXPECT_EQ(OB_NOT_SUPPORTED, repair_result.reason_);
    EXPECT_EQ(expected_domains, repair_result.bump_domains_);
    publication.add_bump_domains(repair_result.bump_domains_);
    ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
    ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
  }
}

TEST_F(TestSimpleSchemaKVCache, fallback_plan_preparation_preserves_reads_and_late_fills)
{
  typedef ObMultiVersionSchemaService::IncrementalSchemaCacheFallbackPlan Plan;
  ObTablegroupTableIdCache cache;
  ObSchemaResolveCache unused_resolve_cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  ObArray<uint64_t> members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(500001));
  ASSERT_EQ(OB_SUCCESS, cache.put(TEST_TENANT_ID, 1, 500010, members));
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  operation.tenant_id_ = TEST_TENANT_ID;
  operation.tablegroup_id_ = 500010;
  operation.op_type_ = OB_DDL_DEL_TABLEGROUP;
  operation.schema_version_ = 110;
  for (int64_t i = 0;
       i < ObSchemaCacheIncrementalRefreshPolicy::MAX_TARGETED_INVALIDATION_OPERATION_COUNT;
       ++i) {
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  }
  Plan plan;
  ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
  ASSERT_EQ(1, plan.get_tablegroup_id_count());
  EXPECT_EQ(0, plan.get_table_id_count());
  EXPECT_EQ(SCHEMA_CACHE_DOMAIN_NONE, plan.get_bump_domains());
  EXPECT_FALSE(publication.is_active());
  ObSchemaPublishSnapshot prepared;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      prepared, ObTimeUtility::current_time() + 1000));
  EXPECT_EQ(before.refresh_seq_, prepared.refresh_seq_);
  ObArray<uint64_t> cached_members;
  ASSERT_EQ(OB_SUCCESS, cache.get(TEST_TENANT_ID, 1, 500010, cached_members));
  ASSERT_EQ(OB_SUCCESS, members.push_back(500002));
  // A lookup admitted before preparation can still fill the old generation.
  ASSERT_EQ(OB_SUCCESS, cache.put_if_current(
      TEST_TENANT_ID, 1, 500010, members, state, before));
  ASSERT_EQ(OB_SUCCESS, publication.begin());
  ObSchemaCacheRepairResult repair_result;
  ASSERT_EQ(OB_SUCCESS, plan.apply(TEST_TENANT_ID, before.cache_generations_,
                                   unused_resolve_cache, cache, repair_result));
  EXPECT_TRUE(repair_result.is_valid());
  EXPECT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(TEST_TENANT_ID, 1, 500010, cached_members));
  EXPECT_EQ(OB_SCHEMA_EAGAIN, cache.put_if_current(
      TEST_TENANT_ID, 1, 500010, members, state, before));
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
  ASSERT_EQ(OB_SUCCESS, state.read_stable(prepared));
  EXPECT_EQ(before.cache_generations_.tablegroup_, prepared.cache_generations_.tablegroup_);

  // Above the independent scan limit, even duplicates use a bounded bump plan.
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
  EXPECT_EQ(SCHEMA_CACHE_DOMAIN_METADATA, plan.get_bump_domains());
  EXPECT_EQ(0, plan.get_tablegroup_id_count());
  EXPECT_EQ(0, plan.get_table_id_count());
}

TEST_F(TestSimpleSchemaKVCache, fallback_plan_unique_key_budget_boundary)
{
  typedef ObSchemaCacheIncrementalRefreshPolicy Policy;
  typedef ObMultiVersionSchemaService::IncrementalSchemaCacheFallbackPlan Plan;
  for (int tablegroup = 0; tablegroup <= 1; ++tablegroup) {
    SCOPED_TRACE(tablegroup);
    ObSchemaService::SchemaOperationSetWithAlloc operations;
    ObSchemaOperation operation;
    operation.tenant_id_ = TEST_TENANT_ID;
    operation.op_type_ = tablegroup ? OB_DDL_DEL_TABLEGROUP : OB_DDL_ALTER_TABLE;
    operation.schema_version_ = 110;
    for (int64_t i = 0; i < Policy::MAX_TARGETED_INVALIDATION_KEY_COUNT; ++i) {
      operation.table_id_ = 500001 + i;
      operation.tablegroup_id_ = operation.table_id_;
      ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
      ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    }
    Plan plan;
    ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
    const uint64_t domain = tablegroup ? SCHEMA_CACHE_DOMAIN_TABLEGROUP
                                      : SCHEMA_CACHE_DOMAIN_ID_RESOLVE;
    EXPECT_EQ(0U, plan.get_bump_domains() & domain);
    EXPECT_EQ(Policy::MAX_TARGETED_INVALIDATION_KEY_COUNT,
              plan.get_table_id_count() + plan.get_tablegroup_id_count());
    operation.table_id_ += 1;
    operation.tablegroup_id_ = operation.table_id_;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    ASSERT_EQ(OB_SUCCESS, plan.prepare(TEST_TENANT_ID, operations));
    EXPECT_EQ(domain, plan.get_bump_domains() & domain);
    EXPECT_EQ(0, plan.get_table_id_count() + plan.get_tablegroup_id_count());
  }
}

TEST_F(TestSimpleSchemaKVCache, incremental_apply_failure_uses_metadata_bump)
{
  const IncrementalHistorySchemaService::TableVersion versions[] = {
    {500001, 100, "before"}, {500001, 110, "after"},
  };
  const IncrementalHistorySchemaService::FetchExpectation fetches[] = {
    {100, 1, 1}, {110, 1, 1},
  };
  IncrementalHistorySchemaService history(versions, ARRAYSIZEOF(versions),
                                          fetches, ARRAYSIZEOF(fetches));
  // Leave caches uninitialized so SQL preparation succeeds and application
  // fails inside the publication window.
  IsolatedIncrementalSchemaService service(history);
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  const ObRefreshSchemaStatus status(
      OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
  const ObServerSchemaService::IncrementalSchemaCacheContext context(
      status, 100, 120, publication);
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  operation.tenant_id_ = OB_SYS_TENANT_ID;
  operation.table_id_ = 500001;
  operation.schema_version_ = 110;
  operation.op_type_ = OB_DDL_ALTER_TABLE;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  operation.table_id_ = OB_INVALID_ID;
  operation.schema_version_ = 120;
  operation.op_type_ = OB_DDL_END_SIGN;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  ObSchemaMgr unused_schema_mgr;
  ObMySQLProxy unused_sql_proxy;
  ObSchemaCacheRepairResult repair_result;
  ASSERT_EQ(OB_SUCCESS, service.refresh_incremental_schema_cache_(
      context, unused_sql_proxy, operations, unused_schema_mgr, repair_result));
  EXPECT_EQ(2, history.get_fetch_count());
  EXPECT_TRUE(publication.is_active());
  EXPECT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
  EXPECT_EQ(OB_NOT_INIT, repair_result.reason_);
  EXPECT_EQ(SCHEMA_CACHE_DOMAIN_METADATA, repair_result.bump_domains_);
  publication.add_bump_domains(repair_result.bump_domains_);
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
  ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(after));
  EXPECT_EQ(120, after.refreshed_version_);
  EXPECT_EQ(2U, after.cache_generations_.id_resolve_);
  EXPECT_EQ(2U, after.cache_generations_.name_resolve_);
  EXPECT_EQ(2U, after.cache_generations_.tablegroup_);
}

TEST_F(TestSimpleSchemaKVCache, targeted_fallback_bounds_unique_deletions)
{
  // Stay below the operation scan limit, but exceed the exact deletion budget.
  // Repeated DDL on one object should still retain unrelated cache entries.
  for (int tablegroup = 0; tablegroup <= 1; ++tablegroup) {
    for (int distinct = 0; distinct <= 1; ++distinct) {
      SCOPED_TRACE(tablegroup);
      SCOPED_TRACE(distinct);
      IncrementalHistorySchemaService history(nullptr, 0, nullptr, 0);
      IsolatedIncrementalSchemaService service(history);
      ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
      ObTenantSchemaPublishState state;
      ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
      ObSchemaPublishSnapshot before;
      ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
      ObSchemaRefreshPublication publication;
      ASSERT_EQ(OB_SUCCESS, publication.init(&state));
      const ObRefreshSchemaStatus status(
          TEST_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      const ObServerSchemaService::IncrementalSchemaCacheContext context(
          status, 100, 120, publication);
      const ObSchemaIDResolveKey id_key(TEST_TENANT_ID, 1, 500001);
      const ObSchemaIDResolveKey unaffected_key(TEST_TENANT_ID, 1, 600001);
      ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
          id_key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, before));
      ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
          unaffected_key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0, 600001),
          state, before));
      ObArray<uint64_t> members;
      ASSERT_EQ(OB_SUCCESS, members.push_back(500001));
      ASSERT_EQ(OB_SUCCESS, service.get_tablegroup_table_id_cache().put(
          TEST_TENANT_ID, 1, 500001, members));
      ASSERT_EQ(OB_SUCCESS, service.get_tablegroup_table_id_cache().put(
          TEST_TENANT_ID, 1, 600001, members));
      ObSchemaService::SchemaOperationSetWithAlloc operations;
      for (int64_t i = 0; i < 4097; ++i) {
        ObSchemaOperation operation;
        operation.tenant_id_ = TEST_TENANT_ID;
        operation.op_type_ = tablegroup ? OB_DDL_ADD_TABLEGROUP : OB_DDL_ALTER_TABLE;
        operation.table_id_ = 500001 + (distinct ? i : 0);
        operation.tablegroup_id_ = operation.table_id_;
        operation.schema_version_ = 110;
        ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
      }
      ObSchemaCacheRepairResult repair_result;
      ObSchemaMgr unused_schema_mgr;
      ObMySQLProxy unused_sql_proxy;
      ASSERT_EQ(OB_SUCCESS, service.refresh_incremental_schema_cache_(
          context, unused_sql_proxy, operations, unused_schema_mgr, repair_result));
      ASSERT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
      const uint64_t domain = tablegroup ? SCHEMA_CACHE_DOMAIN_TABLEGROUP
                                        : SCHEMA_CACHE_DOMAIN_ID_RESOLVE;
      EXPECT_EQ(distinct ? domain : 0, repair_result.bump_domains_ & domain);
      ObSchemaResolveResult result;
      const int lookup_ret = tablegroup
          ? service.get_tablegroup_table_id_cache().get(TEST_TENANT_ID, 1, 500001, members)
          : service.get_schema_resolve_cache().get(id_key, 100, result);
      // A whole-domain bump should not first erase physical entries one by one.
      EXPECT_EQ(distinct ? OB_SUCCESS : OB_ENTRY_NOT_EXIST, lookup_ret);
      publication.add_bump_domains(repair_result.bump_domains_);
      ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
      ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
      ObSchemaPublishSnapshot after;
      ASSERT_EQ(OB_SUCCESS, state.read_stable(after));
      EXPECT_EQ(120, after.refreshed_version_);
      if (tablegroup) {
        EXPECT_EQ(1U + distinct, after.cache_generations_.tablegroup_);
        EXPECT_EQ(distinct ? OB_ENTRY_NOT_EXIST : OB_SUCCESS,
                  service.get_tablegroup_table_id_cache().get(
                      TEST_TENANT_ID, after.cache_generations_.tablegroup_, 600001, members));
        EXPECT_EQ(1U, after.cache_generations_.id_resolve_);
      } else {
        EXPECT_EQ(1U + distinct, after.cache_generations_.id_resolve_);
        EXPECT_EQ(distinct ? OB_ENTRY_NOT_EXIST : OB_SUCCESS,
                  service.get_schema_resolve_cache().get(
                      ObSchemaIDResolveKey(TEST_TENANT_ID, after.cache_generations_.id_resolve_,
                                           600001), 100, result));
      }
    }
  }
}

TEST_F(TestSimpleSchemaKVCache, targeted_fallback_skips_deletions_for_bumped_domains)
{
  for (int active = 0; active <= 1; ++active) {
    SCOPED_TRACE(active);
    IncrementalHistorySchemaService history(nullptr, 0, nullptr, 0);
    IsolatedIncrementalSchemaService service(history);
    ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
    ObTenantSchemaPublishState state;
    ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
    ObSchemaPublishSnapshot before;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
    ObSchemaRefreshPublication publication;
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    const ObRefreshSchemaStatus status(
        TEST_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
    const ObServerSchemaService::IncrementalSchemaCacheContext context(
        status, 100, 120, publication);
    const ObSchemaIDResolveKey key(TEST_TENANT_ID, 1, 500001);
    ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
        key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, before));
    ObSchemaService::SchemaOperationSetWithAlloc operations;
    ObSchemaOperation operation;
    operation.tenant_id_ = TEST_TENANT_ID;
    operation.table_id_ = 500001;
    operation.schema_version_ = 110;
    operation.op_type_ = OB_DDL_ALTER_TABLE;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    if (active) {
      // Model apply_repairs() failing after it has already opened the window.
      ASSERT_EQ(OB_SUCCESS, publication.begin());
    } else {
      // This later operation invalidates the whole ID domain, including the
      // earlier table key. Planning must cancel that now redundant deletion.
      operation.op_type_ = OB_DDL_ALTER_TABLEGROUP_PARTITION;
      operation.tablegroup_id_ = 500010;
      ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    }
    ObSchemaCacheRepairResult repair_result;
    ASSERT_EQ(OB_SUCCESS, service.fallback_incremental_schema_cache_(
        context, operations, OB_ALLOCATE_MEMORY_FAILED, repair_result));
    EXPECT_EQ(SCHEMA_CACHE_DOMAIN_METADATA, repair_result.bump_domains_);
    ObSchemaResolveResult result;
    EXPECT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().get(key, 100, result));
    publication.add_bump_domains(repair_result.bump_domains_);
    ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
    ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
    ObSchemaPublishSnapshot after;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(after));
    EXPECT_EQ(2U, after.cache_generations_.id_resolve_);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, service.get_schema_resolve_cache().get(
        ObSchemaIDResolveKey(TEST_TENANT_ID, after.cache_generations_.id_resolve_, 500001),
        100, result));
  }
}

TEST_F(TestSimpleSchemaKVCache, incremental_preparation_allows_old_reads_before_apply_and_fallback)
{
  const IncrementalHistorySchemaService::TableVersion versions[] = {
    {500001, 100, "before"}, {500001, 110, "after"},
  };
  const IncrementalHistorySchemaService::FetchExpectation fetches[] = {
    {100, 1, 1}, {110, 1, 1},
  };
  for (int64_t mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    const bool fail_preparation = 1 == mode;
    const bool initially_missing = 2 == mode;
    IncrementalHistorySchemaService history(versions, ARRAYSIZEOF(versions),
                                             fetches, ARRAYSIZEOF(fetches));
    IsolatedIncrementalSchemaService service(history);
    ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
    ObTenantSchemaPublishState state;
    ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
    ObSchemaPublishSnapshot before;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
    ObSchemaRefreshPublication publication;
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    const ObSchemaIDResolveKey key(OB_SYS_TENANT_ID, 1, 500001);
    if (!initially_missing) {
      ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
          key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, before));
    }
    history.probe_preparation(state, service.get_schema_resolve_cache(), before,
                              fail_preparation ? 1 : -1, -1, initially_missing);
    const ObRefreshSchemaStatus schema_status(
        OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
    const ObServerSchemaService::IncrementalSchemaCacheContext context(
        schema_status, 100, 120, publication);
    ObSchemaMgr unused_schema_mgr;
    ObMySQLProxy unused_sql_proxy;
    ObSchemaCacheRepairResult repair_result;
    ObSchemaService::SchemaOperationSetWithAlloc operations;
    ObSchemaOperation operation;
    operation.tenant_id_ = OB_SYS_TENANT_ID;
    operation.table_id_ = 500001;
    operation.schema_version_ = 110;
    operation.op_type_ = OB_DDL_ALTER_TABLE;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    operation.table_id_ = OB_INVALID_ID;
    operation.schema_version_ = 120;
    operation.op_type_ = OB_DDL_END_SIGN;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));

    ASSERT_EQ(OB_SUCCESS, service.refresh_incremental_schema_cache_(
        context, unused_sql_proxy, operations, unused_schema_mgr, repair_result));
    EXPECT_EQ(2, history.get_probe_count());
    ASSERT_TRUE(publication.is_active());
    EXPECT_FALSE(state.can_publish(before));
    ObSchemaPublishSnapshot blocked;
    EXPECT_EQ(OB_TIMEOUT, state.read_stable(
        blocked, ObTimeUtility::current_time() + 1000));
    ObSchemaResolveResult result;
    if (fail_preparation) {
      EXPECT_EQ(SCHEMA_CACHE_REPAIR_FALLBACK, repair_result.action_);
      EXPECT_EQ(OB_TIMEOUT, repair_result.reason_);
      // Targeted fallback really removed the affected ID entry inside the
      // publication window, while retaining the ID generation.
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, service.get_schema_resolve_cache().get(key, 120, result));
      EXPECT_EQ(0U, repair_result.bump_domains_ & SCHEMA_CACHE_DOMAIN_ID_RESOLVE);
      publication.add_bump_domains(repair_result.bump_domains_);
    } else {
      EXPECT_EQ(SCHEMA_CACHE_REPAIR_APPLIED, repair_result.action_);
      ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().get(key, 120, result));
      EXPECT_EQ(110, result.precise_schema_version_);
    }
    ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
    ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));
    ObSchemaPublishSnapshot after;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
    EXPECT_EQ(120, after.refreshed_version_);
    EXPECT_EQ(before.cache_generations_.id_resolve_, after.cache_generations_.id_resolve_);
    EXPECT_EQ(before.cache_generations_.name_resolve_ + fail_preparation,
              after.cache_generations_.name_resolve_);
  }
}

TEST_F(TestSimpleSchemaKVCache, stale_preparation_cannot_apply_or_invalidate_id_cache)
{
  const IncrementalHistorySchemaService::TableVersion versions[] = {
    {500001, 100, "before"}, {500001, 110, "after"},
  };
  const IncrementalHistorySchemaService::FetchExpectation fetches[] = {
    {100, 1, 1}, {110, 1, 1},
  };
  IncrementalHistorySchemaService history(versions, ARRAYSIZEOF(versions),
                                           fetches, ARRAYSIZEOF(fetches));
  IsolatedIncrementalSchemaService service(history);
  ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaPublishSnapshot before;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  const ObSchemaIDResolveKey key(OB_SYS_TENANT_ID, 1, 500001);
  ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().publish_if_current(
      key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, before));
  history.probe_preparation(state, service.get_schema_resolve_cache(), before, -1, 1);
  const ObRefreshSchemaStatus schema_status(
      OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
  const ObServerSchemaService::IncrementalSchemaCacheContext context(
      schema_status, 100, 120, publication);
  ObSchemaMgr unused_schema_mgr;
  ObMySQLProxy unused_sql_proxy;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  operation.tenant_id_ = OB_SYS_TENANT_ID;
  operation.table_id_ = 500001;
  operation.schema_version_ = 110;
  operation.op_type_ = OB_DDL_ALTER_TABLE;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  operation.table_id_ = OB_INVALID_ID;
  operation.schema_version_ = 120;
  operation.op_type_ = OB_DDL_END_SIGN;
  ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));

  EXPECT_EQ(OB_SCHEMA_EAGAIN, service.refresh_incremental_schema_cache_(
      context, unused_sql_proxy, operations, unused_schema_mgr, repair_result));
  EXPECT_EQ(2, history.get_probe_count());
  // Neither repair nor its fallback may modify entries prepared against a
  // superseded generation. Inspect the old key directly to detect either.
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().get(key, 120, result));
  EXPECT_EQ(100, result.precise_schema_version_);
  EXPECT_EQ(OB_SCHEMA_EAGAIN, publication.finish(100, OB_INVALID_VERSION));
  ObSchemaPublishSnapshot after;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(
      after, ObTimeUtility::current_time() + 10000));
  EXPECT_EQ(100, after.refreshed_version_);
  EXPECT_EQ(2U, after.cache_generations_.id_resolve_);
}

TEST_F(TestSimpleSchemaKVCache, incremental_refresh_repairs_only_resident_keys)
{
  const IncrementalHistorySchemaService::TableVersion versions[] = {
    {500001, 100, "before"}, {500001, 110, "after"},
  };
  const IncrementalHistorySchemaService::FetchExpectation fetches[] = {
    {100, 1, 1}, {110, 1, 1},
  };
  // ID, old name and new name residency are independent. In particular, an ID
  // miss must not skip a cached name, and a negative name is a resident entry.
  for (int64_t mask = 0; mask < 8; ++mask) {
    SCOPED_TRACE(mask);
    IncrementalHistorySchemaService history(versions, ARRAYSIZEOF(versions),
                                             fetches, ARRAYSIZEOF(fetches));
    IsolatedIncrementalSchemaService service(history);
    ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
    ObSchemaResolveCache &cache = service.get_schema_resolve_cache();
    ObTenantSchemaPublishState state;
    ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
    ObSchemaPublishSnapshot before;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(before));
    const ObSchemaIDResolveKey id_key(OB_SYS_TENANT_ID, 1, 500001);
    const ObSchemaNameResolveKey old_name(
        OB_SYS_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE, ObString::make_string("before"));
    const ObSchemaNameResolveKey new_name(
        OB_SYS_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE, ObString::make_string("after"));
    if (0 != (mask & 1)) {
      // Physical residency, not a hit at the DDL version, decides whether to repair.
      ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(id_key,
          make_exist_range(100, 105, 100, 0), state, before));
    }
    if (0 != (mask & 2)) {
      ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(old_name,
          make_name_range(100, OB_SCHEMA_VERSION_INF, 500001, 100, true), state, before));
    }
    if (0 != (mask & 4)) {
      ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(new_name,
          make_name_range(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID, OB_INVALID_VERSION, false),
          state, before));
    }
    ObSchemaRefreshPublication publication;
    ASSERT_EQ(OB_SUCCESS, publication.init(&state));
    const ObRefreshSchemaStatus schema_status(
        OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
    const ObServerSchemaService::IncrementalSchemaCacheContext context(
        schema_status, 100, 120, publication);
    ObSchemaService::SchemaOperationSetWithAlloc operations;
    ObSchemaOperation operation;
    operation.tenant_id_ = OB_SYS_TENANT_ID;
    operation.table_id_ = 500001;
    operation.schema_version_ = 110;
    operation.op_type_ = OB_DDL_ALTER_TABLE;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    operation.table_id_ = OB_INVALID_ID;
    operation.schema_version_ = 120;
    operation.op_type_ = OB_DDL_END_SIGN;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
    ObSchemaMgr unused_schema_mgr;
    ObMySQLProxy unused_sql_proxy;
    ObSchemaCacheRepairResult repair_result;
    ASSERT_EQ(OB_SUCCESS, service.refresh_incremental_schema_cache_(
        context, unused_sql_proxy, operations, unused_schema_mgr, repair_result));
    ASSERT_EQ(SCHEMA_CACHE_REPAIR_APPLIED, repair_result.action_);
    EXPECT_EQ(SCHEMA_CACHE_DOMAIN_NONE, repair_result.bump_domains_);
    ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(120));
    ASSERT_EQ(OB_SUCCESS, publication.finish(120, 120));

    ObSchemaResolveResult result;
    EXPECT_EQ((mask & 1) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST, cache.get(id_key, 120, result));
    if (0 != (mask & 1)) {
      EXPECT_EQ(110, result.precise_schema_version_);
      ASSERT_EQ(OB_SUCCESS, cache.get(id_key, 100, result));
      EXPECT_EQ(100, result.precise_schema_version_);
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(id_key, 107, result));
    }
    EXPECT_EQ((mask & 2) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST, cache.get(old_name, 120, result));
    if (0 != (mask & 2)) {
      EXPECT_FALSE(result.is_exist_);
      ASSERT_EQ(OB_SUCCESS, cache.get(old_name, 100, result));
      EXPECT_EQ(500001U, result.schema_id_);
    }
    EXPECT_EQ((mask & 4) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST, cache.get(new_name, 120, result));
    if (0 != (mask & 4)) {
      EXPECT_EQ(500001U, result.schema_id_);
      EXPECT_EQ(110, result.precise_schema_version_);
      ASSERT_EQ(OB_SUCCESS, cache.get(new_name, 100, result));
      EXPECT_FALSE(result.is_exist_);
    }
    // A skipped key cannot be repopulated by an old loader; a new loader can
    // still admit it normally after publication, without changing generations.
    EXPECT_EQ(OB_SCHEMA_EAGAIN, cache.publish_if_current(id_key,
        make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0), state, before));
    EXPECT_EQ(OB_SCHEMA_EAGAIN, cache.publish_if_current(new_name,
        make_name_range(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID, OB_INVALID_VERSION, false),
        state, before));
    ObSchemaPublishSnapshot after;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(after));
    EXPECT_EQ(before.cache_generations_, after.cache_generations_);
    ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(id_key,
        make_exist_range(110, OB_SCHEMA_VERSION_INF, 110, 0), state, after));
    ASSERT_EQ(OB_SUCCESS, cache.publish_if_current(new_name,
        make_name_range(110, OB_SCHEMA_VERSION_INF, 500001, 110, true), state, after));
    ASSERT_EQ(OB_SUCCESS, cache.get(id_key, 120, result));
    EXPECT_EQ(110, result.precise_schema_version_);
    ASSERT_EQ(OB_SUCCESS, cache.get(new_name, 120, result));
    EXPECT_EQ(110, result.precise_schema_version_);
  }
}

TEST_F(TestSimpleSchemaKVCache, multi_version_refresh_preserves_history_and_final_names)
{
  const IncrementalHistorySchemaService::TableVersion versions[] = {
    {500001, 100, "plan_a"}, {500002, 100, "plan_b"}, {500003, 100, "plan_c"},
    {500001, 110, "plan_tmp"},
    {500001, 120, "plan_b"}, {500002, 120, "plan_a"}, {500003, 120, "plan_c"},
    {500001, 150, "plan_final"},
  };
  // The repeated operation must not duplicate fetch keys. A later version of
  // the same table must still be fetched, including the empty DROP result.
  const IncrementalHistorySchemaService::FetchExpectation fetches[] = {
    {100, 3, 7}, {110, 1, 1}, {120, 3, 7}, {130, 1, 4}, {150, 1, 1},
  };
  struct Operation
  {
    uint64_t table_id_;
    int64_t version_;
    ObSchemaOperationType type_;
  } operations_to_apply[] = {
    {500001, 110, OB_DDL_ALTER_TABLE},
    {500001, 110, OB_DDL_ALTER_TABLE},
    {500001, 120, OB_DDL_ALTER_TABLE},
    {500002, 120, OB_DDL_ALTER_TABLE},
    {500003, 120, OB_DDL_ALTER_TABLE},
    {500003, 130, OB_DDL_DROP_TABLE},
    {OB_INVALID_ID, 140, OB_DDL_END_SIGN},
    {500001, 150, OB_DDL_ALTER_TABLE},
    {OB_INVALID_ID, 160, OB_DDL_END_SIGN},
  };
  struct IDExpectation
  {
    uint64_t table_id_;
    int64_t snapshot_;
    int64_t precise_version_;
  } id_results[] = {
    {500001, 115, 110}, {500001, 125, 120}, {500001, 145, 120}, {500001, 160, 150},
    {500002, 145, 120}, {500003, 125, 120}, {500003, 135, OB_INVALID_VERSION},
  };
  struct NameExpectation
  {
    const char *name_;
    int64_t snapshot_;
    uint64_t table_id_;
  } name_results[] = {
    {"plan_a", 140, 500002}, {"plan_b", 140, 500001},
    {"plan_tmp", 140, OB_INVALID_ID}, {"plan_c", 140, OB_INVALID_ID},
    {"plan_a", 160, 500002}, {"plan_b", 160, OB_INVALID_ID}, {"plan_final", 160, 500001},
  };
  IncrementalHistorySchemaService history(versions, ARRAYSIZEOF(versions),
                                           fetches, ARRAYSIZEOF(fetches));
  IsolatedIncrementalSchemaService service(history);
  const ObRefreshSchemaStatus schema_status(
      OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(OB_SYS_TENANT_ID, 100));
  ObSchemaRefreshPublication publication;
  ASSERT_EQ(OB_SUCCESS, publication.init(&state));
  const ObServerSchemaService::IncrementalSchemaCacheContext context(
      schema_status, 100, 160, publication);
  ObSchemaMgr unused_schema_mgr;
  ObMySQLProxy unused_sql_proxy;
  ObSchemaCacheRepairResult repair_result;
  ObSchemaService::SchemaOperationSetWithAlloc operations;
  ObSchemaOperation operation;
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, service.init_test_caches());
  for (int64_t i = 0; i < 3; ++i) {
    const ObSchemaIDResolveKey key(OB_SYS_TENANT_ID, 1, versions[i].table_id_);
    ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(service.get_schema_resolve_cache(), key,
        make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0, versions[i].table_id_)));
  }
  const char *cached_names[] = {"plan_a", "plan_b", "plan_c", "plan_tmp", "plan_final"};
  for (int64_t i = 0; i < ARRAYSIZEOF(cached_names); ++i) {
    const ObSchemaNameResolveKey key(
        OB_SYS_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE,
        ObString::make_string(cached_names[i]));
    ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(service.get_schema_resolve_cache(), key,
        make_name_range(100, OB_SCHEMA_VERSION_INF,
                        i < 3 ? versions[i].table_id_ : OB_INVALID_ID, 100, i < 3)));
  }
  for (int64_t i = 0; i < ARRAYSIZEOF(operations_to_apply); ++i) {
    operation.tenant_id_ = OB_SYS_TENANT_ID;
    operation.table_id_ = operations_to_apply[i].table_id_;
    operation.schema_version_ = operations_to_apply[i].version_;
    operation.op_type_ = operations_to_apply[i].type_;
    ASSERT_EQ(OB_SUCCESS, operations.push_back(operation));
  }
  ASSERT_EQ(OB_SUCCESS, service.refresh_incremental_schema_cache_(
      context, unused_sql_proxy, operations,
      unused_schema_mgr, repair_result));
  ASSERT_EQ(OB_SUCCESS, publication.prepare_publish(160));
  ASSERT_EQ(OB_SUCCESS, publication.finish(160, 160));
  ASSERT_EQ(SCHEMA_CACHE_REPAIR_APPLIED, repair_result.action_);
  EXPECT_EQ(SCHEMA_CACHE_DOMAIN_NONE, repair_result.bump_domains_);
  EXPECT_EQ(ARRAYSIZEOF(fetches), history.get_fetch_count());
  for (int64_t i = 0; i < ARRAYSIZEOF(id_results); ++i) {
    const ObSchemaIDResolveKey key(OB_SYS_TENANT_ID, 1, id_results[i].table_id_);
    ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().get(
        key, id_results[i].snapshot_, result)) << "ID expectation " << i;
    EXPECT_EQ(OB_INVALID_VERSION != id_results[i].precise_version_, result.is_exist_);
    EXPECT_EQ(id_results[i].precise_version_, result.precise_schema_version_);
  }
  // A opens plan_b before B closes it at v120, so END_SIGN must restore A's
  // final binding. Neither A's intermediate name nor deleted C can reopen.
  for (int64_t i = 0; i < ARRAYSIZEOF(name_results); ++i) {
    const ObSchemaNameResolveKey key(
        OB_SYS_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE,
        ObString::make_string(name_results[i].name_));
    ASSERT_EQ(OB_SUCCESS, service.get_schema_resolve_cache().get(
        key, name_results[i].snapshot_, result)) << "name expectation " << i;
    EXPECT_EQ(OB_INVALID_ID != name_results[i].table_id_, result.is_exist_);
    EXPECT_EQ(name_results[i].table_id_, result.schema_id_);
  }
}

TEST_F(TestSimpleSchemaKVCache, incremental_alter_keeps_unrelated_generation_and_entries)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ObSimpleSchemaObjectCache object_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ASSERT_EQ(OB_SUCCESS, object_cache.init());

  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot old_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(old_token));
  ObSchemaIDResolveKey table_a_id(
      TEST_TENANT_ID, old_token.cache_generations_.id_resolve_, 500001);
  ObSchemaIDResolveKey table_b_id(
      TEST_TENANT_ID, old_token.cache_generations_.id_resolve_, 500002);
  ObSchemaNameResolveKey table_a_name(
                                      TEST_TENANT_ID,
                                      old_token.cache_generations_.name_resolve_,
                                      500010, 0, NS_NORMAL_TABLE,
                                      ObString::make_string("table_a"));
  ObSchemaNameResolveKey table_b_name(
                                      TEST_TENANT_ID,
                                      old_token.cache_generations_.name_resolve_,
                                      500010, 0, NS_NORMAL_TABLE,
                                      ObString::make_string("table_b"));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      table_a_id, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0, 500001),
      state, old_token));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      table_b_id, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0, 500002),
      state, old_token));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      table_a_name, make_name_range(100, OB_SCHEMA_VERSION_INF, 500001, 100, true),
      state, old_token));
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      table_b_name, make_name_range(100, OB_SCHEMA_VERSION_INF, 500002, 100, true),
      state, old_token));

  ObSimpleTableSchemaV2 table_b;
  init_simple_table(table_b, 500002, 100, "table_b");
  ObSimpleSchemaCacheKey table_b_object_key(
      TEST_TENANT_ID, 500002, 100);
  const ObSimpleTableSchemaV2 *cached_table_b = NULL;
  ObKVCacheHandle table_b_put_handle;
  ASSERT_EQ(OB_SUCCESS, object_cache.put_and_fetch(
      table_b_object_key, table_b, cached_table_b, table_b_put_handle));

  ObSimpleTableSchemaV2 before_a;
  ObSimpleTableSchemaV2 after_a;
  init_simple_table(before_a, 500001, 100, "table_a");
  init_simple_table(after_a, 500001, 120, "table_a");
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, old_token.cache_generations_, 500001, 120,
      &before_a, OB_ORIGIN_AND_SENSITIVE,
      &after_a, OB_ORIGIN_AND_SENSITIVE));
  EXPECT_EQ(OB_SCHEMA_EAGAIN,
            resolve_cache.publish_if_current(
                table_a_id,
                make_exist_range(120, OB_SCHEMA_VERSION_INF, 120, 0, 500001),
                state, old_token));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ObSchemaPublishSnapshot new_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(new_token));
  EXPECT_EQ(old_token.cache_generations_, new_token.cache_generations_);
  EXPECT_NE(old_token.refresh_seq_, new_token.refresh_seq_);
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(table_a_id, 120, result));
  EXPECT_EQ(120, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(table_a_name, 120, result));
  EXPECT_EQ(120, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(table_b_id, 120, result));
  EXPECT_EQ(100, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(table_b_name, 120, result));
  EXPECT_EQ(500002U, result.schema_id_);
  ObKVCacheHandle table_b_get_handle;
  ASSERT_EQ(OB_SUCCESS, object_cache.get(
      table_b_object_key, cached_table_b, table_b_get_handle));
  EXPECT_EQ(ObString("table_b"), cached_table_b->get_table_name_str());
}

TEST_F(TestSimpleSchemaKVCache, multiple_changes_before_end_sign_keep_historical_ranges)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  const ObSchemaCacheGenerations cache_generations(1);
  ObSchemaIDResolveKey id_key(TEST_TENANT_ID, cache_generations.id_resolve_, 500001);
  ObSchemaNameResolveKey name_key(
      TEST_TENANT_ID, cache_generations.name_resolve_, 500010, 0,
      NS_NORMAL_TABLE, ObString::make_string("table_a"));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      id_key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0, 500001)));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      name_key, make_name_range(100, OB_SCHEMA_VERSION_INF, 500001, 100, true)));

  ObSimpleTableSchemaV2 v100;
  ObSimpleTableSchemaV2 v120;
  ObSimpleTableSchemaV2 v130;
  init_simple_table(v100, 500001, 100, "table_a");
  init_simple_table(v120, 500001, 120, "table_a");
  init_simple_table(v130, 500001, 130, "table_a");
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 120,
      &v100, OB_ORIGIN_AND_SENSITIVE, &v120, OB_ORIGIN_AND_SENSITIVE));
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 130,
      &v120, OB_ORIGIN_AND_SENSITIVE, &v130, OB_ORIGIN_AND_SENSITIVE));
  ObSEArray<const ObSimpleTableSchemaV2 *, 1> final_schemas;
  ASSERT_EQ(OB_SUCCESS, final_schemas.push_back(&v130));
  ASSERT_EQ(OB_SUCCESS, refresher.finalize_table_names_at_boundary(
      TEST_TENANT_ID, cache_generations, 140,
      final_schemas, OB_ORIGIN_AND_SENSITIVE));

  struct ExpectedVersion
  {
    int64_t snapshot_version_;
    int64_t precise_version_;
  } expected_versions[] = {
    {119, 100},
    {120, 120},
    {129, 120},
    {130, 130},
    {139, 130},
    {140, 130},
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_versions); ++i) {
    ObSchemaResolveResult result;
    ASSERT_EQ(OB_SUCCESS, resolve_cache.get(
        id_key, expected_versions[i].snapshot_version_, result));
    EXPECT_EQ(expected_versions[i].precise_version_, result.precise_schema_version_);
    ASSERT_EQ(OB_SUCCESS, resolve_cache.get(
        name_key, expected_versions[i].snapshot_version_, result));
    EXPECT_EQ(expected_versions[i].precise_version_, result.precise_schema_version_);
  }
}

TEST_F(TestSimpleSchemaKVCache, incremental_create_rename_drop_and_recreate_repairs_ranges)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  const uint64_t generation = 1;
  const ObSchemaCacheGenerations cache_generations(generation);
  ObSchemaIDResolveKey old_id(TEST_TENANT_ID, generation, 500001);
  ObSchemaIDResolveKey new_id(TEST_TENANT_ID, generation, 500002);
  ObSchemaNameResolveKey created_name(TEST_TENANT_ID, generation, 500010, 0,
                                      NS_NORMAL_TABLE,
                                      ObString::make_string("created_table"));
  ObSchemaNameResolveKey renamed_name(TEST_TENANT_ID, generation, 500010, 0,
                                      NS_NORMAL_TABLE,
                                      ObString::make_string("renamed_table"));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      old_id, ObSchemaResolveRange(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID,
                                   OB_INVALID_VERSION, 0, NS_MAX, false)));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      new_id, ObSchemaResolveRange(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID,
                                   OB_INVALID_VERSION, 0, NS_MAX, false)));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      created_name, make_name_range(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID,
                                    OB_INVALID_VERSION, false)));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      renamed_name, make_name_range(0, OB_SCHEMA_VERSION_INF, OB_INVALID_ID,
                                    OB_INVALID_VERSION, false)));

  ObSimpleTableSchemaV2 created;
  init_simple_table(created, 500001, 110, "Created_Table");
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 110, NULL,
      OB_NAME_CASE_INVALID, &created, OB_LOWERCASE_AND_INSENSITIVE));

  ObSimpleTableSchemaV2 renamed;
  init_simple_table(renamed, 500001, 120, "Renamed_Table");
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 120, &created,
      OB_LOWERCASE_AND_INSENSITIVE, &renamed, OB_LOWERCASE_AND_INSENSITIVE));

  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 130, &renamed,
      OB_LOWERCASE_AND_INSENSITIVE, NULL, OB_NAME_CASE_INVALID));

  ObSimpleTableSchemaV2 recreated;
  init_simple_table(recreated, 500002, 140, "Renamed_Table");
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500002, 140, NULL,
      OB_NAME_CASE_INVALID, &recreated, OB_LOWERCASE_AND_INSENSITIVE));

  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(old_id, 125, result));
  EXPECT_TRUE(result.is_exist_);
  EXPECT_EQ(120, result.precise_schema_version_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(old_id, 135, result));
  EXPECT_FALSE(result.is_exist_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(created_name, 115, result));
  EXPECT_EQ(500001U, result.schema_id_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(created_name, 125, result));
  EXPECT_FALSE(result.is_exist_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(renamed_name, 125, result));
  EXPECT_EQ(500001U, result.schema_id_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(renamed_name, 135, result));
  EXPECT_FALSE(result.is_exist_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(renamed_name, 145, result));
  EXPECT_EQ(500002U, result.schema_id_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(new_id, 145, result));
  EXPECT_EQ(140, result.precise_schema_version_);
}

TEST_F(TestSimpleSchemaKVCache, incremental_same_boundary_name_swap_keeps_final_bindings)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  const ObSchemaCacheGenerations cache_generations(1);

  ObSimpleTableSchemaV2 before_a;
  ObSimpleTableSchemaV2 before_b;
  ObSimpleTableSchemaV2 after_a;
  ObSimpleTableSchemaV2 after_b;
  init_simple_table(before_a, 500001, 100, "index_name");
  init_simple_table(before_b, 500002, 100, "rebuild_tmp_name");
  init_simple_table(after_a, 500001, 120, "rebuild_tmp_name");
  init_simple_table(after_b, 500002, 120, "index_name");

  const ObSchemaNameResolveKey original_name(
      TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE,
      ObString::make_string("index_name"));
  const ObSchemaNameResolveKey temporary_name(
      TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE,
      ObString::make_string("rebuild_tmp_name"));
  for (int64_t mask = 0; mask < 4; ++mask) {
    SCOPED_TRACE(mask);
    ASSERT_EQ(OB_SUCCESS, resolve_cache.erase(original_name));
    ASSERT_EQ(OB_SUCCESS, resolve_cache.erase(temporary_name));
    if (0 != (mask & 1)) {
      ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache, original_name,
          make_name_range(100, OB_SCHEMA_VERSION_INF, 500001, 100, true)));
    }
    if (0 != (mask & 2)) {
      ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache, temporary_name,
          make_name_range(100, OB_SCHEMA_VERSION_INF, 500002, 100, true)));
    }
    // Match vector rebuild: A opens the temporary name before B closes it.
    // Finalization must restore A even when only that name is resident.
    ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
        TEST_TENANT_ID, cache_generations, 500001, 120,
        &before_a, OB_ORIGIN_AND_SENSITIVE, &after_a, OB_ORIGIN_AND_SENSITIVE));
    ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
        TEST_TENANT_ID, cache_generations, 500002, 120,
        &before_b, OB_ORIGIN_AND_SENSITIVE, &after_b, OB_ORIGIN_AND_SENSITIVE));
    ObArray<const ObSimpleTableSchemaV2 *> final_schemas;
    ASSERT_EQ(OB_SUCCESS, final_schemas.push_back(&after_a));
    ASSERT_EQ(OB_SUCCESS, final_schemas.push_back(&after_b));
    ASSERT_EQ(OB_SUCCESS, refresher.finalize_table_names_at_boundary(
        TEST_TENANT_ID, cache_generations, 120, final_schemas, OB_ORIGIN_AND_SENSITIVE));
    ObSchemaResolveResult result;
    EXPECT_EQ((mask & 1) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST,
              resolve_cache.get(original_name, 125, result));
    if (0 != (mask & 1)) {
      EXPECT_EQ(500002U, result.schema_id_);
      ASSERT_EQ(OB_SUCCESS, resolve_cache.get(original_name, 110, result));
      EXPECT_EQ(500001U, result.schema_id_);
    }
    EXPECT_EQ((mask & 2) ? OB_SUCCESS : OB_ENTRY_NOT_EXIST,
              resolve_cache.get(temporary_name, 125, result));
    if (0 != (mask & 2)) {
      EXPECT_EQ(500001U, result.schema_id_);
      ASSERT_EQ(OB_SUCCESS, resolve_cache.get(temporary_name, 110, result));
      EXPECT_EQ(500002U, result.schema_id_);
    }
  }
}

TEST_F(TestSimpleSchemaKVCache, name_finalization_does_not_readmit_evicted_key)
{
  ObSchemaResolveCache cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  const ObSchemaNameResolveKey name_key(
      TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE, ObString::make_string("table_a"));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(cache, name_key,
      make_name_range(100, OB_SCHEMA_VERSION_INF, 500001, 100, true)));
  ObSimpleTableSchemaV2 before;
  ObSimpleTableSchemaV2 after;
  init_simple_table(before, 500001, 100, "table_a");
  init_simple_table(after, 500001, 110, "table_a");
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ObSchemaCacheIncrementalRefresher refresher(cache, tablegroup_cache);
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, publication.before().cache_generations_, 500001, 110,
      &before, OB_ORIGIN_AND_SENSITIVE, &after, OB_ORIGIN_AND_SENSITIVE));
  // Model eviction (also the repair-failure erase fallback) before END_SIGN.
  ASSERT_EQ(OB_SUCCESS, cache.erase(name_key));
  ObSEArray<const ObSimpleTableSchemaV2 *, 1> final_schemas;
  ASSERT_EQ(OB_SUCCESS, final_schemas.push_back(&after));
  ASSERT_EQ(OB_SUCCESS, refresher.finalize_table_names_at_boundary(
      TEST_TENANT_ID, publication.before().cache_generations_, 120,
      final_schemas, OB_ORIGIN_AND_SENSITIVE));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));
  ObSchemaResolveResult result;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache.get(name_key, 120, result));
}

TEST_F(TestSimpleSchemaKVCache, incremental_session_table_drop_keeps_global_fallback)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  const ObSchemaCacheGenerations cache_generations(1);
  ObSimpleTableSchemaV2 session_table;
  init_simple_table(session_table, 500002, 20, "same_name", 500010, 9988);
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      ObSchemaNameResolveKey(TEST_TENANT_ID, 1, 500010, 0, NS_NORMAL_TABLE,
                              ObString::make_string("same_name")),
      make_name_range(10, OB_SCHEMA_VERSION_INF, 500001, 10, true)));
  ASSERT_EQ(OB_SUCCESS, seed_resolve_cache(resolve_cache,
      ObSchemaNameResolveKey(TEST_TENANT_ID, 1, 500010, 9988, NS_NORMAL_TABLE,
                              ObString::make_string("same_name")),
      make_name_range(20, OB_SCHEMA_VERSION_INF, 500002, 20, true)));
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500002, 30,
      &session_table, OB_ORIGIN_AND_SENSITIVE,
      NULL, OB_NAME_CASE_INVALID));

  ObSchemaNameResolveKey private_key(TEST_TENANT_ID, 1, 500010, 9988,
                                     NS_NORMAL_TABLE,
                                     ObString::make_string("same_name"));
  ObSchemaNameResolveKey global_key(TEST_TENANT_ID, 1, 500010, 0,
                                    NS_NORMAL_TABLE,
                                    ObString::make_string("same_name"));
  ObSchemaResolveResult result;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(private_key, 35, result));
  EXPECT_FALSE(result.is_exist_);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.get(global_key, 35, result));
  EXPECT_TRUE(result.is_exist_);
  EXPECT_EQ(500001U, result.schema_id_);
}

TEST_F(TestSimpleSchemaKVCache, incremental_tablegroup_change_erases_only_old_and_new_generation_keys)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObSEArray<uint64_t, 2> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(500001));
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put(TEST_TENANT_ID, 1, 501, ids));
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put(TEST_TENANT_ID, 1, 502, ids));
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put(TEST_TENANT_ID, 1, 503, ids));
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put(TEST_TENANT_ID, 2, 501, ids));

  ObSimpleTableSchemaV2 before;
  ObSimpleTableSchemaV2 after;
  init_simple_table(before, 500001, 100, "table_a", 500010, 0, 501);
  init_simple_table(after, 500001, 120, "table_a", 500010, 0, 502);
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  const ObSchemaCacheGenerations cache_generations(1);
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 120, &before,
      OB_ORIGIN_AND_SENSITIVE,
      &after, OB_ORIGIN_AND_SENSITIVE));

  ObArray<uint64_t> result;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, tablegroup_cache.get(TEST_TENANT_ID, 1, 501, result));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, tablegroup_cache.get(TEST_TENANT_ID, 1, 502, result));
  EXPECT_EQ(OB_SUCCESS, tablegroup_cache.get(TEST_TENANT_ID, 1, 503, result));
  EXPECT_EQ(OB_SUCCESS, tablegroup_cache.get(TEST_TENANT_ID, 2, 501, result));

  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put(TEST_TENANT_ID, 1, 502, ids));
  ObSimpleTableSchemaV2 after_same_group;
  init_simple_table(after_same_group, 500001, 130, "table_a", 500010, 0, 502);
  ASSERT_EQ(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, cache_generations, 500001, 130, &after,
      OB_ORIGIN_AND_SENSITIVE,
      &after_same_group, OB_ORIGIN_AND_SENSITIVE));
  EXPECT_EQ(OB_SUCCESS, tablegroup_cache.get(TEST_TENANT_ID, 1, 502, result));
}

TEST_F(TestSimpleSchemaKVCache, tablegroup_conditional_put_cannot_repopulate_after_refresh_erase)
{
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSEArray<uint64_t, 2> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(500001));
  const uint64_t tablegroup_id = 501;

  for (int64_t i = 0; i < 64; ++i) {
    ObSchemaPublishSnapshot token;
    ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
    std::atomic<bool> start(false);
    std::atomic<int> reader_ret(OB_SUCCESS);
    std::thread old_loader([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      reader_ret.store(tablegroup_cache.put_if_current(
          TEST_TENANT_ID, token.cache_generations_.tablegroup_, tablegroup_id,
          ids, state, token), std::memory_order_release);
    });

    start.store(true, std::memory_order_release);
    ObTenantSchemaPublicationGuard publication;
    EXPECT_EQ(OB_SUCCESS, publication.begin(state));
    EXPECT_EQ(OB_SUCCESS, tablegroup_cache.erase(
        TEST_TENANT_ID, token.cache_generations_.tablegroup_, tablegroup_id));
    EXPECT_EQ(OB_SUCCESS, publication.commit(101 + i));
    old_loader.join();

    const int put_ret = reader_ret.load(std::memory_order_acquire);
    EXPECT_TRUE(OB_SUCCESS == put_ret || OB_SCHEMA_EAGAIN == put_ret);
    ObArray<uint64_t> result;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, tablegroup_cache.get(
        TEST_TENANT_ID, token.cache_generations_.tablegroup_, tablegroup_id, result));
  }

  ObSchemaPublishSnapshot current_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(current_token));
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.put_if_current(
      TEST_TENANT_ID, current_token.cache_generations_.tablegroup_, tablegroup_id,
      ids, state, current_token));
  ObArray<uint64_t> result;
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.get(
      TEST_TENANT_ID, current_token.cache_generations_.tablegroup_, tablegroup_id, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(500001U, result.at(0));
}

TEST_F(TestSimpleSchemaKVCache, incremental_repair_failure_invalidates_metadata)
{
  ObSchemaResolveCache resolve_cache;
  ObTablegroupTableIdCache tablegroup_cache;
  ASSERT_EQ(OB_SUCCESS, resolve_cache.init());
  ASSERT_EQ(OB_SUCCESS, tablegroup_cache.init());
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TEST_TENANT_ID, 100));
  ObSchemaPublishSnapshot old_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(old_token));
  ObSchemaIDResolveKey old_key(
      TEST_TENANT_ID, old_token.cache_generations_.id_resolve_, 500001);
  ASSERT_EQ(OB_SUCCESS, resolve_cache.publish_if_current(
      old_key, make_exist_range(100, OB_SCHEMA_VERSION_INF, 100, 0),
      state, old_token));

  ObTenantSchemaPublicationGuard publication;
  ASSERT_EQ(OB_SUCCESS, publication.begin(state));
  ObSchemaCacheIncrementalRefresher refresher(resolve_cache, tablegroup_cache);
  ASSERT_NE(OB_SUCCESS, refresher.refresh_table(
      TEST_TENANT_ID, old_token.cache_generations_, 500001, 120,
      NULL, OB_NAME_CASE_INVALID, NULL, OB_NAME_CASE_INVALID));
  ASSERT_EQ(OB_SUCCESS, publication.bump_generation(SCHEMA_CACHE_DOMAIN_METADATA));
  ASSERT_EQ(OB_SUCCESS, publication.commit(120));

  ObSchemaPublishSnapshot new_token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(new_token));
  EXPECT_EQ(old_token.cache_generations_.id_resolve_ + 1,
            new_token.cache_generations_.id_resolve_);
  EXPECT_EQ(old_token.cache_generations_.name_resolve_ + 1,
            new_token.cache_generations_.name_resolve_);
  EXPECT_EQ(old_token.cache_generations_.tablegroup_ + 1,
            new_token.cache_generations_.tablegroup_);
  ObSchemaIDResolveKey new_key(
      TEST_TENANT_ID, new_token.cache_generations_.id_resolve_, 500001);
  ObSchemaResolveResult result;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, resolve_cache.get(new_key, 120, result));
}

} // namespace
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
