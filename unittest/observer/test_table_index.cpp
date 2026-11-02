/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "deps/oblib/unittest/lib/unittest_diagnostic_info_util.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "common/ob_clock_generator.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_get_compat_mode.h"
#include "share/config/ob_server_config.h"

#define private public
#define protected public
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/mock_schema_service.h"
#include "observer/virtual_table/ob_table_index.h"
#undef protected
#undef private
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace observer;

namespace
{
const uint64_t TENANT_ID = 1002;
const uint64_t DATABASE_ID = 500010;
const int64_t SCHEMA_VERSION = 100;
ObSimpleMemLimitGetter MEM_LIMIT_GETTER;

// Use the real schema cache value/handle lifetime, while keeping schema loading
// deterministic and independent of an observer or an inner SQL connection.
class TableIndexSchemaService : public MockSchemaService
{
public:
  TableIndexSchemaService() : fail_table_id_(OB_INVALID_ID), full_fetch_count_(0) {}
  ~TableIndexSchemaService()
  {
    // The optional SQL provider used by the on-demand test is borrowed.
    schema_service_ = NULL;
    sql_proxy_ = NULL;
    config_ = NULL;
  }

  int get_schema(const ObSchemaMgr *mgr, const ObRefreshSchemaStatus &status,
                 const ObSchemaType type, const uint64_t id, const int64_t version,
                 ObKVCacheHandle &handle, const ObSchema *&schema) override
  {
    int ret = OB_SUCCESS;
    schema = NULL;
    if (TABLE_SCHEMA != type) {
      ret = MockSchemaService::get_schema(mgr, status, type, id, version, handle, schema);
    } else if (id == fail_table_id_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ++full_fetch_count_;
      const ObSchemaCacheKey key(type, status.tenant_id_, id, version);
      const ObSchemaCacheValue *value = NULL;
      ret = full_cache_.get(key, value, handle);
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(MockSchemaService::get_schema(mgr, status, type, id, version, handle, schema))) {
        } else {
          const ObSchemaCacheValue input(type, schema);
          ret = full_cache_.put_and_fetch(key, input, value, handle);
        }
      }
      if (OB_SUCC(ret)) {
        schema = value->schema_;
      }
    }
    return ret;
  }

  ObKVCache<ObSchemaCacheKey, ObSchemaCacheValue> full_cache_;
  uint64_t fail_table_id_;
  int64_t full_fetch_count_;
};

class TestTableIndex : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init_for_obcdc());
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().set_tenant_compat_mode(TENANT_ID, mode));
  }

  static void TearDownTestCase()
  {
    ObCompatModeGetter::instance().destroy();
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  void SetUp() override
  {
    for (const uint64_t tenant_id : {OB_SYS_TENANT_ID, OB_SERVER_TENANT_ID}) {
      auto allocator = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
          tenant_id, ObCtxIds::KVSTORE_CACHE_ID);
      if (NULL == allocator.ref_allocator()) {
        ASSERT_EQ(OB_SUCCESS,
                  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id));
      }
      ASSERT_EQ(OB_SUCCESS, MEM_LIMIT_GETTER.add_tenant(tenant_id, 0, INT64_MAX));
    }
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(
        &MEM_LIMIT_GETTER, 1024, 1L << 30, lib::ACHUNK_SIZE));
    const int clock_ret = ObClockGenerator::init();
    ASSERT_TRUE(OB_SUCCESS == clock_ret || OB_INIT_TWICE == clock_ret);
    CHUNK_MGR.set_limit(5L << 30);
    ASSERT_EQ(OB_SUCCESS, service_.init());
    ASSERT_EQ(OB_SUCCESS, service_.full_cache_.init("TableIndexTest", 1));

    ObTenantSchema tenant;
    tenant.set_tenant_id(TENANT_ID);
    tenant.set_schema_version(SCHEMA_VERSION);
    ASSERT_EQ(OB_SUCCESS, tenant.set_tenant_name("table_index_tenant"));
    ASSERT_EQ(OB_SUCCESS, service_.add_tenant_schema(tenant, SCHEMA_VERSION));
    ObSysVariableSchema variables;
    variables.set_tenant_id(TENANT_ID);
    variables.set_schema_version(SCHEMA_VERSION);
    variables.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, service_.add_sys_variable_schema(variables, SCHEMA_VERSION));
    ObDatabaseSchema database;
    database.set_tenant_id(TENANT_ID);
    database.set_database_id(DATABASE_ID);
    database.set_schema_version(SCHEMA_VERSION);
    database.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, database.set_database_name("index_db"));
    ASSERT_EQ(OB_SUCCESS, service_.add_database_schema(database, SCHEMA_VERSION));
  }

  void TearDown() override
  {
    iter_.reset();
    scan_guard_.reset();
    service_.get_schema_resolve_cache().destroy();
    service_.full_cache_.destroy();
    ObKVGlobalCache::get_instance().destroy();
    MEM_LIMIT_GETTER.reset();
  }

  void make_table(ObTableSchema &table, const uint64_t id, const char *name,
                  const ObTableType type = USER_TABLE)
  {
    table.set_tenant_id(TENANT_ID);
    table.set_database_id(DATABASE_ID);
    table.set_table_id(id);
    table.set_schema_version(SCHEMA_VERSION);
    table.set_table_type(type);
    table.set_index_type(USER_INDEX == type ? INDEX_TYPE_NORMAL_LOCAL : INDEX_TYPE_IS_NOT);
    if (USER_INDEX == type) {
      table.set_data_table_id(id - 1);
    }
    table.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    table.set_table_organization_mode(TOM_INDEX_ORGANIZED);
    table.set_table_pk_exists_mode(TOM_TABLE_WITH_PK);
    table.set_charset_type(CHARSET_UTF8MB4);
    table.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    table.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + 1);
    ASSERT_EQ(OB_SUCCESS, table.set_table_name(name));
    for (int64_t i = 0; i < 2; ++i) {
      ObColumnSchemaV2 column;
      column.set_tenant_id(TENANT_ID);
      column.set_table_id(id);
      column.set_schema_version(SCHEMA_VERSION);
      column.set_column_id(OB_APP_MIN_COLUMN_ID + i);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name(0 == i ? "id" : "value"));
      column.set_data_type(ObIntType);
      column.set_data_length(static_cast<int64_t>(sizeof(int64_t)));
      column.set_charset_type(CHARSET_BINARY);
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_rowkey_position(0 == i ? 1 : 0);
      column.set_index_position(USER_INDEX == type && 0 == i ? 1 : 0);
      column.set_order_in_rowkey(ObOrderType::ASC);
      column.set_nullable(0 != i);
      ASSERT_EQ(OB_SUCCESS, table.add_column(column));
    }
    ASSERT_TRUE(table.is_valid());
  }

  void add_table_with_index(const uint64_t id)
  {
    char name[64];
    snprintf(name, sizeof(name), "table_%lu", id);
    ObTableSchema table;
    make_table(table, id, name);
    ASSERT_EQ(OB_SUCCESS, table.add_simple_index_info(
        ObAuxTableMetaInfo(id + 1, USER_INDEX, INDEX_TYPE_NORMAL_LOCAL)));
    ASSERT_EQ(OB_SUCCESS, service_.add_table_schema(table, SCHEMA_VERSION));
    snprintf(name, sizeof(name), "__idx_%lu_idx", id);
    ObTableSchema index;
    make_table(index, id + 1, name, USER_INDEX);
    index.set_data_table_id(id);
    index.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
    index.set_index_status(INDEX_STATUS_AVAILABLE);
    ASSERT_EQ(OB_SUCCESS, index.set_comment("index comment"));
    ASSERT_EQ(OB_SUCCESS, service_.add_table_schema(index, SCHEMA_VERSION));
  }

  void open_scan(const uint64_t table_id = OB_INVALID_ID)
  {
    iter_.reset();
    ASSERT_EQ(OB_SUCCESS, service_.get_schema_guard(scan_guard_, SCHEMA_VERSION));
    scan_guard_.set_session_id(42);
    iter_.set_schema_guard(&scan_guard_);
    iter_.set_allocator(&allocator_);
    iter_.tenant_id_ = TENANT_ID;
    iter_.min_data_version_ = DATA_VERSION_4_3_5_0;
    iter_.cur_row_.count_ = 19;
    void *buf = allocator_.alloc(sizeof(ObObj) * 19);
    ASSERT_NE(nullptr, buf);
    iter_.cur_row_.cells_ = new (buf) ObObj[19];
    for (int64_t i = 0; i < 19; ++i) {
      ASSERT_EQ(OB_SUCCESS, iter_.output_column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i));
    }
    if (OB_INVALID_ID != table_id) {
      key_.set_int(table_id);
      ObNewRange range;
      range.start_key_.assign(&key_, 1);
      range.end_key_.assign(&key_, 1);
      ASSERT_EQ(OB_SUCCESS, iter_.key_ranges_.push_back(range));
    }
    ASSERT_EQ(OB_SUCCESS, iter_.inner_open());
  }

  int64_t full_pins(const ObSchemaGetterGuard &guard) const
  {
    int64_t count = 0;
    for (int64_t i = 0; i < guard.schema_objs_.count(); ++i) {
      if (TABLE_SCHEMA == guard.schema_objs_.at(i).schema_type_) {
        EXPECT_TRUE(guard.schema_objs_.at(i).handle_.is_valid());
        ++count;
      }
    }
    return count;
  }

  TableIndexSchemaService service_;
  ObSchemaGetterGuard scan_guard_;
  ObArenaAllocator allocator_;
  ObTableIndex iter_;
  ObObj key_;
};

TEST_F(TestTableIndex, full_scan_bounds_pins_and_preserves_parent)
{
  const int64_t table_count = 128;
  for (int64_t i = 0; i < table_count; ++i) {
    add_table_with_index(500100 + i * 2);
  }
  open_scan();
  const ObTableSchema *parent_table = NULL;
  ASSERT_EQ(OB_SUCCESS, scan_guard_.get_table_schema(TENANT_ID, 500100, parent_table));
  ASSERT_NE(nullptr, parent_table);
  ObNewRow *row = NULL;
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  while (OB_SUCCESS == (ret = iter_.inner_get_next_row(row))) {
    ASSERT_NE(nullptr, row);
    EXPECT_LE(full_pins(iter_.table_guard_), 2);
    EXPECT_EQ(1, full_pins(scan_guard_));
    EXPECT_EQ(ObString("index_db"), row->cells_[3].get_string());
    EXPECT_EQ(42U, iter_.table_guard_.get_session_id());
    int64_t version = 0;
    ASSERT_EQ(OB_SUCCESS, iter_.table_guard_.get_schema_version(TENANT_ID, version));
    EXPECT_EQ(SCHEMA_VERSION, version);
    ++row_count;
  }
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(table_count * 4, row_count);
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
  EXPECT_EQ(1, full_pins(scan_guard_));
  EXPECT_EQ(ObString("table_500100"), parent_table->get_table_name_str());
  EXPECT_EQ(OB_ITER_END, iter_.inner_get_next_row(row));
  EXPECT_EQ(nullptr, row);
}

TEST_F(TestTableIndex, last_row_stays_pinned_until_next_call)
{
  add_table_with_index(500100);
  open_scan(500100);
  ObNewRow *row = NULL;
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
  }
  EXPECT_EQ(2, full_pins(iter_.table_guard_));
  // Removing cache mappings must not invalidate strings in the returned row.
  ASSERT_EQ(OB_SUCCESS, service_.full_cache_.erase(
      ObSchemaCacheKey(TABLE_SCHEMA, TENANT_ID, 500100, SCHEMA_VERSION)));
  ASSERT_EQ(OB_SUCCESS, service_.full_cache_.erase(
      ObSchemaCacheKey(TABLE_SCHEMA, TENANT_ID, 500101, SCHEMA_VERSION)));
  EXPECT_EQ(ObString("table_500100"), row->cells_[4].get_string());
  EXPECT_EQ(ObString("index comment"), row->cells_[15].get_string());
  EXPECT_EQ(OB_ITER_END, iter_.inner_get_next_row(row));
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
  EXPECT_EQ(0, full_pins(scan_guard_));
}

TEST_F(TestTableIndex, on_demand_scan_releases_previous_tables)
{
  const int64_t table_count = 16;
  for (int64_t i = 0; i < table_count; ++i) {
    add_table_with_index(500100 + i * 2);
  }
  open_scan();
  // Supply the enumeration fixture without an inner SQL connection; all full
  // schema point lookups below go through the real on-demand implementation.
  ASSERT_EQ(OB_SUCCESS, scan_guard_.get_database_schemas_in_tenant(TENANT_ID, iter_.database_schemas_));
  ASSERT_EQ(OB_SUCCESS, scan_guard_.get_table_ids_in_database(TENANT_ID, DATABASE_ID, iter_.table_ids_));
  iter_.database_schema_idx_ = 0;
  iter_.table_schema_idx_ = 0;
  ObSchemaServiceSQLImpl history;
  ObMySQLProxy proxy;
  service_.schema_service_ = &history;
  service_.sql_proxy_ = &proxy;
  service_.config_ = &GCONF;
  service_.init_ = true;
  ASSERT_EQ(OB_SUCCESS, service_.get_schema_resolve_cache().init());
  ASSERT_EQ(OB_SUCCESS, service_.simple_schema_on_demand_coordinator_.init(&service_));
  ASSERT_EQ(OB_SUCCESS, scan_guard_.init_table_impl(ObSimpleSchemaMode::ON_DEMAND));
  ObTenantSchemaPublishState state;
  ASSERT_EQ(OB_SUCCESS, state.init(TENANT_ID, SCHEMA_VERSION));
  ObSchemaPublishSnapshot token;
  ASSERT_EQ(OB_SUCCESS, state.read_stable(token));
  for (int64_t i = 0; i < scan_guard_.schema_mgr_infos_.count(); ++i) {
    if (TENANT_ID == scan_guard_.schema_mgr_infos_.at(i).get_tenant_id()) {
      scan_guard_.schema_mgr_infos_.at(i).set_publish_state(&state);
    }
  }
  for (int64_t i = 0; i < table_count * 2; ++i) {
    const uint64_t id = 500100 + i;
    const ObSchemaResolveRange range(SCHEMA_VERSION, OB_SCHEMA_VERSION_INF, id,
                                     SCHEMA_VERSION, 0, NS_MAX, true);
    ASSERT_EQ(OB_SUCCESS, service_.get_schema_resolve_cache().publish_if_current(
        ObSchemaIDResolveKey(TENANT_ID, token.cache_generations_.id_resolve_, id), range, state, token));
  }
  ObNewRow *row = NULL;
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  while (OB_SUCCESS == (ret = iter_.inner_get_next_row(row))) {
    EXPECT_TRUE(iter_.table_guard_.is_on_demand_schema_mode());
    EXPECT_LE(full_pins(iter_.table_guard_), 2);
    EXPECT_EQ(0, full_pins(scan_guard_));
    ++row_count;
  }
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(table_count * 4, row_count);
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
  iter_.inner_close();
  scan_guard_.reset();
  service_.schema_service_ = NULL;
  service_.sql_proxy_ = NULL;
  service_.config_ = NULL;
}

TEST_F(TestTableIndex, close_and_reopen_release_and_restart_table_scope)
{
  add_table_with_index(500100);
  open_scan(500100);
  ObNewRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
  EXPECT_EQ(1, full_pins(iter_.table_guard_));
  const int64_t parent_pins = scan_guard_.schema_objs_.count();
  ASSERT_EQ(OB_SUCCESS, iter_.inner_close());
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
  EXPECT_EQ(parent_pins, scan_guard_.schema_objs_.count());
  ASSERT_EQ(OB_SUCCESS, iter_.inner_open());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
  }
  EXPECT_EQ(OB_ITER_END, iter_.inner_get_next_row(row));
  iter_.reset();
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
}

TEST_F(TestTableIndex, index_load_error_releases_current_table)
{
  add_table_with_index(500100);
  service_.fail_table_id_ = 500101;
  open_scan(500100);
  ObNewRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
  EXPECT_EQ(1, full_pins(iter_.table_guard_));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, iter_.inner_get_next_row(row));
  EXPECT_EQ(nullptr, row);
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
  EXPECT_EQ(0, full_pins(scan_guard_));
}

TEST_F(TestTableIndex, materialized_view_container_uses_table_scope)
{
  add_table_with_index(500100);
  ObTableSchema view;
  make_table(view, 500200, "mv", MATERIALIZED_VIEW);
  view.set_data_table_id(500100);
  ASSERT_EQ(OB_SUCCESS, service_.add_table_schema(view, SCHEMA_VERSION));
  open_scan(500200);
  ObNewRow *row = NULL;
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter_.inner_get_next_row(row));
    EXPECT_EQ(500200, row->cells_[0].get_int());
    EXPECT_LE(full_pins(iter_.table_guard_), 3);
    EXPECT_EQ(0, full_pins(scan_guard_));
  }
  EXPECT_EQ(3, service_.full_fetch_count_);
  EXPECT_EQ(OB_ITER_END, iter_.inner_get_next_row(row));
  EXPECT_EQ(0, full_pins(iter_.table_guard_));
}

TEST_F(TestTableIndex, snapshot_inheritance_is_independent_and_point_lookup_only)
{
  add_table_with_index(500100);
  open_scan(500100);
  const ObTableSchema *table = NULL;
  ASSERT_EQ(OB_SUCCESS, scan_guard_.get_table_schema(TENANT_ID, 500100, table));
  ObSchemaGetterGuard child;
  ASSERT_EQ(OB_SUCCESS, child.init_from_snapshot(scan_guard_, TENANT_ID));
  EXPECT_EQ(0, full_pins(child));
  EXPECT_EQ(42U, child.get_session_id());
  EXPECT_EQ(OB_NOT_SUPPORTED, child.check_can_full_scan(TENANT_ID));
  int64_t version = 0;
  ASSERT_EQ(OB_SUCCESS, child.get_schema_version(TENANT_ID, version));
  EXPECT_EQ(SCHEMA_VERSION, version);
  ASSERT_EQ(OB_SUCCESS, child.get_table_schema(TENANT_ID, 500100, table));
  EXPECT_EQ(1, full_pins(child));
  EXPECT_EQ(OB_INIT_TWICE, child.init_from_snapshot(scan_guard_, TENANT_ID));
  EXPECT_EQ(1, full_pins(child));
  ASSERT_EQ(OB_SUCCESS, child.reset());
  EXPECT_EQ(1, full_pins(scan_guard_));
  EXPECT_EQ(ObString("table_500100"), table->get_table_name_str());
  // Inherit the strategy as well as the versions; do not use the service's
  // current mode or acquire a fresh/latest tenant guard.
  ASSERT_EQ(OB_SUCCESS, scan_guard_.init_table_impl(ObSimpleSchemaMode::ON_DEMAND));
  ASSERT_EQ(OB_SUCCESS, child.init_from_snapshot(scan_guard_, TENANT_ID));
  EXPECT_TRUE(child.is_on_demand_schema_mode());
  EXPECT_EQ(0, full_pins(child));
  EXPECT_EQ(nullptr, child.schema_scan_trans_);
}

TEST_F(TestTableIndex, invalid_snapshot_does_not_damage_source_or_destination)
{
  ObSchemaGetterGuard empty;
  ObSchemaGetterGuard child;
  EXPECT_EQ(OB_NOT_INIT, child.init_from_snapshot(empty, TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, child.init_from_snapshot(child, TENANT_ID));
  open_scan();
  EXPECT_EQ(OB_TENANT_NOT_EXIST, child.init_from_snapshot(scan_guard_, TENANT_ID + 1));
  EXPECT_FALSE(child.is_inited());
  EXPECT_EQ(0, child.schema_mgr_infos_.count());
  scan_guard_.tenant_id_ = TENANT_ID;
  EXPECT_EQ(OB_OP_NOT_ALLOW, child.init_from_snapshot(scan_guard_, TENANT_ID + 1));
  ASSERT_EQ(OB_SUCCESS, child.init_from_snapshot(scan_guard_, TENANT_ID));
}

TEST_F(TestTableIndex, child_retains_mgr_after_source_reset)
{
  open_scan();
  ObSchemaMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(TENANT_ID));
  mgr.set_schema_version(104);
  ObSchemaMgrCache cache;
  ASSERT_EQ(OB_SUCCESS, cache.init(2, ObSchemaMgrCache::REFRESH));
  ObSchemaMgr *evicted = NULL;
  ObSchemaMgrHandle handle;
  ASSERT_EQ(OB_SUCCESS, cache.put(&mgr, evicted, &handle));
  ASSERT_TRUE(handle.is_valid());
  ObSchemaMgrItem *item = handle.schema_mgr_item_;
  ObSchemaGetterGuard source;
  ASSERT_EQ(OB_SUCCESS, source.init(ObSimpleSchemaMode::ORIGINAL));
  source.schema_service_ = &service_;
  source.schema_guard_type_ = ObSchemaGetterGuard::SCHEMA_GUARD;
  const ObSchemaMgrInfo *sys_info = NULL;
  ASSERT_EQ(OB_SUCCESS, scan_guard_.get_schema_mgr_info(OB_SYS_TENANT_ID, sys_info));
  ASSERT_EQ(OB_SUCCESS, source.schema_mgr_infos_.push_back(*sys_info));
  {
    const ObSchemaMgr *mgr_ptr = &mgr;
    ObRefreshSchemaStatus status;
    status.tenant_id_ = TENANT_ID;
    status.snapshot_timestamp_ = 123456;
    const ObSchemaMgrInfo info(TENANT_ID, 104, mgr_ptr, handle, status);
    ASSERT_EQ(OB_SUCCESS, source.schema_mgr_infos_.push_back(info));
  }
  handle.reset();
  ASSERT_EQ(1, item->ref_cnt_);
  ObSchemaGetterGuard child;
  ASSERT_EQ(OB_SUCCESS, child.init_from_snapshot(source, TENANT_ID));
  ASSERT_EQ(2, item->ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, source.reset());
  EXPECT_EQ(1, item->ref_cnt_);
  const ObSchemaMgr *retained = NULL;
  ASSERT_EQ(OB_SUCCESS, child.get_schema_mgr(TENANT_ID, retained));
  EXPECT_EQ(&mgr, retained);
  int64_t version = 0;
  ASSERT_EQ(OB_SUCCESS, child.get_schema_version(TENANT_ID, version));
  EXPECT_EQ(104, version);
  ObRefreshSchemaStatus status;
  ASSERT_EQ(OB_SUCCESS, child.get_schema_status(TENANT_ID, status));
  EXPECT_EQ(123456, status.snapshot_timestamp_);
  ASSERT_EQ(OB_SUCCESS, child.reset());
  EXPECT_EQ(0, item->ref_cnt_);
}

} // namespace
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
